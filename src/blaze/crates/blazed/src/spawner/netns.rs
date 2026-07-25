// SPDX-License-Identifier: Apache-2.0
//! Per-VM network namespace allocation and compensated setup.

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;

use blaze_core::{BlazeError, Result};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

const NET_VETH_BASE: usize = 4;
const NET_VETH_TOP: usize = 0x1_0000;
const NET_MAX_SLOT: usize = (NET_VETH_TOP - NET_VETH_BASE) / 4;

#[derive(Debug, Default)]
struct SlotState {
    used: HashSet<usize>,
    next: usize,
}

/// Process-local allocator and lifecycle owner for Blaze network namespaces.
#[derive(Debug)]
pub struct NetworkManager {
    state: Mutex<SlotState>,
    command_timeout: Duration,
}

impl Default for NetworkManager {
    fn default() -> Self {
        Self {
            state: Mutex::new(SlotState::default()),
            command_timeout: Duration::from_secs(5),
        }
    }
}

/// One fully configured per-VM network namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkSlot {
    slot: usize,
    netns: String,
    tap_name: String,
    veth_host: String,
}

impl NetworkSlot {
    /// Network namespace name.
    pub fn netns(&self) -> &str {
        &self.netns
    }

    /// Tap device visible inside the namespace.
    pub fn tap_name(&self) -> &str {
        &self.tap_name
    }
}

impl NetworkManager {
    /// Create an isolated namespace, veth uplink, tap, route, and NAT rule.
    pub async fn create(&self) -> Result<NetworkSlot> {
        let slot = self.allocate()?;
        let network = network_slot(slot);
        if let Err(error) = self.cleanup_commands(&network).await {
            self.release(slot);
            return Err(error);
        }

        let (host_ip, peer_ip) = veth_ips(slot);
        let peer = format!("blz-vpeer-{slot}");
        let host_steps = vec![
            vec!["netns".into(), "add".into(), network.netns.clone()],
            vec![
                "link".into(),
                "add".into(),
                network.veth_host.clone(),
                "type".into(),
                "veth".into(),
                "peer".into(),
                "name".into(),
                peer.clone(),
            ],
            vec![
                "link".into(),
                "set".into(),
                peer.clone(),
                "netns".into(),
                network.netns.clone(),
            ],
            vec![
                "addr".into(),
                "add".into(),
                format!("{host_ip}/30"),
                "dev".into(),
                network.veth_host.clone(),
            ],
            vec![
                "link".into(),
                "set".into(),
                network.veth_host.clone(),
                "up".into(),
            ],
        ];
        for args in host_steps {
            if let Err(error) = self.run_ip(&args).await {
                return self.fail_setup(&network, error).await;
            }
        }

        let ns_steps = vec![
            vec![
                "ip".into(),
                "addr".into(),
                "add".into(),
                format!("{peer_ip}/30"),
                "dev".into(),
                peer.clone(),
            ],
            vec![
                "ip".into(),
                "link".into(),
                "set".into(),
                peer.clone(),
                "up".into(),
            ],
            vec![
                "ip".into(),
                "link".into(),
                "set".into(),
                "lo".into(),
                "up".into(),
            ],
            vec![
                "ip".into(),
                "tuntap".into(),
                "add".into(),
                network.tap_name.clone(),
                "mode".into(),
                "tap".into(),
            ],
            vec![
                "ip".into(),
                "addr".into(),
                "add".into(),
                "169.254.0.1/30".into(),
                "dev".into(),
                network.tap_name.clone(),
            ],
            vec![
                "ip".into(),
                "link".into(),
                "set".into(),
                network.tap_name.clone(),
                "up".into(),
            ],
            vec![
                "ip".into(),
                "route".into(),
                "add".into(),
                "default".into(),
                "via".into(),
                host_ip,
            ],
            vec!["sysctl".into(), "-w".into(), "net.ipv4.ip_forward=1".into()],
            vec![
                "iptables".into(),
                "-t".into(),
                "nat".into(),
                "-A".into(),
                "POSTROUTING".into(),
                "-s".into(),
                "169.254.0.2".into(),
                "-o".into(),
                peer,
                "-j".into(),
                "SNAT".into(),
                "--to".into(),
                peer_ip,
            ],
        ];
        for command in ns_steps {
            if let Err(error) = self.run_in_namespace(&network.netns, &command).await {
                return self.fail_setup(&network, error).await;
            }
        }
        Ok(network)
    }

    /// Remove all resources for a slot and return it to the allocator.
    pub async fn destroy(&self, network: &NetworkSlot) -> Result<()> {
        self.cleanup_commands(network).await?;
        self.release(network.slot);
        Ok(())
    }

    fn allocate(&self) -> Result<usize> {
        let mut state = self.state.lock().map_err(|_| BlazeError::BackendError {
            msg: "network slot allocator lock poisoned".to_string(),
        })?;
        for offset in 0..NET_MAX_SLOT {
            let slot = (state.next + offset) % NET_MAX_SLOT;
            if state.used.insert(slot) {
                state.next = (slot + 1) % NET_MAX_SLOT;
                return Ok(slot);
            }
        }
        Err(BlazeError::BackendError {
            msg: format!("network slots exhausted (max {NET_MAX_SLOT})"),
        })
    }

    fn release(&self, slot: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.used.remove(&slot);
        }
    }

    async fn cleanup_commands(&self, network: &NetworkSlot) -> Result<()> {
        self.run_ip_cleanup(&["link".into(), "del".into(), network.veth_host.clone()])
            .await?;
        self.run_ip_cleanup(&["netns".into(), "del".into(), network.netns.clone()])
            .await?;
        Ok(())
    }

    async fn fail_setup<T>(&self, network: &NetworkSlot, original: BlazeError) -> Result<T> {
        match self.cleanup_commands(network).await {
            Ok(()) => {
                self.release(network.slot);
                Err(original)
            }
            Err(cleanup) => Err(BlazeError::BackendError {
                msg: format!(
                    "network setup failed ({original}); cleanup failed ({cleanup}); slot {} retained",
                    network.slot
                ),
            }),
        }
    }

    async fn run_in_namespace(&self, netns: &str, command: &[String]) -> Result<()> {
        let mut args = vec!["netns".to_string(), "exec".to_string(), netns.to_string()];
        args.extend_from_slice(command);
        self.run_ip(&args).await
    }

    async fn run_ip(&self, args: &[String]) -> Result<()> {
        let output =
            tokio::time::timeout(self.command_timeout, Command::new("ip").args(args).output())
                .await
                .map_err(|_| BlazeError::BackendError {
                    msg: format!("ip {} timed out", args.join(" ")),
                })??;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.chars().take(4096).collect::<String>();
        Err(BlazeError::BackendError {
            msg: format!(
                "ip {} exited with {}: {}",
                args.join(" "),
                output.status,
                stderr.trim()
            ),
        })
    }

    async fn run_ip_cleanup(&self, args: &[String]) -> Result<()> {
        let output =
            tokio::time::timeout(self.command_timeout, Command::new("ip").args(args).output())
                .await
                .map_err(|_| BlazeError::BackendError {
                    msg: format!("ip {} timed out during cleanup", args.join(" ")),
                })??;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if [
            "Cannot find device",
            "No such file",
            "Cannot remove namespace file",
            "Invalid \"netns\" value",
        ]
        .iter()
        .any(|marker| stderr.contains(marker))
        {
            return Ok(());
        }
        Err(BlazeError::BackendError {
            msg: format!(
                "ip {} cleanup exited with {}: {}",
                args.join(" "),
                output.status,
                stderr.chars().take(4096).collect::<String>().trim()
            ),
        })
    }
}

fn network_slot(slot: usize) -> NetworkSlot {
    NetworkSlot {
        slot,
        netns: format!("blz-ns-{slot}"),
        tap_name: "tap0".to_string(),
        veth_host: format!("blz-veth-{slot}"),
    }
}

fn veth_ips(slot: usize) -> (String, String) {
    let base = NET_VETH_BASE + slot * 4;
    let third = (base >> 8) & 0xff;
    (
        format!("169.254.{third}.{}", (base & 0xff) + 1),
        format!("169.254.{third}.{}", (base & 0xff) + 2),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocator_is_unique_and_recycles() {
        let manager = NetworkManager::default();
        let first = manager.allocate().expect("first");
        let second = manager.allocate().expect("second");
        assert_ne!(first, second);
        manager.release(first);
        for _ in 0..NET_MAX_SLOT {
            if manager.allocate().expect("slot") == first {
                return;
            }
        }
        panic!("released slot was not recycled");
    }

    #[test]
    fn addresses_match_go_contract() {
        assert_eq!(
            veth_ips(0),
            ("169.254.0.5".to_string(), "169.254.0.6".to_string())
        );
        assert_eq!(
            veth_ips(63),
            ("169.254.1.1".to_string(), "169.254.1.2".to_string())
        );
    }
}
