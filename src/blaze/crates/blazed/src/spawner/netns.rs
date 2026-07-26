// SPDX-License-Identifier: Apache-2.0
//! Per-VM network namespace allocation and compensated setup.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
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

#[derive(Debug, Clone)]
pub(super) struct IpOutput {
    pub(super) success: bool,
    pub(super) status: String,
    pub(super) stdout: Vec<u8>,
    pub(super) stderr: Vec<u8>,
}

#[async_trait]
pub(super) trait IpCommandRunner: Send + Sync {
    async fn output(&self, args: &[String], timeout: Duration) -> Result<IpOutput>;
}

struct SystemIpCommandRunner;

#[async_trait]
impl IpCommandRunner for SystemIpCommandRunner {
    async fn output(&self, args: &[String], timeout: Duration) -> Result<IpOutput> {
        let output = tokio::time::timeout(
            timeout,
            Command::new("ip").env("LC_ALL", "C").args(args).output(),
        )
        .await
        .map_err(|_| BlazeError::BackendError {
            msg: format!("ip {} timed out", args.join(" ")),
        })??;
        Ok(IpOutput {
            success: output.status.success(),
            status: output.status.to_string(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

/// Process-local allocator and lifecycle owner for Blaze network namespaces.
pub struct NetworkManager {
    state: Mutex<SlotState>,
    command_timeout: Duration,
    runner: Arc<dyn IpCommandRunner>,
}

impl fmt::Debug for NetworkManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetworkManager")
            .field("command_timeout", &self.command_timeout)
            .finish_non_exhaustive()
    }
}

impl Default for NetworkManager {
    fn default() -> Self {
        Self {
            state: Mutex::new(SlotState::default()),
            command_timeout: Duration::from_secs(5),
            runner: Arc::new(SystemIpCommandRunner),
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
    #[cfg(test)]
    pub(super) fn with_runner(runner: Arc<dyn IpCommandRunner>) -> Self {
        Self {
            state: Mutex::new(SlotState::default()),
            command_timeout: Duration::from_secs(1),
            runner,
        }
    }

    /// Create an isolated namespace, veth uplink, tap, route, and NAT rule.
    pub async fn create(&self) -> Result<NetworkSlot> {
        let mut blocked = self.existing_slots().await?;
        let (slot, network) = loop {
            let slot = self.allocate(&blocked)?;
            let network = network_slot(slot);
            let add_namespace = vec!["netns".into(), "add".into(), network.netns.clone()];
            match self.run_ip(&add_namespace).await {
                Ok(()) => break (slot, network),
                Err(error) => {
                    self.release(slot);
                    let refreshed = self.existing_slots().await?;
                    if refreshed.contains(&slot) {
                        blocked.extend(refreshed);
                        continue;
                    }
                    return Err(error);
                }
            }
        };
        let (host_ip, peer_ip) = veth_ips(slot);
        let peer = format!("blz-vpeer-{slot}");
        let add_veth = vec![
            "link".into(),
            "add".into(),
            network.veth_host.clone(),
            "type".into(),
            "veth".into(),
            "peer".into(),
            "name".into(),
            peer.clone(),
        ];
        if let Err(error) = self.run_ip(&add_veth).await {
            return self.fail_setup(&network, error, false).await;
        }
        let host_steps = vec![
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
                return self.fail_setup(&network, error, true).await;
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
                return self.fail_setup(&network, error, true).await;
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

    fn allocate(&self, blocked: &HashSet<usize>) -> Result<usize> {
        let mut state = self.state.lock().map_err(|_| BlazeError::BackendError {
            msg: "network slot allocator lock poisoned".to_string(),
        })?;
        for offset in 0..NET_MAX_SLOT {
            let slot = (state.next + offset) % NET_MAX_SLOT;
            if !blocked.contains(&slot) && state.used.insert(slot) {
                state.next = (slot + 1) % NET_MAX_SLOT;
                return Ok(slot);
            }
        }
        Err(BlazeError::BackendError {
            msg: format!("network slots exhausted (max {NET_MAX_SLOT})"),
        })
    }

    async fn existing_slots(&self) -> Result<HashSet<usize>> {
        let args = vec!["netns".to_string(), "list".to_string()];
        let output = self.run_ip_output(&args).await?;
        if !output.success {
            return Err(command_error(&args, &output, "listing namespaces"));
        }
        Ok(parse_existing_slots(&output.stdout))
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

    async fn fail_setup<T>(
        &self,
        network: &NetworkSlot,
        original: BlazeError,
        delete_veth: bool,
    ) -> Result<T> {
        match self.cleanup_partial(network, delete_veth).await {
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

    async fn cleanup_partial(&self, network: &NetworkSlot, delete_veth: bool) -> Result<()> {
        if delete_veth {
            self.run_ip_cleanup(&["link".into(), "del".into(), network.veth_host.clone()])
                .await?;
        }
        self.run_ip_cleanup(&["netns".into(), "del".into(), network.netns.clone()])
            .await
    }

    async fn run_in_namespace(&self, netns: &str, command: &[String]) -> Result<()> {
        let mut args = vec!["netns".to_string(), "exec".to_string(), netns.to_string()];
        args.extend_from_slice(command);
        self.run_ip(&args).await
    }

    async fn run_ip(&self, args: &[String]) -> Result<()> {
        let output = self.run_ip_output(args).await?;
        if output.success {
            return Ok(());
        }
        Err(command_error(args, &output, "running command"))
    }

    async fn run_ip_cleanup(&self, args: &[String]) -> Result<()> {
        let output = self.run_ip_output(args).await?;
        if output.success {
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
        Err(command_error(args, &output, "cleaning up"))
    }

    async fn run_ip_output(&self, args: &[String]) -> Result<IpOutput> {
        self.runner.output(args, self.command_timeout).await
    }
}

fn command_error(args: &[String], output: &IpOutput, context: &str) -> BlazeError {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.chars().take(4096).collect::<String>();
    BlazeError::BackendError {
        msg: format!(
            "ip {} failed while {context} ({}): {}",
            args.join(" "),
            output.status,
            stderr.trim()
        ),
    }
}

fn parse_existing_slots(stdout: &[u8]) -> HashSet<usize> {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter_map(|name| name.strip_prefix("blz-ns-"))
        .filter_map(|slot| slot.parse::<usize>().ok())
        .filter(|slot| *slot < NET_MAX_SLOT)
        .collect()
}

fn network_slot(slot: usize) -> NetworkSlot {
    NetworkSlot {
        slot,
        netns: format!("blz-ns-{slot}"),
        tap_name: "tap0".to_string(),
        veth_host: format!("blz-veth-{slot}"),
    }
}

#[cfg(test)]
pub(super) fn test_network_slot(slot: usize) -> NetworkSlot {
    network_slot(slot)
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
    use std::collections::VecDeque;

    use super::*;

    #[test]
    fn allocator_is_unique_and_recycles() {
        let manager = NetworkManager::default();
        let first = manager.allocate(&HashSet::new()).expect("first");
        let second = manager.allocate(&HashSet::new()).expect("second");
        assert_ne!(first, second);
        manager.release(first);
        for _ in 0..NET_MAX_SLOT {
            if manager.allocate(&HashSet::new()).expect("slot") == first {
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

    #[test]
    fn existing_slot_parser_ignores_unrelated_and_invalid_names() {
        let slots = parse_existing_slots(
            b"blz-ns-0\nblz-ns-17 (id: 2)\nunrelated\nblz-ns-nope\nblz-ns-16383\n",
        );
        assert_eq!(slots, HashSet::from([0, 17]));
    }

    #[tokio::test]
    async fn create_skips_namespaces_owned_by_another_process() {
        let runner = Arc::new(FakeIpRunner::with_responses([success(
            b"blz-ns-0 (id: 4)\n",
        )]));
        let manager = NetworkManager::with_runner(runner.clone());

        let slot = manager.create().await.expect("create slot");

        assert_eq!(slot.slot, 1);
        let calls = runner.calls();
        assert!(
            calls
                .iter()
                .any(|args| args == &["netns", "add", "blz-ns-1"])
        );
        assert!(
            !calls
                .iter()
                .any(|args| args == &["netns", "del", "blz-ns-0"])
        );
    }

    #[tokio::test]
    async fn create_retries_when_namespace_appears_during_allocation() {
        let runner = Arc::new(FakeIpRunner::with_responses([
            success(b""),
            failure("namespace already exists"),
            success(b"blz-ns-0\n"),
        ]));
        let manager = NetworkManager::with_runner(runner.clone());

        let slot = manager.create().await.expect("create slot");

        assert_eq!(slot.slot, 1);
        assert!(
            runner
                .calls()
                .iter()
                .any(|args| args == &["netns", "add", "blz-ns-1"])
        );
    }

    #[tokio::test]
    async fn failed_veth_creation_only_removes_new_namespace() {
        let runner = Arc::new(FakeIpRunner::with_responses([
            success(b""),
            success(b""),
            failure("veth already exists"),
        ]));
        let manager = NetworkManager::with_runner(runner.clone());

        manager.create().await.expect_err("veth creation must fail");

        let calls = runner.calls();
        assert!(
            calls
                .iter()
                .any(|args| args == &["netns", "del", "blz-ns-0"])
        );
        assert!(
            !calls
                .iter()
                .any(|args| args == &["link", "del", "blz-veth-0"])
        );
    }

    #[derive(Default)]
    struct FakeIpRunner {
        responses: Mutex<VecDeque<IpOutput>>,
        calls: Mutex<Vec<Vec<String>>>,
    }

    impl FakeIpRunner {
        fn with_responses<const N: usize>(responses: [IpOutput; N]) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    #[async_trait]
    impl IpCommandRunner for FakeIpRunner {
        async fn output(&self, args: &[String], _timeout: Duration) -> Result<IpOutput> {
            self.calls.lock().expect("calls lock").push(args.to_vec());
            Ok(self
                .responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .unwrap_or_else(|| success(b"")))
        }
    }

    fn success(stdout: &[u8]) -> IpOutput {
        IpOutput {
            success: true,
            status: "exit status: 0".to_string(),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        }
    }

    fn failure(stderr: &str) -> IpOutput {
        IpOutput {
            success: false,
            status: "exit status: 1".to_string(),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        }
    }
}
