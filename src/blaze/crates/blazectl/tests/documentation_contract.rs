// SPDX-License-Identifier: Apache-2.0

const AGENTS: &str = include_str!("../../../AGENTS.md");
const README: &str = include_str!("../../../README.md");
const README_ZH: &str = include_str!("../../../README_zh.md");
const GUIDE_EN: &str =
    include_str!("../../../../../docs/user-guide/en/runtime/blaze/QUICKSTART.md");
const GUIDE_ZH: &str =
    include_str!("../../../../../docs/user-guide/zh/runtime/blaze/QUICKSTART.md");
const INDEX_EN: &str = include_str!("../../../../../docs/user-guide/en/README.md");
const INDEX_ZH: &str = include_str!("../../../../../docs/user-guide/zh/README.md");

const REMOTE_COMMANDS: [&str; 14] = [
    "create",
    "exec",
    "list",
    "kill",
    "hibernate",
    "checkpoint",
    "rollback",
    "checkpoints",
    "prune-checkpoints",
    "resume",
    "cleanup-devices",
    "pool-status",
    "read",
    "write",
];

const REMOTE_HTTP: [(&str, &str); 14] = [
    ("create", "POST /v1/sandboxes"),
    ("exec", "POST /v1/sandboxes/{id}/exec"),
    ("list", "GET /v1/sandboxes"),
    ("kill", "DELETE /v1/sandboxes/{id}"),
    ("hibernate", "POST /v1/sandboxes/{id}/hibernate"),
    ("checkpoint", "POST /v1/sandboxes/{id}/checkpoint"),
    ("rollback", "POST /v1/sandboxes/{id}/rollback/{checkpoint}"),
    ("checkpoints", "GET /v1/sandboxes/{id}/checkpoints"),
    (
        "prune-checkpoints",
        "POST /v1/sandboxes/{id}/checkpoints/prune",
    ),
    ("resume", "POST /v1/sandboxes/{id}/resume"),
    ("cleanup-devices", "POST /v1/pool/cleanup"),
    ("pool-status", "GET /v1/pool/status"),
    ("read", "POST /v1/sandboxes/{id}/read"),
    ("write", "POST /v1/sandboxes/{id}/write"),
];

#[test]
fn scoped_agent_contract_describes_all_three_crates() {
    for crate_name in ["blaze-core", "blazed", "blazectl"] {
        assert!(AGENTS.contains(&format!("**{crate_name}**")));
    }
    assert!(AGENTS.contains("Three-crate workspace"));
    assert!(AGENTS.contains("`blazectl`"));
    assert!(AGENTS.contains("HTTP client"));
    assert!(!AGENTS.contains("Two-crate workspace"));
    assert!(!AGENTS.contains("No CLI client"));
}

#[test]
fn bilingual_guides_cover_the_exact_remote_and_local_surfaces() {
    for guide in [GUIDE_EN, GUIDE_ZH] {
        for command in REMOTE_COMMANDS {
            let row = format!("| `blazectl {command}");
            assert_eq!(
                guide
                    .lines()
                    .filter(|line| {
                        line.strip_prefix(&row)
                            .is_some_and(|tail| tail.starts_with('`') || tail.starts_with(' '))
                    })
                    .count(),
                1,
                "missing or duplicate canonical command row: {command}"
            );
        }
        assert!(guide.contains("| `blazectl version`"));
        assert!(guide.contains("`blazectl --version`"));
        for forbidden in ["template", "policy", "metrics", "admin", "daemon"] {
            assert!(!guide.contains(&format!("| `blazectl {forbidden}")));
        }
    }
}

#[test]
fn bilingual_guides_freeze_transport_output_and_exit_contracts() {
    for guide in [GUIDE_EN, GUIDE_ZH] {
        for required in [
            "--socket",
            "--url",
            "BLAZED_URL",
            "--output",
            "BLAZECTL_OUTPUT",
            "/run/blaze/api.sock",
            "stdout",
            "stderr",
            "16 MiB",
            "32 MiB",
            "5 seconds",
            "30 seconds",
            "50",
            "| 0 |",
            "| 1 |",
            "| 2 |",
        ] {
            assert!(guide.contains(required), "guide misses {required}");
        }
    }
}

#[test]
fn bilingual_guides_match_the_frozen_http_routes() {
    for guide in [GUIDE_EN, GUIDE_ZH] {
        for (command, request) in REMOTE_HTTP {
            let command_row = format!("| `blazectl {command}");
            let line = guide
                .lines()
                .find(|line| {
                    line.strip_prefix(&command_row)
                        .is_some_and(|tail| tail.starts_with('`') || tail.starts_with(' '))
                })
                .unwrap_or_else(|| panic!("missing command row: {command}"));
            assert!(
                line.contains(&format!("`{request}`")),
                "{command} row misses {request}"
            );
        }
        assert!(
            guide.contains("`--all` first uses `GET /v1/sandboxes`")
                || guide.contains("`--all` 先使用 `GET /v1/sandboxes`")
        );
    }
}

#[test]
fn bilingual_docs_are_linked_and_shell_examples_match() {
    assert!(README.contains("../../docs/user-guide/en/runtime/blaze/QUICKSTART.md"));
    assert!(README_ZH.contains("../../docs/user-guide/zh/runtime/blaze/QUICKSTART.md"));
    assert!(GUIDE_EN.contains("../../../zh/runtime/blaze/QUICKSTART.md"));
    assert!(GUIDE_ZH.contains("../../../en/runtime/blaze/QUICKSTART.md"));
    assert!(INDEX_EN.contains("runtime/blaze/QUICKSTART.md"));
    assert!(INDEX_ZH.contains("runtime/blaze/QUICKSTART.md"));

    assert_eq!(bash_blocks(README), bash_blocks(README_ZH));
    assert_eq!(bash_blocks(GUIDE_EN), bash_blocks(GUIDE_ZH));
}

#[test]
fn component_readmes_expose_the_client_without_expanding_its_scope() {
    for readme in [README, README_ZH] {
        assert!(readme.contains("blazectl"));
        assert!(readme.contains("blaze-core"));
        assert!(readme.contains("blazed"));
        assert!(readme.contains("14"));
        for forbidden in ["`blazectl template", "`blazectl policy", "`blazectl admin"] {
            assert!(!readme.contains(forbidden));
        }
    }
}

fn bash_blocks(document: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current = Vec::new();
    let mut in_block = false;
    for line in document.lines() {
        if line == "```bash" {
            assert!(!in_block, "nested bash fence");
            in_block = true;
            current.clear();
        } else if line == "```" && in_block {
            blocks.push(current.join("\n"));
            in_block = false;
        } else if in_block {
            current.push(line);
        }
    }
    assert!(!in_block, "unterminated bash fence");
    blocks
}
