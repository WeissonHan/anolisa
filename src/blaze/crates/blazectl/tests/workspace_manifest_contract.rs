// SPDX-License-Identifier: Apache-2.0

const WORKSPACE: &str = include_str!("../../../Cargo.toml");
const CORE: &str = include_str!("../../blaze-core/Cargo.toml");
const DAEMON: &str = include_str!("../../blazed/Cargo.toml");
const CLIENT: &str = include_str!("../Cargo.toml");

#[test]
fn third_party_dependency_versions_are_owned_only_by_the_workspace() {
    assert_eq!(
        WORKSPACE
            .lines()
            .filter(|line| line.trim_start().starts_with("tempfile = "))
            .count(),
        1,
        "workspace must own exactly one tempfile version"
    );
    for (name, manifest) in [
        ("blaze-core", CORE),
        ("blazed", DAEMON),
        ("blazectl", CLIENT),
    ] {
        assert_workspace_dependencies(name, manifest);
    }
}

fn assert_workspace_dependencies(name: &str, manifest: &str) {
    let mut dependency_section = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            dependency_section = is_dependency_section(line);
            continue;
        }
        if !dependency_section || line.is_empty() || line.starts_with('#') {
            continue;
        }
        assert!(
            line.contains("workspace = true"),
            "{name} dependency must inherit its version from the workspace"
        );
    }
}

fn is_dependency_section(header: &str) -> bool {
    matches!(
        header,
        "[dependencies]" | "[dev-dependencies]" | "[build-dependencies]"
    ) || header.ends_with(".dependencies]")
        || header.ends_with(".dev-dependencies]")
        || header.ends_with(".build-dependencies]")
}
