%define anolis_release 1
%global debug_package %{nil}

Name:           blaze
Version:        0.3.0
Release:        %{anolis_release}%{?dist}
Summary:        Per-host sandbox orchestrator daemon and client

License:        Apache-2.0
URL:            https://github.com/alibaba/anolisa
Packager:        Blaze Package Builder
Source0:        %{name}-%{version}.tar.gz
Source1:        %{name}-%{version}-vendor.tar.gz

BuildRequires:  rust >= 1.88
BuildRequires:  cargo
BuildRequires:  systemd-rpm-macros

Provides:       anolisa-component(blaze)

%description
Blaze provides the per-host sandbox orchestrator daemon and its HTTP client.
The daemon manages sandbox lifecycles with policy-driven backend selection,
including Firecracker microVM, Bubblewrap, and Mock backends. The blazectl
client exposes the supported remote management operations.

%prep
%setup -q
%setup -q -T -D -a 1

mkdir -p .cargo
cat > .cargo/config.toml <<'EOF'
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "vendor"
EOF

%build
export CARGO_HOME=$(pwd)/.cargo-home
mkdir -p "$CARGO_HOME"
cp .cargo/config.toml "$CARGO_HOME/config.toml"
cargo build --workspace --release --offline --locked

%install
rm -rf $RPM_BUILD_ROOT
install -d -m 0755 %{buildroot}%{_libexecdir}/anolisa
install -d -m 0755 %{buildroot}%{_unitdir}
install -d -m 0755 %{buildroot}%{_sysconfdir}/anolisa/blaze/policies
install -d -m 0755 %{buildroot}%{_datadir}/anolisa/components/blaze
install -d -m 0755 %{buildroot}%{_docdir}/blaze
install -d -m 0755 %{buildroot}%{_tmpfilesdir}
install -d -m 0755 %{buildroot}/var/lib/blaze

install -Dm755 target/release/blazed %{buildroot}%{_libexecdir}/anolisa/blazed
install -Dm755 target/release/blazectl %{buildroot}%{_bindir}/blazectl
install -p -m 0644 dist/blazed.service %{buildroot}%{_unitdir}/
install -Dm644 dist/tmpfiles-blaze.conf %{buildroot}%{_tmpfilesdir}/blaze.conf
install -Dm644 .anolisa/component.toml %{buildroot}%{_datadir}/anolisa/components/blaze/component.toml
install -p -m 0644 examples/config.toml %{buildroot}%{_sysconfdir}/anolisa/blaze/config.toml
install -p -m 0644 examples/policies/agent-rl.toml %{buildroot}%{_sysconfdir}/anolisa/blaze/policies/
install -p -m 0644 examples/policies/agent-tool.toml %{buildroot}%{_sysconfdir}/anolisa/blaze/policies/
install -p -m 0644 README.md %{buildroot}%{_docdir}/blaze/
install -p -m 0644 README_zh.md %{buildroot}%{_docdir}/blaze/
install -p -m 0644 LICENSE %{buildroot}%{_docdir}/blaze/

%post
%tmpfiles_create %{_tmpfilesdir}/blaze.conf
%systemd_post blazed.service

%preun
%systemd_preun blazed.service

%postun
%systemd_postun blazed.service

%files
%defattr(0644,root,root,0755)
%attr(0755,root,root) %{_libexecdir}/anolisa/blazed
%attr(0755,root,root) %{_bindir}/blazectl
%config(noreplace) %{_sysconfdir}/anolisa/blaze/config.toml
%dir %{_sysconfdir}/anolisa/blaze/policies
%config(noreplace) %{_sysconfdir}/anolisa/blaze/policies/agent-rl.toml
%config(noreplace) %{_sysconfdir}/anolisa/blaze/policies/agent-tool.toml
%{_unitdir}/blazed.service
%{_tmpfilesdir}/blaze.conf
%{_datadir}/anolisa/components/blaze/component.toml
%dir /var/lib/blaze
%doc %{_docdir}/blaze/README.md
%doc %{_docdir}/blaze/README_zh.md
%license %{_docdir}/blaze/LICENSE
