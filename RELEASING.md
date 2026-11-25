# Releasing

`fastexec` follows [Semantic Versioning](https://semver.org). The `version` in `Cargo.toml` sets the
release version, and the annotated tag `vX.Y.Z` publishes it. Before `1.0`, additions or breaking
changes bump the minor version (`0.14.0` to `0.15.0`), and bug-fix releases bump the patch version
(`0.14.0` to `0.14.1`).

## Release

1. On `main`, with CI passing, set `version` in `Cargo.toml`, run `cargo check` to update
   `Cargo.lock`, and commit both as `Release X.Y.Z`. Push `main`.
2. Tag the commit and push the tag:

   ```bash
   git tag -a vX.Y.Z -m "fastexec X.Y.Z"
   git push origin vX.Y.Z
   ```

3. On a pushed tag, `.github/workflows/release.yml` verifies that `vX.Y.Z` matches `Cargo.toml` and
   points to an ancestor of `origin/main`, runs `.github/workflows/ci.yml` on the tagged commit,
   builds and smoke-tests all seven platform archives, and creates a draft GitHub Release with the
   archives, `SHA256SUMS`, and generated release notes.
4. Review the draft on the Releases page, verify `SHA256SUMS`, and publish it there or with
   `gh release edit vX.Y.Z --draft=false`.

Running the `release` workflow manually (`workflow_dispatch` from Actions) skips the `ci` job and
builds and smoke-tests all seven archives on the selected branch without creating a draft release.

## MCP compatibility gate

Normal CI and release smoke checks cover both supported MCP lifecycles:
`initialize` / `notifications/initialized` sessions and stateless `server/discover` requests with
inline `_meta`. Keep both the session smoke path and the discovery and `tools/list` probe in
[`scripts/ci/smoke-release.py`](scripts/ci/smoke-release.py) and `.github/workflows/release.yml`.
`supportedVersions` is a set; discovery assertions check membership (such as `"2026-07-28"`) without
restricting the list to a single protocol revision.

## Failed release

Use "Re-run failed jobs" on the same workflow run when a transient runner failure occurs. For a code
defect found before publishing, delete the draft release and tag, land the fix on `main`, and tag
again:

```bash
gh release delete vX.Y.Z --yes
git push --delete origin vX.Y.Z
git tag -d vX.Y.Z
```

Never move a published tag; ship fixes in the next patch release.

## Platforms

| Archive | Target | Release runner |
|---|---|---|
| `linux-amd64` | `x86_64-unknown-linux-musl` | `ubuntu-24.04` |
| `linux-arm64` | `aarch64-unknown-linux-musl` | `ubuntu-24.04-arm` |
| `macos-amd64` | `x86_64-apple-darwin` | `macos-latest` |
| `macos-arm64` | `aarch64-apple-darwin` | `macos-latest` |
| `windows-amd64` | `x86_64-pc-windows-msvc` | `windows-latest` |
| `windows-arm64` | `aarch64-pc-windows-msvc` | `windows-11-arm` |
| `freebsd-amd64` | `x86_64-unknown-freebsd` | FreeBSD 15.1 VM on `ubuntu-24.04` |

Every release archive includes the `fastexec` (`fastexec.exe`) binary, `LICENSE`, `NOTICE`, and
`README.md`; Windows `.zip` archives also include `vendor/conpty/LICENSE`. The `check` and `release`
jobs in `.github/workflows/release.yml` run on `ubuntu-latest`.

Linux release binaries link musl statically. Both Linux archives are extracted and verified inside
`alpine:3.23` containers of the matching architecture with `bash` and `python3` installed and no
glibc compatibility package, running `scripts/ci/smoke-release.py`. Alpine deployments use these
Linux archives directly.

The FreeBSD archive is built natively and verified on FreeBSD 15.1 amd64 with
`scripts/ci/smoke-release.py`. Its binary links the FreeBSD base-system libraries and does not
require Rust at runtime. Both the Alpine and FreeBSD `smoke-release.py` checks exercise MCP
`initialize` and `server/discover`, `tools/list`, file `write`/`edit`/`read`, pipe `exec`, PTY
input and terminal resizing, and `daemon start`/`status`/`stop` with tasks carried across sessions.
macOS and Windows release jobs verify extracted archives over stdio across both `initialize` and
`server/discover` / inline `tools/list`.

Windows binaries link the C runtime statically (`.cargo/config.toml`) and embed the matching
architecture's ConPTY binaries. macOS binaries are unsigned.

Normal CI (`.github/workflows/ci.yml`) runs the full test suite with stable Rust on `ubuntu-latest`,
`macos-latest`, `windows-latest`, and `windows-11-arm`, compiles and runs the suite natively inside
`rust:1.92.0-alpine3.23` containers on `ubuntu-24.04` (amd64) and `ubuntu-24.04-arm` (arm64) to pin
the Rust `1.92` minimum supported version, and runs natively on FreeBSD 15.1 amd64 (`ubuntu-24.04`
VM host) with the packaged Rust toolchain.
