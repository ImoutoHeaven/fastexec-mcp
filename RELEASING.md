# Releasing

fastexec follows [Semantic Versioning](https://semver.org). The `version` in `Cargo.toml` is the
release version, and the tag `vX.Y.Z` publishes it. Before 1.0, a release with new features or
breaking changes raises the minor version (`0.1.0` to `0.2.0`), and a release with only fixes
raises the patch version (`0.1.0` to `0.1.1`).

## Release

1. On `main`, with CI passing, set `version` in `Cargo.toml`, run `cargo check` to update
   `Cargo.lock`, and commit both as `Release X.Y.Z`. Push `main`.
2. Tag the commit and push the tag:

   ```bash
   git tag -a vX.Y.Z -m "fastexec X.Y.Z"
   git push origin vX.Y.Z
   ```

3. The `release` workflow checks that the tag matches `Cargo.toml` and points into `main`, runs the
   CI suite on the tagged commit, builds and smoke-tests every platform, and creates a draft
   release with the archives, `SHA256SUMS`, and generated notes.
4. Review the draft on the Releases page, then publish it there or with
   `gh release edit vX.Y.Z --draft=false`.

Running the `release` workflow by hand (Actions, `release`, Run workflow) builds and smoke-tests the
archives of a branch without releasing, as a rehearsal before tagging.

## Failed release

A job that failed on a flaky test passes with Re-run failed jobs on the same run. For a problem in
the code, before publishing, delete the draft and the tag, fix the problem on `main`, and tag again:

```bash
gh release delete vX.Y.Z --yes
git push --delete origin vX.Y.Z
git tag -d vX.Y.Z
```

A published tag stays where it is: fix the problem in the next patch release.

## Platforms

| Archive | Target | Runner |
|---|---|---|
| `linux-amd64` | `x86_64-unknown-linux-musl` | `ubuntu-24.04` |
| `linux-arm64` | `aarch64-unknown-linux-musl` | `ubuntu-24.04-arm` |
| `macos-amd64` | `x86_64-apple-darwin` | `macos-latest` |
| `macos-arm64` | `aarch64-apple-darwin` | `macos-latest` |
| `windows-amd64` | `x86_64-pc-windows-msvc` | `windows-latest` |
| `windows-arm64` | `aarch64-pc-windows-msvc` | `windows-11-arm` |
| `freebsd-amd64` | `x86_64-unknown-freebsd` | FreeBSD 15.1 VM on `ubuntu-24.04` |

Linux binaries link musl statically. Both Linux archives are extracted and exercised in Alpine
3.23 containers of the matching architecture, with bash and Python installed for the checks
and no glibc compatibility package. Alpine uses these Linux archives.

The FreeBSD archive is built natively and checked on FreeBSD 15.1 amd64. Its binary uses the
FreeBSD base-system libraries; it does not require Rust at runtime. The FreeBSD and Alpine
archive checks cover MCP initialization and tool discovery, file write/edit/read, pipe exec,
PTY input and resizing, and daemon tasks carried between sessions.

Windows binaries link the C runtime statically (`.cargo/config.toml`) and embed the ConPTY
of their architecture. macOS binaries are unsigned.

Normal CI also compiles and runs the full suite natively on Alpine 3.23 amd64/arm64 with Rust
1.92 (the minimum supported version), and on FreeBSD 15.1 amd64 with the packaged Rust
toolchain. Linux, macOS, and both Windows architectures retain their existing CI coverage.
