//! Platform setup shared by the process-level integration tests.

/// Let the production Windows directory creator establish an owner-only runtime root. A
/// normal mkdir can inherit a Users ACE from a CI runner's system TEMP directory, which the
/// production safety check must reject. `status` creates the paths but never starts a daemon.
#[cfg(windows)]
pub fn create_private_runtime(path: &std::path::Path) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_fastexec"))
        .args(["daemon", "status"])
        .env("FASTEXEC_TEST_RUNTIME_DIR", path)
        .env(
            "FASTEXEC_NAMESPACE",
            format!("private-fixture-{}", std::process::id()),
        )
        .env_remove("FASTEXEC_DAEMON")
        .env_remove("FASTEXEC_OUTPUT_MODE")
        .env_remove("FASTEXEC_STRUCTURED_CONTENT")
        .stdin(std::process::Stdio::null())
        .output()
        .expect("the binary creates the fixture's private runtime root");
    let out = String::from_utf8_lossy(&output.stdout);
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "cannot create private runtime: {out}{err}"
    );
    assert!(
        out.starts_with("No fastexec daemon"),
        "the fixture unexpectedly reached a daemon: {out}{err}"
    );
    // Tests that inspect namespace counts start with no bootstrap namespace of their own.
    let namespaces = path.join("ns");
    if namespaces.exists() {
        std::fs::remove_dir_all(namespaces).unwrap();
    }
}
