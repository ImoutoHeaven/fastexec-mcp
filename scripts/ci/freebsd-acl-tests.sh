#!/bin/sh
# Exercise native ACLs on disposable filesystems, including an ordinary user's writes.
set -eu

[ "$(uname -s)" = FreeBSD ] && [ "$(id -u)" = 0 ] || {
    echo "FreeBSD ACL fixtures require a FreeBSD root process" >&2
    exit 1
}
for tool in cargo python3 mdconfig newfs mount umount zpool zfs truncate timeout su getfacl setfacl; do
    command -v "$tool" >/dev/null || { echo "Missing ACL fixture prerequisite: $tool" >&2; exit 1; }
done
[ "$(id -u nobody)" = 65534 ] || { echo "The ACL fixtures require nobody (uid 65534)" >&2; exit 1; }

cargo test --locked --bin fastexec --no-run --message-format=json > ci-freebsd-acl-build.log
unit_binary=$(python3 - <<'PY'
import json
from pathlib import Path

artifacts = [json.loads(line) for line in Path("ci-freebsd-acl-build.log").read_text().splitlines()]
executables = {item["executable"] for item in artifacts
               if item.get("reason") == "compiler-artifact"
               and item.get("profile", {}).get("test")
               and item.get("target", {}).get("name") == "fastexec"
               and item.get("executable")}
if len(executables) != 1:
    raise SystemExit("Expected one fastexec unit-test executable")
print(executables.pop())
PY
)
"$unit_binary" freebsd_acl_ concurrent_private_directory_creators_can_immediately_create_contents \
    private_directory_publication_preserves_existing_entries --list > ci-freebsd-acl-tests.log
expected_tests=$(grep -c ': test$' ci-freebsd-acl-tests.log) || {
    echo "No FreeBSD ACL tests were compiled" >&2
    exit 1
}
[ "$expected_tests" = 7 ] || {
    echo "Expected all seven FreeBSD ACL and private-directory tests; found $expected_tests" >&2
    exit 1
}

cleanup_fixture() {
    fixture_status=$?
    trap - 0 HUP INT TERM
    set +e
    cleanup_failed=0
    if [ "$fixture_kind" = ufs ]; then
        devices=
        if [ -f "$fixture_image" ]; then
            devices=$(mdconfig -l -f "$fixture_image") || cleanup_failed=1
        fi
        if mount -p > "$fixture_root/mounts"; then
            mounted=$(awk -v path="$fixture_mount" '$2 == path { print $1 }' "$fixture_root/mounts")
            if [ -n "$mounted" ]; then
                owned=0
                for device in $devices; do
                    [ "$mounted" = "/dev/$device" ] && owned=1
                done
                if [ "$owned" = 1 ]; then
                    umount "$fixture_mount" || cleanup_failed=1
                else
                    echo "Refusing to unmount an unexpected filesystem at $fixture_mount" >&2
                    cleanup_failed=1
                fi
            fi
        else
            cleanup_failed=1
        fi
        for device in $devices; do
            mdconfig -d -u "$device" || cleanup_failed=1
        done
    else
        if zpool list -H -o name > "$fixture_root/pools"; then
            if grep -Fxq "$fixture_pool" "$fixture_root/pools"; then
                if zpool status -P "$fixture_pool" > "$fixture_root/pool-status" &&
                   awk -v image="$fixture_image" '$1 == image { found=1 } END { exit !found }' "$fixture_root/pool-status"; then
                    zpool destroy "$fixture_pool" || cleanup_failed=1
                else
                    echo "Refusing to destroy a pool without the owned vdev: $fixture_pool" >&2
                    cleanup_failed=1
                fi
            fi
        else
            cleanup_failed=1
        fi
    fi
    if [ "$cleanup_failed" = 0 ]; then
        rm -rf "$fixture_root" || cleanup_failed=1
    fi
    if [ "$cleanup_failed" != 0 ]; then
        echo "ACL fixture cleanup failed; retained $fixture_root for inspection" >&2
        [ "$fixture_status" != 0 ] || fixture_status=1
    fi
    exit "$fixture_status"
}

prepare_fixture() {
    fixture_label=$1
    fixture_kind=$2
    fixture_root=$(mktemp -d "/tmp/fastexec-acl-$fixture_label.XXXXXX")
    fixture_image="$fixture_root/fs.img"
    fixture_mount="$fixture_root/mount"
    fixture_pool="fastexec_acl_${fixture_root##*.}_$$"
    trap cleanup_fixture 0
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    chmod 755 "$fixture_root"
    mkdir "$fixture_mount"
}

run_logged() {
    case_log="ci-freebsd-acl-$fixture_label-$1.log"
    printf 'Testing %s ACLs as %s\n' "$fixture_label" "$1"
    shift
    if "$@" > "$case_log" 2>&1; then
        cat "$case_log"
        grep -Fq "test result: ok. $expected_tests passed; 0 failed; 0 ignored;" "$case_log" || {
            echo "The ACL fixture did not run every required test: $case_log" >&2
            return 1
        }
    else
        case_status=$?
        cat "$case_log"
        return "$case_status"
    fi
}

run_fixture_tests() {
    chmod 755 "$fixture_mount"
    mkdir "$fixture_mount/tests" "$fixture_mount/nobody-tests"
    chmod 755 "$fixture_mount/tests" "$fixture_mount/nobody-tests"
    chown nobody "$fixture_mount/nobody-tests"
    # A checkout under /root is inaccessible to nobody; keep the test image off the 64 MiB UFS.
    cp "$unit_binary" "$fixture_root/unit-tests"
    chmod 755 "$fixture_root/unit-tests"
    required_denied_acl=0
    [ "$fixture_label" != zfs-restricted ] || required_denied_acl=1
    run_logged root env TMPDIR="$fixture_mount/tests" FASTEXEC_REQUIRE_ACL_TEST=1 \
        FASTEXEC_REQUIRE_DENIED_ACL_TEST=0 \
        timeout 600 cargo test --locked --bin fastexec -- freebsd_acl_ \
        concurrent_private_directory_creators_can_immediately_create_contents \
        private_directory_publication_preserves_existing_entries --nocapture
    run_logged nobody timeout 600 su -m nobody -c \
        "env TMPDIR='$fixture_mount/nobody-tests' FASTEXEC_REQUIRE_ACL_TEST=1 FASTEXEC_REQUIRE_DENIED_ACL_TEST=$required_denied_acl '$fixture_root/unit-tests' freebsd_acl_ concurrent_private_directory_creators_can_immediately_create_contents private_directory_publication_preserves_existing_entries --nocapture"
}

run_ufs() (
    prepare_fixture "$1" ufs
    truncate -s 64m "$fixture_image"
    device=$(mdconfig -a -t vnode -f "$fixture_image")
    newfs -U "/dev/$device"
    mount -t ufs -o "$2" "/dev/$device" "$fixture_mount"
    run_fixture_tests
)

run_zfs() (
    prepare_fixture "zfs-$1" zfs
    truncate -s 256m "$fixture_image"
    # An existing pool must never be claimed by this run, even if its random name collides.
    zpool list -H -o name > "$fixture_root/pools"
    if grep -Fxq "$fixture_pool" "$fixture_root/pools"; then
        echo "ACL fixture pool already exists: $fixture_pool" >&2
        exit 1
    fi
    zpool create -o cachefile=none -m "$fixture_mount" \
        -O acltype=nfsv4 -O aclmode="$1" -O aclinherit=passthrough \
        "$fixture_pool" "$fixture_image"
    zfs get acltype,aclmode,aclinherit "$fixture_pool"
    run_fixture_tests
)

run_ufs ufs-posix acls
run_ufs ufs-nfsv4 nfsv4acls
run_zfs passthrough
run_zfs restricted
echo "PASS: UFS POSIX/NFSv4 and ZFS passthrough/restricted ACLs (root and nobody)"
