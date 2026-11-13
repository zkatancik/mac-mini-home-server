# macOS host utilities

These utilities run on macOS under launchd or from a terminal. Choose installation paths and dependencies locally, and validate each component before scheduling it. The examples require no cloud services.

## Sleep and temperature

- `launchd/local.example.caffeinate.plist` keeps the login session's server available with `caffeinate -is`. The system-sleep assertion applies on AC power, and the display follows its own energy settings. Set the power-loss restart behavior separately in macOS Energy settings.
- `push-cpu-temp.sh` sends one Celsius gauge to a StatsD receiver on loopback. Set `CPU_TEMP_COMMAND` to the absolute path of a compatible sensor executable that prints a value such as `42.1°C`. Test that the sensor supports your hardware. The disabled plist template uses a 60-second sample interval. Render its script path, add the sensor executable under `EnvironmentVariables` if required, and enable it in the intended login domain after reviewing the result.

## OpenZFS readiness

Render `zfs-pool-ready.sh.template` with absolute ZFS binary paths, the expected pool name, its decimal GUID and its mount path. Use paths without spaces, shell metacharacters or symlinks. Read the GUID from your installed ZFS tools. Keep the rendered script, the ZFS executables and their parent directories under administrator control. Keep your rendered configuration local.

The read-only `--check` verifies the exact pool identity, its mountpoint and the automatic datasets that should be mounted. Run it as the account that will use the Docker startup check. The administrator-only `--mount` mode first uses a normal, non-forced import of the specified pool by GUID and then mounts automatic datasets within the expected root. The helper refuses an unexpected GUID or mountpoint. Configure how your OpenZFS installation discovers disks before testing a cold boot. When using the optional LaunchDaemon template, render the absolute helper path and keep its installation disabled until the check and mount modes have been validated locally.

## External Docker Desktop storage

Use Docker Desktop's supported settings UI to move its data disk. Turn off Docker Desktop's automatic login startup if an external-volume check should gate startup. Install a trusted, root-owned readiness helper that accepts `--check`. Its read-only check should verify the exact expected volume identity and complete promptly without daemonizing or detaching subprocesses. The OpenZFS helper is one implementation.

Run the check as the Docker Desktop login account on macOS with Python 3.10 or later. Create a private state parent in that account first.

```sh
install -d -m 700 "$HOME/Library/Application Support/home-server"
python3 host/docker_storage_location.py \
  --external-mount /Volumes/ServerData \
  --disk-directory containers \
  --readiness-helper /Library/PrivilegedHelperTools/local.example.zfs-pool-ready
```

Replace these paths with your own absolute mounted-volume and helper paths and the directory containing `Docker.raw` relative to that mount. The account's home and Docker Desktop settings must be on the internal Data volume. The checker verifies the settings before accessing the external storage. It requires a matching explicit `DataFolder`, `AutoStart=false`, a successful helper check, a separate mounted filesystem and an existing account-owned regular `Docker.raw`. Docker Desktop may change its settings schema between releases. Validate these checks after upgrading and allow the launchd job to read the Docker settings through macOS privacy controls as needed.

An isolated worker performs the storage probes under a 25-second deadline. A four-second cleanup window checks whether its owned process group drained. If cleanup cannot be confirmed, later attempts use the recorded worker and boot identity to decide when a fresh probe is safe. The private state directory contains local process and boot metadata. Keep its files private and preserve them through a deferred attempt.

Exit status zero means an operator-owned recovery job can proceed with its next check. On a nonzero result, defer startup and retry from the next scheduled invocation. Place the gate before any Docker launch or recovery action that depends on the volume. The checker itself reads settings and file metadata, invokes the helper in check mode and writes only its private supervisor state. Validate startup, Docker context and bind mounts separately before allowing automated container recovery.

## Local verification

```sh
plutil -lint host/launchd/*.plist*
/bin/sh -n host/push-cpu-temp.sh
/bin/sh -n host/zfs-pool-ready.sh.template
python3 -B -m unittest discover -s host/tests
```
