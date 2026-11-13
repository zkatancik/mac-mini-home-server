# Mac mini home server

Reusable macOS home-server scripts and configuration examples. Each component can be used independently. Keep rendered configuration, tokens, storage identifiers, and runtime data in a private directory outside the checkout.

- [Host startup and storage](host/README.md) covers a bounded Docker Desktop storage readiness check, OpenZFS readiness, and launchd examples for idle-sleep prevention and temperature metrics.
- [Memory admission](memory/coordinator/README.md) serializes explicitly wrapped jobs. The example observes host memory pressure and checks available guest memory for Docker workloads.
- [Memory telemetry](memory/telemetry/README.md) collects macOS pressure, compression, swap, and Codex desktop measurements when the app is present, for local inspection or StatsD.
- [Media normalization](media/normalizer/README.md) uses native FFmpeg and VideoToolbox, limits FFmpeg threads, and can use the admission coordinator.
- [Container examples](examples/README.md) cover a serial Nextcloud cron wrapper, Apache worker sizing, and Docker image retention.

On a new machine, validate external storage before starting workloads that depend on it. Establish a memory baseline, measure each job's peak, and choose a budget that leaves room for interactive use. Keep credentials outside Git and use a separate state directory per deployment. Use each component's read-only or fixture checks before enabling a schedule. `plutil -lint FILE` checks a LaunchAgent, and `plutil -extract ProgramArguments json -o - FILE` prints its command without rewriting the plist.
