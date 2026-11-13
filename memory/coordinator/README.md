# Memory admission coordinator

The coordinator serializes explicitly wrapped jobs through one local admission
slot. Its client starts the requested command after grant, renews ownership
while it runs, and releases after it can verify that the command has stopped.
Other processes and containers continue to use the host normally.

The example uses capacity-only admission. macOS memory pressure and swap are
observed. A guest job needs a recent `MemAvailable` sample, its declared
peak, and the configured guest reserve. Native jobs skip the guest-capacity
check. The peak is a scheduling declaration. Memory enforcement, if needed,
belongs to the workload's runtime.

## Try it on macOS

Use Python 3 and choose a new private state directory. Render
`config.example.json` into that directory as `config.json`, replacing
`__STATE_DIR__` with its absolute path. The following assumes
`state_dir` is set to that path and that the shell is in this directory.

```sh
umask 077
mkdir -m 700 "$state_dir"
python3 -c 'import secrets; print(secrets.token_urlsafe(48))' > "$state_dir/token"
# Render config.json here before initializing.
python3 coordinator.py --config "$state_dir/config.json" --initialize-state
python3 coordinator.py --config "$state_dir/config.json"
```

Use `--initialize-state` only for a new deployment. Keep the state and its
lock together across restarts and upgrades.

In another shell, set `state_dir` to the same directory and try a native job.

```sh
python3 client.py --url http://127.0.0.1:8765 --token-file "$state_dir/token" \
  run --native --job example --peak-mib 64 --wait-timeout 30 --run-timeout 30 \
  -- /usr/bin/true
```

Use the client's `status` subcommand to inspect the queue. Keep the API
on loopback and keep the bearer token in the private directory. The included
LaunchAgent template takes absolute values for `__PYTHON__`,
`__SOURCE_DIR__`, and `__STATE_DIR__`. Give it a label in your own namespace.
Start one coordinator per state directory.

## Guest jobs and ownership

For Docker guests, add `docker_path` (absolute CLI path) and
`probe_container` (a running container in the VM that can read
`/proc/meminfo`) to the local config. Route only the intended clients to
the coordinator. A file bridge is available for a client that shares a
private directory with the host and cannot connect over HTTP. Configure
`bridge_dir` on the host and use a `file://` URL rooted at the client's
view of the same directory. Share the token only with trusted clients.

A queued client has a wait deadline and periodically confirms that it is
still present. A grant carries an owner lease and a run deadline. If a
running owner disappears, the coordinator retains its slot until stop is
confirmed. The client can write a private `--receipt-file` to preserve the
request's lifecycle across interruptions. Use `resolve` only after checking
that the exact workload and its descendants have stopped. Restarting the
coordinator preserves ownership.

## Test

```sh
PYTHONPATH=. python3 -B -m unittest discover -s tests -p 'test_*.py'
```

The fixtures use temporary directories, simulated probes and loopback test
servers.
