# macOS memory telemetry

`home-memory.plugin` reports pressure, compressor storage, swap, interval paging rates, and the process footprint of the Codex desktop app when its standard bundle is present. It uses Python's standard library and macOS command-line tools.

```sh
python3 home-memory.plugin --once
python3 home-memory.plugin 60 --statsd-once
python3 -B -m unittest discover -s . -p 'test_*.py'
```

`--once` prints one sample. `--statsd-once` stores local history and sends numeric gauges to the loopback StatsD listener on UDP 8125. Invoke it from a scheduler every minute. Its nonblocking lock prevents overlapping collection. Without either flag it speaks the Netdata external-plugin protocol; choose one scheduled mode for each state directory.

History is kept under `~/.config/home-server-memory-monitor` by default. It includes the app's runtime path and process metadata, so keep it private. The collector limits local history by age and size. A first sample or a sample after a reboot can lack rate data. StatsD consumers should check the age and quality of a sample independently. Compressor memory, process footprints, and guest memory can overlap in host accounting.
