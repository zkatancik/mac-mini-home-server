# Native media normalizer

This macOS worker inspects a Sonarr/Radarr-managed library for Apple TV playback compatibility. It uses FFmpeg and FFprobe and encodes with VideoToolbox. Encoding is serial and FFmpeg decoder and filter threads are bounded. It checks playback and file stability, creates a rollback copy or hardlink, and validates streams and quality before replacing an eligible file.

```sh
python3 scripts/media-normalizer.py --help
python3 scripts/media-normalizer.py audit --json
python3 scripts/media-normalizer.py normalize --path "$sample_file"
python3 -B -m unittest discover -s tests -p 'test_*.py'
```

Set `SONARR_API_KEY` and `RADARR_API_KEY` in a private `~/.config/home-server-media-normalizer/.env` or in the process environment. Configure `MEDIA_NORMALIZER_SONARR_URL`, `MEDIA_NORMALIZER_RADARR_URL`, and `MEDIA_NORMALIZER_TAUTULLI_URL` when the service URLs differ from their loopback defaults. `MEDIA_NORMALIZER_TAUTULLI_CONFIG` can point to a private Tautulli INI. `MEDIA_NORMALIZER_FFMPEG` and `MEDIA_NORMALIZER_FFPROBE` override executable paths. For container paths, set `MEDIA_NORMALIZER_PATH_MAPPINGS` to comma-separated `container_prefix=host_prefix` pairs with absolute paths.

`poll` and `normalize` preview by default and can run inspection probes. Test against a copy of a sample library. To enable changes, set `MEDIA_NORMALIZER_LIBRARY_ROOT` to the intended absolute host directory and pass `--apply`. Keep the state directory, rollback root, and lock outside the checkout. Set `MEDIA_NORMALIZER_ROLLBACK_ROOT` to storage with room for original files. If other tools mutate the same library, coordinate with them through `MEDIA_MUTATION_LOCK_PATH` or schedule separate windows.

To use [memory admission](../../memory/coordinator/README.md), set `MEDIA_NORMALIZER_MEMORY_COORDINATOR_URL`, `MEDIA_NORMALIZER_MEMORY_CLIENT_FILE`, and `MEDIA_NORMALIZER_MEMORY_TOKEN_FILE` to your local coordinator and prepare the private `~/.config/home-server-media-normalizer` directory with mode 0700. `MEDIA_NORMALIZER_MEMORY_PEAK_MIB` is the measured peak declaration. The admission wait is capped at 60 seconds while holding the mutation lock. A scheduler may invoke `retry-deferred --apply` with admission configured to retry unfinished polls. Choose schedules locally and retain runtime state between runs.
