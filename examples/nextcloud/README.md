# Nextcloud worker and cron example

`apache-memory.conf` caps concurrent Apache prefork workers and recycles them. Confirm the web container uses prefork with embedded PHP and measure peak worker memory before tuning its request budget. Other Apache MPMs and PHP-FPM need their own limits.

`cron-loop.py` lets one admitted `cron.php` finish before another attempt can queue. Each result, including a queue timeout, is followed by a configurable delay. SIGTERM and SIGINT go to the admission client so it can stop its owned PHP process group before releasing capacity. A lifecycle receipt must persist across container restarts. Use one replica and one active cron schedule for the same Nextcloud instance.

The Compose example is an overlay for an existing installation. Merge the application, config and data mounts, network, and dependencies with your base project. Keep the web and cron images compatible and provide Python 3 and PHP in the cron image. Configure a coordinator endpoint accessible to that container over a private transport and mount the matching client and a readable private token file. For `file://` transport, add a read-write mount for the coordinator bridge directory at the path in the URL. Make the receipt directory writable and private (`0700`) for the effective cron user as seen from the container. Size the guest reservation and queue/run deadlines together with the coordinator's configured limits.

Use `docker compose -f compose.yaml -f compose.memory.example.yaml config --quiet` after setting the required variables in your local environment and adapting service names. Check the resolved service definitions before deployment. The script can preview job budgets independently of Docker, Nextcloud or the coordinator. Omitting `--run` prints the plan without opening the supplied files or executing a workload.

```sh
python3 cron-loop.py \
  --client /opt/admission/client.py --url file:///run/admission-bridge \
  --token-file /run/secrets/admission-token \
  --receipt-file /var/lib/admission/nextcloud-cron.json \
  --cron-file /var/www/html/cron.php --peak-mib 768 \
  --run-timeout 900 --wait-timeout 3600 --interval 600
```
