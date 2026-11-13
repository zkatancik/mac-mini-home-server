# Docker disposable-image retention

This example previews old dangling images on an explicitly selected local Docker socket. It considers images built with `io.example.home-server.disposable=true`. Add the label only to builds that can be regenerated. Keep rollback images tagged. Preview is the default and sends only inventory commands to the engine.

Set `DOCKER_SOCKET_PATH` in your shell to the absolute path of your local Docker Unix socket, then run:

```sh
python3 prune-disposable-images.py --socket "$DOCKER_SOCKET_PATH"
```

The report includes logical image sizes. Shared layers mean those sizes can exceed the space reclaimed. Run again with `--apply` during an idle build window to request Docker's own dangling-image prune with the positive label and the same age cutoff. The daemon rechecks eligibility against current tags and container references. A changing inventory can change which images it prunes. The script does not issue volume, network, container or build-cache cleanup. Docker documentation describes the [image prune filters](https://docs.docker.com/reference/cli/docker/image/prune/#filtering---filter).
