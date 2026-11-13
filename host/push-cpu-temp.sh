#!/bin/sh
# Publish the first valid Celsius reading to a local StatsD listener.
set -eu
sensor=${CPU_TEMP_COMMAND:-/opt/homebrew/bin/osx-cpu-temp}
if [ ! -x "$sensor" ]; then
    echo 'Set CPU_TEMP_COMMAND to an installed Celsius sensor executable.' >&2
    exit 1
fi
sample=$("$sensor")
temperature=$(printf '%s\n' "$sample" |
    LC_ALL=C /usr/bin/awk '/^[0-9]+([.][0-9]+)?[[:space:]]*(°)?[Cc][[:space:]]*$/ { print $1 + 0; exit }')
[ -n "$temperature" ] || exit 0
printf 'cpu.temp:%s|g\n' "$temperature" | /usr/bin/nc -u -w 1 127.0.0.1 8125
