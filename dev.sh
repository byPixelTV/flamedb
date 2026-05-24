#!/bin/bash

COUNT=${1:-4}

configs=(
  "config.yml"
  "config2.yml"
  "config3.yml"
  "config4.yml"
)

if [ "$COUNT" -lt 1 ] || [ "$COUNT" -gt "${#configs[@]}" ]; then
  echo "Usage: ./start.sh [1-${#configs[@]}]"
  exit 1
fi

processes=()

for ((i=0; i<COUNT; i++)); do
  ./flamedb "${configs[$i]}" &
  processes+=($!)
done

cleanup() {
  for pid in "${processes[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid"
    fi
  done
}

trap cleanup EXIT INT TERM

wait