#!/bin/bash

# Runs the local setup.sh on every host with nohup so it continues on the VM.
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"

for HOST in $(cat "$HOSTS_FILE"); do
  (
    echo ">>> Running setup script on $HOST"
  ssh "$REMOTE_USER@$HOST" "nohup ./setup.sh > setup.log 2>&1 &" </dev/null
  ) &
done

wait   # wait for all SSH sessions to be started