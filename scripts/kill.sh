#!/bin/bash

# Kills anything listening on port 8080 across all hosts (plus common dev tools).
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"

for HOST in $(cat "$HOSTS_FILE"); do
  (
    echo ">>> Killing processes on port 8080 at $HOST"
    ssh "$REMOTE_USER@$HOST" '
      pkill -f mp2-node
    '
  ) &
done
wait