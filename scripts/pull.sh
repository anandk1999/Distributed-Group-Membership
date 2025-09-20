#!/bin/bash

# Runs git pull inside the repo on each host.
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"
for HOST in $(cat $HOSTS_FILE); do
  (
    echo ">>> Pulling new code from repo on $HOST"
  ssh "$REMOTE_USER@$HOST" "cd mp1-g02 && git pull"
  ) &
done
wait