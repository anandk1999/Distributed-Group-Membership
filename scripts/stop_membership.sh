#!/bin/bash

# Stops all mp2-node processes on all hosts
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"

echo "🛑 Stopping all membership nodes..."

for HOST in $(cat "$HOSTS_FILE"); do
  if [ -n "$HOST" ]; then
    (
      echo "🛑 Stopping nodes on $HOST"
      ssh -T "$REMOTE_USER@$HOST" "
        pkill -f mp2-node || true
        echo '✅ Stopped all mp2-node processes on $HOST'
        exit
      "
    ) &
  fi
done < "$HOSTS_FILE"

wait
echo "✅ All nodes stopped!"
