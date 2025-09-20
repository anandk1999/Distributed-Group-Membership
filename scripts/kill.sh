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
      PIDS=$(lsof -ti:8080)
      if [ -n "$PIDS" ]; then
        echo "Found processes: $PIDS"
        # Kill all children too
        for PID in $PIDS; do
          pkill -P "$PID" 2>/dev/null
          kill -9 "$PID" 2>/dev/null
        done
      else
        echo "No process running on port 8080"
      fi

      # Also kill any leftover `air` or `go run` just in case
      pkill -9 -f "air -c .air.toml" 2>/dev/null
      pkill -9 -f "go run" 2>/dev/null
    '
  ) &
done
wait