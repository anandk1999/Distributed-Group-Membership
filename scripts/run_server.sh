#!/bin/bash

# Starts the server using air on all hosts if port 8080 is free.
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"
PORT=8080

for HOST in $(cat "$HOSTS_FILE"); do
  if [ -n "$HOST" ]; then
    (
  echo ">>> Checking port $PORT on $HOST"
      ssh -T "$REMOTE_USER@$HOST" "
        if lsof -i TCP:$PORT -sTCP:LISTEN >/dev/null 2>&1; then
          echo 'Port $PORT on $HOST is in use. Skipping air.'
        else
          echo 'Port $PORT on $HOST is free. Starting air...'
          cd mp1-g02
          nohup air -c .air.toml > air.log 2>&1 < /dev/null &
        fi
        exit
      "
    ) &
  fi
done < "$HOSTS_FILE"

wait
echo "✅ Checked all hosts."
