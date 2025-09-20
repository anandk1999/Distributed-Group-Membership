#!/bin/bash

# Kills any process listening on port 8080 on a specific VM (by number).
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"
HOSTS_FILE="../hosts.txt"

if [ -z "$1" ]; then
  echo "Usage: $0 <machine_number 01-10>"
  exit 1
fi

MACH_NUM=$(printf "%02d" $1)
HOST="fa25-cs425-02${MACH_NUM}.cs.illinois.edu"


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