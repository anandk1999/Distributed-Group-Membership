#!/bin/bash

# Kills anything listening on port 8080 across all hosts (plus common dev tools).
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

if [ -z "$1" ]; then
  echo "Usage: $0 <machine_number 01-10>"
  exit 1
fi

if [ -z "$2" ]; then
  echo "Usage: $0 $1 IP:port"
  exit 1
fi

MACH_NUM=$(printf "%02d" $1)   # zero-pad to 2 digits if needed
HOST="fa25-cs425-02${MACH_NUM}.cs.illinois.edu"

echo ">>> Starting process on port 8080 at $HOST"
ssh "$REMOTE_USER@$HOST" "cd mp2-g02 && ./mp2-node -port 8080 -introducer $2 -mode pingack"