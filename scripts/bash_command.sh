#!/bin/bash

# Runs an arbitrary shell command on all hosts listed in hosts.txt.
# Example: ./bash_command.sh 'uname -a'
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"

# Make sure user passed a command
if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <command>"
  echo "Example: $0 'mkdir -p ~/logs && mv ~/vm*.log ~/logs/'"
  exit 1
fi

CMD="$*"

for HOST in $(cat "$HOSTS_FILE"); do
  (
    echo ">>> Executing on $HOST: $CMD"
    ssh "$REMOTE_USER@$HOST" "$CMD"
  ) &
done

wait