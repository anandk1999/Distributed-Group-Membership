#!/bin/bash

# Deletes a handful of common files and directories on every VM (careful!).
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"

TARGETS=("go" "go.mod" "*.log" "mp1-g02" "setup.log" "setup.sh" "tmp")  # edit as needed

for HOST in $(cat "$HOSTS_FILE"); do
  (
    echo ">>> Cleaning specified files on $HOST"
    ssh "$REMOTE_USER@$HOST" "
      for item in ${TARGETS[@]}; do
        if [ -e \$HOME/\$item ]; then
          echo \"Removing \$HOME/\$item\"
          rm -rf \$HOME/\$item
        fi
      done
    "
  ) &
done
wait