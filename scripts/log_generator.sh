#!/bin/bash

# Runs the Go log generator on each host to create machine.N.log files.
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"

for HOST in $(cat "$HOSTS_FILE"); do
  if [ -n "$HOST" ]; then
    (
      num="${HOST#fa25-cs425-02}"   
      num="${num%%.*}"              
      num=$((10#$num))              
      echo ">>> Execute log_generator.go on $HOST"
      ssh -T "$REMOTE_USER@$HOST" "cd mp1-g02 && go run log_generator.go $num"
    ) &
  fi
done < "$HOSTS_FILE"

wait
echo "✅ Checked all hosts."
