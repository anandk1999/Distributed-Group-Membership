#!/bin/bash

# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

# Usage: ./ssh_into.sh 02
# Argument should be a number between 01 and 10

if [ -z "$1" ]; then
  echo "Usage: $0 <machine_number 01-10>"
  exit 1
fi

MACH_NUM=$(printf "%02d" $1)   # zero-pad to 2 digits if needed
HOST="fa25-cs425-02${MACH_NUM}.cs.illinois.edu"

ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$HOST"

