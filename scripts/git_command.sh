#!/bin/bash

# Runs a git (or any) command inside the mp1-g02 repo on each host.
# Example: ./git_command.sh "git status -sb"
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

# Ask user for the command to run on all hosts
read -p "Enter the command to run on all hosts: " USER_CMD
if [[ -z "$USER_CMD" ]]; then
  echo "No command entered. Exiting."
  exit 1
fi

HOSTS_FILE="../hosts.txt"
for HOST in $(cat $HOSTS_FILE); do
  (
    echo ">>> Running $USER_CMD in repo on $HOST"
  ssh "$REMOTE_USER@$HOST" "cd mp1-g02 && $USER_CMD"
  ) &
done
wait