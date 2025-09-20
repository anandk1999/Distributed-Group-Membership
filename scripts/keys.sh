#!/bin/bash

# Copies your SSH keys to each VM (~/.ssh). Assumes keys exist locally.
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

# File containing list of VM IPs or hostnames (one per line)
HOSTS_FILE="../hosts.txt"

# SSH key to use
SSH_PUBLIC_KEY="$HOME/.ssh/cs_425_ssh.pub"
SSH_PRIVATE_KEY="$HOME/.ssh/cs_425_ssh"

# Loop through hosts
for HOST in $(cat $HOSTS_FILE); do
  (  
    echo ">>> Setting up $HOST"
  scp "$SSH_PRIVATE_KEY" "$REMOTE_USER@$HOST:~/.ssh/"
  scp "$SSH_PUBLIC_KEY" "$REMOTE_USER@$HOST:~/.ssh/"
  ) &
done
wait