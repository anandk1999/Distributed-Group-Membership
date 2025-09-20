#!/bin/bash

# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

# Ask for file to transfer
read -p "Enter file name: " file

# Check if file exists locally (only for common mode)
if [[ ! -f "$file" ]]; then
  echo "Warning: File '$file' not found. This is expected if you are doing VM-specific transfer."
fi

# Confirm file with user
read -p "Continue? (Y/N): " confirm && [[ $confirm =~ ^[Yy](es)?$ ]] || exit 1

# Ask if this is common or VM-specific
read -p "Is this a common file for all VMs? (Y/N): " common

HOSTS_FILE="../hosts.txt"

for HOST in $(cat "$HOSTS_FILE"); do
  (
    if [[ $common =~ ^[Yy](es)?$ ]]; then
      # ---------- Common transfer ----------
      remote_file="\$HOME/$(basename "$file")"

      # Check if file already exists on VM
      if ssh "$REMOTE_USER@$HOST" "[ -f $remote_file ]"; then
        echo ">>> Skipping $HOST: $remote_file already exists"
      else
        echo ">>> Transferring common file $file to $HOST"
        scp "$file" "$REMOTE_USER@$HOST:~/"

        # If it's a .sh file, give execute permissions on the VM
        if [[ "$file" == *.sh ]]; then
          echo ">>> Setting execute permission on $HOST"
          ssh "$REMOTE_USER@$HOST" "chmod +x $remote_file"
        fi
      fi

    else
      # ---------- VM-specific transfer ----------
      num="${HOST#fa25-cs425-02}"   # remove prefix
      num="${num%%.*}"              # remove suffix
      num=$((10#$num))              # convert safely to decimal
      vmfile="/Users/ASUS1/Downloads/MP1 Demo Data FA22/vm${num}.log"
      remote_file="\$HOME/$(basename "$vmfile")"

      if [[ ! -f "$vmfile" ]]; then
        echo "Warning: File $vmfile not found, skipping $HOST"
      elif ssh "$REMOTE_USER@$HOST" "[ -f $remote_file ]"; then
        echo ">>> Skipping $HOST: $remote_file already exists"
      else
        echo ">>> Transferring $vmfile to $HOST"
        scp "$vmfile" "$REMOTE_USER@$HOST:~/"
      fi
    fi
  ) &
done

wait