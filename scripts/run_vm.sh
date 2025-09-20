#!/bin/bash

# Powers on each VM via the campus tooling using expect. Requires `pass` set up
# with an entry named uiuc-vm-auth that stores the password.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

PASS_ENTRY="uiuc-vm-auth"
PASSWORD=$(pass show "$PASS_ENTRY")
if [[ -z "$PASSWORD" ]]; then
  echo "Error: no password found in pass entry '$PASS_ENTRY'"
  exit 1
fi

HOSTS_FILE="../hosts.txt"

# Loop through each VM host
for HOST in $(cat "$HOSTS_FILE"); do
  [[ -z "$HOST" ]] && continue   # skip blank lines

  echo "➡️ Powering on $HOST ..."

expect <<END_EXPECT
  spawn ssh -T "$REMOTE_USER@linux.ews.illinois.edu" cs-vmfarm-poweron

  # First prompt: username
  expect -re "User:? *"
  send "$REMOTE_USER\r"

  # Second prompt: password
  expect -re "Password for user $REMOTE_USER:? *"
  log_user 0
  send "$PASSWORD\r"
  log_user 1

  # Third prompt: VM hostname
  expect -re ".*VM.*"
  send "$HOST\r"

  expect eof
END_EXPECT

done

echo "🎉 All VMs processed!"
