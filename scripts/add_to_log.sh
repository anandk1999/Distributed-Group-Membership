#!/bin/bash

# Appends a small block of text to the machine.N.log on each VM.
# Uses REMOTE_USER from .env if present.
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"   # list of hostnames

read -r -d '' TEXT_TO_WRITE <<'EOF'
hi
hello
bye
goodbye
Hi
HI
HELLO
BYE
GOODBYE
EOF

# Loop through all hosts in parallel
for HOST in $(cat $HOSTS_FILE); do
  if [ -n "$HOST" ]; then
    (
  echo ">>> Writing to log file in $HOST"
  ssh "$REMOTE_USER@$HOST" "
        host=\$(hostname)
        num=\${host#fa25-cs425-02}
        num=\${num%%.*}
        num=\$((10#\$num))
        cat >> ./mp1-g02/machine.\$num.log <<EOT
    $TEXT_TO_WRITE
    EOT
        " 
    ) &
  fi
done < "$HOSTS_FILE"
wait
