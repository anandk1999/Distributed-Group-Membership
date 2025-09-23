#!/bin/bash

# Simulates a node becoming unresponsive by suspending the process
# This will trigger suspicion from other nodes

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

if [ $# -ne 2 ]; then
    echo "Usage: $0 <host_number> <duration_seconds>"
    echo "Example: $0 2 15  # Suspend host 2 for 15 seconds"
    exit 1
fi

HOST_NUMBER=$1
DURATION=$2

# Read hosts file
HOSTS_FILE="../hosts.txt"
HOSTS=()
while IFS= read -r line; do
    if [ -n "$(echo "$line" | tr -d '[:space:]')" ]; then
        HOSTS+=("$line")
    fi
done < "$HOSTS_FILE"

if [ $HOST_NUMBER -lt 1 ] || [ $HOST_NUMBER -gt ${#HOSTS[@]} ]; then
    echo "❌ Invalid host number. Available hosts: 1-${#HOSTS[@]}"
    exit 1
fi

TARGET_HOST="${HOSTS[$((HOST_NUMBER - 1))]}"

echo "⏸️  Simulating process suspension for $TARGET_HOST"
echo "📍 Suspending mp2-node process for $DURATION seconds"

# Suspend the process on the target host
ssh -T "$REMOTE_USER@$TARGET_HOST" "
    PID=\$(pgrep mp2-node)
    if [ -z \"\$PID\" ]; then
        echo 'No mp2-node process found!'
        exit 1
    fi
    
    echo \"Found mp2-node process with PID: \$PID\"
    echo \"Suspending process for $DURATION seconds...\"
    
    # Suspend the process
    kill -STOP \$PID
    sleep $DURATION
    
    # Resume the process
    kill -CONT \$PID
    echo \"Process resumed. Simulation complete.\"
"

echo "✅ Process suspension simulation completed for $TARGET_HOST"
