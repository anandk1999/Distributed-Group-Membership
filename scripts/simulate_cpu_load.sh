#!/bin/bash

# Simulates high CPU load to make a node slow to respond
# This may trigger suspicion due to delayed responses

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

if [ $# -ne 2 ]; then
    echo "Usage: $0 <host_number> <duration_seconds>"
    echo "Example: $0 4 20  # Overload host 4 for 20 seconds"
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

echo "🔥 Simulating CPU overload for $TARGET_HOST"
echo "📍 Creating high CPU load for $DURATION seconds"

# Create CPU load on the target host
ssh -T "$REMOTE_USER@$TARGET_HOST" "
    echo 'Starting CPU stress test...'
    
    # Create CPU load using multiple background processes
    for i in {1..4}; do
        yes > /dev/null &
        PIDS[\$i]=\$!
    done
    
    echo 'CPU load created. Waiting $DURATION seconds...'
    sleep $DURATION
    
    echo 'Stopping CPU stress test...'
    for pid in \"\${PIDS[@]}\"; do
        kill \$pid 2>/dev/null
    done
    
    echo 'CPU overload simulation complete.'
"

echo "✅ CPU overload simulation completed for $TARGET_HOST"
