#!/bin/bash

# Simulates network partition by blocking UDP traffic on port 8080 for a specific host
# This will cause other nodes to suspect the "partitioned" node

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

if [ $# -ne 2 ]; then
    echo "Usage: $0 <host_number> <duration_seconds>"
    echo "Example: $0 3 10  # Block host 3 for 10 seconds"
    exit 1
fi

HOST_NUMBER=$1
DURATION=$2
PORT=8080

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

echo "🚫 Simulating network partition for $TARGET_HOST"
echo "📍 Blocking UDP traffic on port $PORT for $DURATION seconds"

# Block incoming UDP traffic on the target host
ssh -T "$REMOTE_USER@$TARGET_HOST" "
    echo 'Blocking UDP traffic on port $PORT...'
    sudo iptables -A INPUT -p udp --dport $PORT -j DROP
    echo 'Traffic blocked. Waiting $DURATION seconds...'
    sleep $DURATION
    echo 'Removing traffic block...'
    sudo iptables -D INPUT -p udp --dport $PORT -j DROP
    echo 'Network partition simulation complete.'
"

echo "✅ Network partition simulation completed for $TARGET_HOST"
