#!/bin/bash

# Simple script to start distributed membership system
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"
HOSTS_FILE="../hosts.txt"
PORT="8080"

# Read hosts
HOSTS=($(cat "$HOSTS_FILE"))

# 1. Ask for introducer
echo "Select introducer host:"
for i in "${!HOSTS[@]}"; do
    echo "  $((i + 1)). ${HOSTS[$i]}"
done
read -p "Enter number: " choice
INTRODUCER="${HOSTS[$((choice - 1))]}"

# Get introducer IP
INTRODUCER_IP=$(ssh "$REMOTE_USER@$INTRODUCER" "hostname -I | awk '{print \$1}'" | tr -d '[:space:]')

# Start introducer
echo "Starting introducer on $INTRODUCER..."
ssh "$REMOTE_USER@$INTRODUCER" "
    if lsof -i UDP:$PORT >/dev/null 2>&1; then
        echo 'Port $PORT already in use on $INTRODUCER - skipping'
    else
        cd mp2-g02
        nohup ./mp2-node -port $PORT -is-introducer -mode pingack > node.log 2>&1 &
        echo 'Started introducer on $INTRODUCER'
    fi
" &

sleep 2

# 2. Ask for normal nodes
echo "Select normal nodes (space-separated numbers, or ENTER for all):"
OTHER_HOSTS=()
for i in "${!HOSTS[@]}"; do
    if [ "${HOSTS[$i]}" != "$INTRODUCER" ]; then
        OTHER_HOSTS+=("${HOSTS[$i]}")
        echo "  ${#OTHER_HOSTS[@]}. ${HOSTS[$i]}"
    fi
done

read -p "Enter numbers: " selected
if [ -z "$selected" ]; then
    NORMAL_NODES=("${OTHER_HOSTS[@]}")
else
    NORMAL_NODES=()
    for num in $selected; do
        NORMAL_NODES+=("${OTHER_HOSTS[$((num - 1))]}")
    done
fi

# Start normal nodes in parallel
echo "Starting ${#NORMAL_NODES[@]} normal nodes..."
for host in "${NORMAL_NODES[@]}"; do
    ssh "$REMOTE_USER@$host" "
        if lsof -i UDP:$PORT >/dev/null 2>&1; then
            echo 'Port $PORT already in use on $host - skipping'
        else
            cd mp2-g02
            nohup ./mp2-node -port $PORT -introducer $INTRODUCER_IP:$PORT -mode pingack -foreground > node.log 2>&1 &
            echo 'Started normal node on $host'
        fi
    " &
done

wait
echo "All nodes started!"
echo "Introducer: $INTRODUCER ($INTRODUCER_IP)"
echo "Normal nodes: ${#NORMAL_NODES[@]}"
