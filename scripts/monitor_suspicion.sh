#!/bin/bash

# Monitors logs for suspicion-related events across all nodes
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"

echo "🔍 Monitoring suspicion events across all nodes..."
echo "📋 Press Ctrl+C to stop monitoring"
echo ""

# Read hosts into array
HOSTS=()
while IFS= read -r line; do
    if [ -n "$(echo "$line" | tr -d '[:space:]')" ]; then
        HOSTS+=("$line")
    fi
done < "$HOSTS_FILE"

# Function to monitor logs from a single host
monitor_host() {
    local host=$1
    local host_num=$2
    
    ssh -T "$REMOTE_USER@$host" "
        cd mp2-g02 2>/dev/null || cd /home/$REMOTE_USER/mp2-g02 2>/dev/null || exit 1
        tail -f node.log 2>/dev/null | while read line; do
            # Filter for suspicion-related messages
            if echo \"\$line\" | grep -iE '(suspect|suspicion|incarnation|alive|refut)' >/dev/null; then
                echo \"[Host $host_num - $host] \$line\"
            fi
        done
    " &
    
    echo $! # Return the PID of the background process
}

# Start monitoring all hosts
PIDS=()
for i in "${!HOSTS[@]}"; do
    HOST_NUM=$((i + 1))
    PID=$(monitor_host "${HOSTS[$i]}" "$HOST_NUM")
    PIDS+=($PID)
    echo "📡 Started monitoring Host $HOST_NUM (${HOSTS[$i]})"
    sleep 0.2
done

echo ""
echo "🎯 All nodes being monitored. Look for:"
echo "   • Suspect messages (when a node is suspected)"
echo "   • Alive messages (when a node refutes suspicion)"
echo "   • Incarnation number changes"
echo ""

# Wait for user interrupt
trap 'echo ""; echo "🛑 Stopping monitors..."; for pid in "${PIDS[@]}"; do kill $pid 2>/dev/null; done; exit 0' INT

# Keep the script running
while true; do
    sleep 1
done
