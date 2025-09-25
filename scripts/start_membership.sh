#!/bin/bash

# Starts the distributed membership system with one random introducer and remaining as normal nodes
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

HOSTS_FILE="../hosts.txt"
PORT="8080"

# Read all hosts into an array
HOSTS=()
while IFS= read -r line; do
    if [ -n "$(echo "$line" | tr -d '[:space:]')" ]; then
        HOSTS+=("$line")
        echo "Debug: Added host: '$line'"
    fi
done < "$HOSTS_FILE"

if [ ${#HOSTS[@]} -eq 0 ]; then
    echo "❌ No hosts found in $HOSTS_FILE"
    exit 1
fi

echo "📋 Found ${#HOSTS[@]} hosts"
echo "📋 Host list:"
for i in "${!HOSTS[@]}"; do
    echo "  [$((i + 1))] ${HOSTS[$i]}"
done

# Ask user to select introducer
echo ""
echo "🎯 Please select which host should be the introducer:"
while true; do
    read -p "Enter the number (1-10): " USER_INPUT
    
    # Check if input is a valid number between 1 and 10
    if [[ "$USER_INPUT" =~ ^[0-9]+$ ]] && [ "$USER_INPUT" -ge 1 ] && [ "$USER_INPUT" -le 10 ]; then
        # Convert to 0-based index and check if it's within the available hosts
        INTRODUCER_INDEX=$((USER_INPUT - 1))
        if [ "$INTRODUCER_INDEX" -lt ${#HOSTS[@]} ]; then
            break
        else
            echo "❌ Host number $USER_INPUT is not available. Only ${#HOSTS[@]} hosts found."
        fi
    else
        echo "❌ Invalid input. Please enter a number between 1 and 10"
    fi
done

INTRODUCER_HOST="${HOSTS[$INTRODUCER_INDEX]}"
echo "✅ Selected introducer: $INTRODUCER_HOST"

# Get the introducer's IP address for other nodes to connect to
echo "🔍 Getting introducer IP address..."
INTRODUCER_IP=$(ssh -T "$REMOTE_USER@$INTRODUCER_HOST" "hostname -I | awk '{print \$1}'" | tr -d '[:space:]')

if [ -z "$INTRODUCER_IP" ]; then
    echo "❌ Failed to get IP address from introducer host $INTRODUCER_HOST"
    exit 1
fi

echo "📡 Introducer IP: $INTRODUCER_IP"

# Function to start introducer
start_introducer() {
    local host=$1
    echo "🚀 Starting introducer on $host"
    ssh -T "$REMOTE_USER@$host" "
        if lsof -i UDP:$PORT >/dev/null 2>&1; then
            echo '❌ Port $PORT on $host is in use. Killing existing process...'
            pkill -f mp2-node || true
            sleep 2
        fi
        echo '✅ Port $PORT on $host is free. Starting introducer...'
        cd mp2-g02
        nohup ./mp2-node -port $PORT -is-introducer -mode pingack -foreground > node.log 2>&1 < /dev/null &
        sleep 1
        echo '✅ Introducer started on $host'
        exit
    "
}

# Function to start normal node
start_normal_node() {
    local host=$1
    local introducer_addr="${INTRODUCER_IP}:${PORT}"
    echo "🔗 Starting normal node on $host (connecting to $introducer_addr)"
    echo "🔍 Debug - INTRODUCER_IP: '$INTRODUCER_IP', PORT: '$PORT'"
    ssh -T "$REMOTE_USER@$host" "
        if lsof -i UDP:$PORT >/dev/null 2>&1; then
            echo '❌ Port $PORT on $host is in use. Killing existing process...'
            pkill -f mp2-node || true
            sleep 2
        fi
        echo '✅ Port $PORT on $host is free. Starting normal node...'
        cd mp2-g02
        nohup ./mp2-node -port $PORT -introducer \"$introducer_addr\" -mode pingack -foreground > node.log 2>&1 < /dev/null &
        sleep 1
        echo '✅ Normal node started on $host'
        exit
    "
}

# Start introducer first
start_introducer "$INTRODUCER_HOST"

# Wait a moment for introducer to start
echo "⏳ Waiting for introducer to initialize..."
sleep 3

# Start all other nodes as normal nodes
echo "🌐 Starting normal nodes..."
for i in "${!HOSTS[@]}"; do
    if [ $i -ne $INTRODUCER_INDEX ]; then
        start_normal_node "${HOSTS[$i]}"
        # Small delay to avoid overwhelming the introducer
        sleep 0.5
    fi
done

echo ""
echo "🎉 All nodes started!"
echo "📊 Summary:"
echo "   📍 Introducer: $INTRODUCER_HOST ($INTRODUCER_IP:$PORT)"
echo "   🔢 Normal nodes: $((${#HOSTS[@]} - 1))"
echo "   🌍 Total nodes: ${#HOSTS[@]}"
echo ""
echo "📋 To check status, run: ./scripts/check_membership.sh"
echo "📋 To view logs, run: ./scripts/view_logs.sh"
echo "📋 To stop all nodes, run: ./scripts/stop_membership.sh"
