#!/bin/bash
# Live monitoring script - shows SUSPECT events and allows CLI commands
# Usage: ./scripts/monitor_node.sh [host] [port]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"
HOST=${1:-$(hostname)}
PORT=${2:-8080}

# Local monitoring
echo "Monitoring local node on port $PORT"
echo "Commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, quit"
echo "You can add arguments after a command, e.g.: switch -mode gossip -interval 500ms"
echo "Membership events (SUSPECT/FAILED/CLEARED/joins/leaves) will appear automatically below:"
echo "Note: Exact duplicate lines (including timestamp) are filtered out"
echo "----------------------------------------"

# Start background process to show suspects from log
if [ -f "node.log" ]; then
    # Show SUSPECT, FAILED, CLEARED events and member joins/leaves (deduplicated)
    # Start the pipeline in a new process group to make cleanup easier
    setsid bash -c '
        tail -F node.log 2>/dev/null | \
            grep --line-buffered -E "(SUSPECT|FAILED|CLEARED|joined|left|New member)" | \
            awk "!seen[\$0]++ {print \$0; if(length(seen) > 1000) {delete seen}}"
    ' &
    TAIL_PID=$!
fi

# Cleanup function - kills all background processes
cleanup() {
    echo ""
    echo "Cleaning up background processes..."
    
    # Kill the tail process and its children
    if [ -n "$TAIL_PID" ]; then
        # Kill the entire process group to ensure all pipeline processes are terminated
        kill -TERM -$TAIL_PID 2>/dev/null || kill $TAIL_PID 2>/dev/null
        
        # Wait a moment for graceful termination
        sleep 0.2
        
        # Force kill if still running
        kill -KILL -$TAIL_PID 2>/dev/null || kill -KILL $TAIL_PID 2>/dev/null
    fi
    
    # Kill any remaining background jobs from this shell
    jobs -p | xargs -r kill -TERM 2>/dev/null
    sleep 0.1
    jobs -p | xargs -r kill -KILL 2>/dev/null
    
    echo "Cleanup complete."
    exit 0
}

# Set up signal traps for cleanup
trap cleanup INT TERM EXIT QUIT

# Interactive CLI loop
while true; do
    echo ""
    read -p "mp2-node> " line

    # Enable extglob for trimming if available
    shopt -s extglob 2>/dev/null || true
    line="${line##+([[:space:]])}"; line="${line%%+([[:space:]])}" 2>/dev/null || true
    shopt -u extglob 2>/dev/null || true

    tokens=($line)
    cmd="${tokens[0]}"
    unset 'tokens[0]'

    case "$cmd" in
        ""|"help")
            echo "Available commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, quit"
            echo "Examples:"
            echo "  switch -mode gossip -interval 500ms"
            echo "  switch -mode ping -suspicion -interval 200ms"
            echo "  join 172.22.156.123:8080"
            ;;
        "quit"|"exit"|"q")
            echo "Goodbye!"
            cleanup
            ;;
        *)
            # Forward the command to the actual application
            if [ -n "$line" ]; then
               ./mp2-node -cmd "$cmd" "${tokens[@]}" || echo "Error: Could not connect to node on port $PORT"
            fi
            ;;
    esac
done
