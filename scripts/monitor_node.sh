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
echo "----------------------------------------"

# Start background process to show suspects from log
if [ -f "node.log" ]; then
    # Show SUSPECT, FAILED, CLEARED events and member joins/leaves
    tail -F node.log 2>/dev/null | grep --line-buffered "SUSPECT\|FAILED\|CLEARED\|joined\|left\|New member" &
    TAIL_PID=$!
fi

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
            echo "  join seed-host:8080"
            ;;
        "quit"|"exit"|"q")
            echo "Exiting monitor..."
            [ -n "$TAIL_PID" ] && kill $TAIL_PID 2>/dev/null
            break
            ;;
        "list_mem"|"list_self"|"display_suspects"|"display_protocol"|"join"|"leave"|"switch")
            ./mp2-node -cmd "$cmd" "${tokens[@]}"
            ;;
        *)
            echo "Unknown command: $cmd (try 'help')"
            ;;
    esac
done
