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

# Create a temporary file to store recent exact lines for deduplication
TEMP_FILE="/tmp/mp2_exact_lines_$$"
trap "rm -f $TEMP_FILE" EXIT
touch "$TEMP_FILE"

# Function to check for exact line duplicates in the last few seconds
check_exact_duplicate() {
    local line="$1"
    local current_time=$(date +%s)
    
    # Check if this exact line appeared recently (within 2 seconds)
    while IFS='|' read -r timestamp stored_line 2>/dev/null; do
        if [[ -n "$timestamp" && -n "$stored_line" ]]; then
            # Remove old entries (older than 2 seconds)
            if (( current_time - timestamp <= 2 )); then
                # Check for exact match
                if [[ "$line" == "$stored_line" ]]; then
                    return 0  # Found duplicate
                fi
            fi
        fi
    done < "$TEMP_FILE"
    
    # Update temp file - remove old entries and add current line
    while IFS='|' read -r timestamp stored_line 2>/dev/null; do
        if [[ -n "$timestamp" ]] && (( current_time - timestamp <= 2 )); then
            echo "$timestamp|$stored_line"
        fi
    done < "$TEMP_FILE" > "${TEMP_FILE}.tmp" 2>/dev/null
    
    echo "$current_time|$line" >> "${TEMP_FILE}.tmp"
    mv "${TEMP_FILE}.tmp" "$TEMP_FILE" 2>/dev/null
    
    return 1  # Not a duplicate
}
    
    # Update the temp file (remove old entries and add current)
    while IFS='|' read -r old_time old_key old_line 2>/dev/null; do
        if [[ -n "$old_time" ]] && (( current_time - old_time <= 3 )); then
            echo "$old_time|$old_key|$old_line"
        fi
    done < "$TEMP_FILE" > "${TEMP_FILE}.tmp" 2>/dev/null
# Start background process to show suspects from log
if [ -f "node.log" ]; then
    # Show SUSPECT, FAILED, CLEARED events and member joins/leaves with exact duplicate filtering
    tail -F node.log 2>/dev/null | \
        grep --line-buffered -E "(SUSPECT|FAILED|CLEARED|joined|left|New member)" | \
        while IFS= read -r line; do
            if ! check_exact_duplicate "$line"; then
                echo "$line"
            fi
        done &
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
            echo "  switch -mode ping -suspicion -interval 200ms"
            echo "  join 172.22.156.123:8080"
            ;;
        "quit"|"exit")
            echo "Goodbye!"
            break
            ;;
        *)
            # Forward the command to the actual application
            if [ -n "$line" ]; then
               ./mp2-node -cmd "$cmd" "${tokens[@]}" || echo "Error: Could not connect to node on port $PORT"
            fi
            ;;
    esac
done

# Cleanup function
cleanup() {
    if [ -n "$TAIL_PID" ]; then
        kill $TAIL_PID 2>/dev/null
    fi
    rm -f "$TEMP_FILE" "${TEMP_FILE}.tmp" 2>/dev/null
    exit 0
}

trap cleanup INT TERM EXIT
