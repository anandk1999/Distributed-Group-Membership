#!/bin/bash
# Live monitoring script - shows SUSPECT events and allows CLI commands
# Usage: ./scripts/monitor_node.sh [host] [port]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"
HOST=${1:-$(hostname)}
PORT=${2:-8080}

STATE_FILE=$(mktemp -t mp2-monitor-state.XXXXXX)
EVENT_FILE=$(mktemp -t mp2-monitor-events.XXXXXX)

# Local monitoring
echo "Monitoring local node on port $PORT"
echo "Commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, show_status, show_events, grep_logs, quit"
echo "You can add arguments after a command, e.g.: switch -mode gossip -interval 500ms"
echo "Membership events (SUSPECT/FAILED/CLEARED/joins/leaves) will appear automatically below:"
echo "Note: Exact duplicate lines (including timestamp) are filtered out"
echo "----------------------------------------"

# Start background process to show suspects from log
if [ -f "node.log" ]; then
    # Show only the latest SUSPECT, FAILED, CLEARED events and member joins/leaves
    # Use bash subshell for better process control on macOS
    (
        tail -F node.log 2>/dev/null | \
            grep --line-buffered -E "(SUSPECT|FAILED|CLEARED|joined|left|New member|cleared suspicion)" | \
            awk -v state_file="$STATE_FILE" -v event_file="$EVENT_FILE" '
            function flush_state(   node) {
                close(state_file)
                print "" > state_file
                close(state_file)
                for (node in current_state) {
                    printf "%s %s\n", node, current_state[node] >> state_file
                }
                close(state_file)
            }

            function emit(ts, status, node, raw,   color_start, color_end, summary) {
                tail -F node.log 2>/dev/null | \
                    grep --line-buffered -E "(SUSPECT|FAILED|CLEARED|joined|left|New member|cleared suspicion)" | \
                    awk -v state_file="$STATE_FILE" -v event_file="$EVENT_FILE" '
        function flush_state(   node) {
            close(state_file)
            print "" > state_file
            close(state_file)
            for (node in current_state) {
                printf "%s %s\n", node, current_state[node] >> state_file
            }
            close(state_file)
        }

        function emit(ts, status, node, raw,   color_start, color_end, summary) {
            color_start=""
            color_end="\033[0m"
            if (status == "FAILED") color_start="\033[31m"
            else if (status == "SUSPECT") color_start="\033[33m"
            else if (status == "CLEARED") color_start="\033[32m"
            else if (status == "JOINED" || status == "NEW_MEMBER") color_start="\033[36m"
            else if (status == "LEFT") color_start="\033[35m"
            else color_end=""

            summary = ts " [" status "]"
            if (node != "") {
                summary = summary " " node
            }

            if (color_end != "") {
                printf "%s%s %s%s\n", color_start, summary, raw, color_end
            } else {
                printf "%s %s\n", summary, raw
            }

            printf "%s %s\n", summary, raw >> event_file
            close(event_file)
        }

        function extract_node(line, pattern,   pos, rest, parts, n) {
            pos = index(line, pattern)
            if (pos > 0) {
                rest = substr(line, pos + length(pattern))
                n = split(rest, parts, /[[:space:]]+/)
                if (n > 0) {
                    return parts[1]
                }
            }
            return ""
        }

        {
            ts = $1 " " $2
            line = $0
            status = ""
            node = ""

            if (index(line, "SUSPECT: ") > 0) {
                status = "SUSPECT"
                node = extract_node(line, "SUSPECT: ")
            } else if (index(line, "FAILED: ") > 0) {
                status = "FAILED"
                node = extract_node(line, "FAILED: ")
            } else if (index(line, "CLEARED: ") > 0) {
                status = "CLEARED"
                node = extract_node(line, "CLEARED: ")
            } else if (index(line, "New member joined: ") > 0) {
                status = "JOINED"
                node = extract_node(line, "New member joined: ")
            } else if (index(line, "left the group: ") > 0) {
                status = "LEFT"
                node = extract_node(line, "left the group: ")
            } else if (index(line, "Member ") > 0 && index(line, " cleared suspicion") > 0) {
                status = "CLEARED"
                node = extract_node(line, "Member ")
                if (node != "") {
                    sub(/ cleared suspicion.*/, "", node)
                }
            } else {
                next
            }

            key = (node != "" ? node : line)
            message = substr(line, length($1 " " $2 " ") + 1)

            if (current_state[key] != status || last_line[key] != line) {
                current_state[key] = status
                last_line[key] = line
                emit(ts, status, node, message)
                flush_state()
            }
        }'
        # Wait a moment for graceful termination
        sleep 0.2
        
        # Force kill if still running
        kill -KILL -$TAIL_PID 2>/dev/null || kill -KILL $TAIL_PID 2>/dev/null
    fi
    
    # Kill any remaining background jobs from this shell
    jobs -p | xargs -r kill -TERM 2>/dev/null
    sleep 0.1
    jobs -p | xargs -r kill -KILL 2>/dev/null
    
    rm -f "$STATE_FILE" "$EVENT_FILE"
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
            echo "Available commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, show_status, show_events, grep_logs, quit"
            echo "Examples:"
            echo "  switch -mode gossip -interval 500ms"
            echo "  switch -mode ping -suspicion -interval 200ms"
            echo "  join 172.22.156.123:8080"
            echo "  grep_logs (then follow the prompts for your grep command)"
            ;;
        "quit"|"exit"|"q")
            echo "Goodbye!"
            cleanup
            ;;
        "show_status")
            if [ -s "$STATE_FILE" ]; then
                echo "Current per-node event state:"
                if command -v column >/dev/null 2>&1; then
                    column -t "$STATE_FILE"
                else
                    cat "$STATE_FILE"
                fi
            else
                echo "No events recorded yet."
            fi
            ;;
        "show_events"|"events")
            if [ -s "$EVENT_FILE" ]; then
                echo "Recent event transitions:"
                tail -n 20 "$EVENT_FILE"
            else
                echo "No events recorded yet."
            fi
            ;;
        "grep_logs")
            read -p "Enter grep command (e.g., grep -E 'SUSPECT|FAILED'): " grep_cmd
            if [ -z "$grep_cmd" ]; then
                echo "No grep command provided."
                continue
            fi
            read -p "Log key [default: node]: " log_key
            log_key=${log_key:-node}
            ./mp2-node -cmd grep_logs -arg1 "$grep_cmd" -arg2 "$log_key"
            ;;
        *)
            # Forward the command to the actual application
            if [ -n "$line" ]; then
               ./mp2-node -cmd "$cmd" "${tokens[@]}" || echo "Error: Could not connect to node on port $PORT"
            fi
            ;;
    esac
done
