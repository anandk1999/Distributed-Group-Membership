#!/bin/bash
# Live monitoring script - shows SUSPECT events and allows CLI commands
# Usage: ./scripts/monitor_node.sh [host] [port]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"
HOST=${1:-$(hostname)}
PORT=${2:-8080}

if [ "$HOST" = "$(hostname)" ] || [ "$HOST" = "localhost" ] || [ "$HOST" = "127.0.0.1" ]; then
    # Local monitoring
    echo "Monitoring local node on port $PORT"
    echo "Commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, quit"
    echo "(You can append extra arguments; the command is detected by keyword substring.)"
    echo "SUSPECT events will appear automatically below:"
    echo "----------------------------------------"
    
    # Start background process to show suspects from log
    if [ -f "node.log" ]; then
        tail -F node.log 2>/dev/null | grep --line-buffered "SUSPECT\|OnSuspect" &
        TAIL_PID=$!
    fi
    
    # Interactive CLI loop
    while true; do
        echo ""
        read -p "mp2-node> " cmd

        # Normalize detection by substring so users can type e.g. "leave now"
        if [[ -z "$cmd" || "$cmd" == "help"* ]]; then
            echo "Available commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, quit"
            continue
        fi

        if [[ "$cmd" == *"quit"* || "$cmd" == *"exit"* || "$cmd" == "q" ]]; then
            echo "Exiting monitor..."
            [ -n "$TAIL_PID" ] && kill $TAIL_PID 2>/dev/null
            break
        fi

        # Map substring to base command
        baseCmd=""
        for candidate in list_mem list_self display_suspects display_protocol join leave switch; do
            if [[ "$cmd" == *"$candidate"* ]]; then
                baseCmd="$candidate"
                break
            fi
        done

        if [[ -n "$baseCmd" ]]; then
            ./mp2-node -cmd "$baseCmd"
        else
            echo "Unknown command: $cmd (try 'help')"
        fi
    done
else
    # Remote monitoring
    echo "Monitoring remote node $HOST:$PORT"
    ssh -t "$REMOTE_USER@$HOST" "
        cd mp2-g02
    echo 'Monitoring node on $HOST'
    echo 'Commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, quit'
    echo '(Extra arguments allowed; command detected by keyword substring.)'
        echo 'SUSPECT events will appear automatically below:'
        echo '----------------------------------------'
        
        # Start background process to show suspects
        if [ -f node.log ]; then
            tail -F node.log 2>/dev/null | grep --line-buffered 'SUSPECT\|OnSuspect' &
            TAIL_PID=\$!
        fi
        
        # Interactive CLI loop
        while true; do
            echo ''
            read -p 'mp2-node> ' cmd
            if [[ -z "\$cmd" || "\$cmd" == help* ]]; then
                echo 'Available commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, quit'
                continue
            fi
            if [[ "\$cmd" == *quit* || "\$cmd" == *exit* || "\$cmd" == q ]]; then
                echo 'Exiting monitor...'
                [ -n "\$TAIL_PID" ] && kill \$TAIL_PID 2>/dev/null
                break
            fi
            baseCmd=""
            for candidate in list_mem list_self display_suspects display_protocol join leave switch; do
                if [[ "\$cmd" == *"\$candidate"* ]]; then
                    baseCmd="\$candidate"
                    break
                fi
            done
            if [[ -n "\$baseCmd" ]]; then
                ./mp2-node -cmd "\$baseCmd"
            else
                echo "Unknown command: \$cmd (try help)"
            fi
        done
    "
fi
