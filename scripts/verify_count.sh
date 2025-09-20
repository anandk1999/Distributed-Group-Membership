#!/bin/bash

LOG_DIR="/Users/ASUS1/Desktop/Fall 2025 Semester (UIUC)/Distributed Systems/Machine Problems/MP1/MP1 Demo Data FA22"
CLIENT_OUTPUT_FILE="../client_output.txt"
PATTERN='[0-9]{2}/Aug/[0-9]{4}'
PATTERN='RARETOKEN'
# PATTERN='13:[0-9]{2}:[0-9]{2}'
# PATTERN='AppleWebKit/5320'
# PATTERN='138.171.13.249'
# PATTERN='GET'
# PATTERN='Intel'
# PATTERN='http'
# PATTERN='14.0.898.0'
# PATTERN='http://hensley.org/'

LOG_TYPE="$1"
if [[ "$LOG_TYPE" != "demo" && "$LOG_TYPE" != "unit" ]]; then
    echo "Invalid log type: $LOG_TYPE. Must be 'demo' or 'unit'."
    exit 2
fi

case $LOG_TYPE in
  demo)
    FILE_PATTERN=( "$LOG_DIR"/vm[1-9].log "$LOG_DIR"/vm10.log )
    ;;
  unit)
    FILE_PATTERN=( "$LOG_DIR"/machine.[1-9].log "$LOG_DIR"/machine.10.log )
    ;;
esac

echo "Running client.go and saving output to $CLIENT_OUTPUT_FILE..."
echo `pwd`
echo "grep -c -E $PATTERN \n $LOG_TYPE" | go run ../client/client.go > "$CLIENT_OUTPUT_FILE"

if [[ $? -ne 0 ]]; then
    echo "❌ Failed to run client.go. Exiting."
    exit 1
fi

echo "✅ client.go executed successfully."
echo

echo "Comparing client output to local grep counts..."
echo "Using pattern: $PATTERN"
echo

local_filenames=()
local_counts=()

for log_file in "${FILE_PATTERN[@]}"; do
    echo "$log_file"
    if [[ -f "$log_file" ]]; then
        filename=$(basename "$log_file")
        count=$(grep -c -E "$PATTERN" "$log_file")
        local_filenames+=("$filename")
        local_counts+=("$count")
    fi
done

echo "Report:"
echo "-------------------"
mismatch_count=0
total_checked=0

while IFS= read -r response_line && IFS= read -r data_line; do
    if [[ "$data_line" =~ \./([^.]+\.log):([0-9]+) ]]; then
        machine="${BASH_REMATCH[1]}"
        remote_count="${BASH_REMATCH[2]}"

        local_count=""
        for i in "${!local_filenames[@]}"; do
            if [[ "${local_filenames[$i]}" == "$machine" ]]; then
                local_count="${local_counts[$i]}"
                break
            fi
        done

        if [[ -n "$local_count" ]]; then
            ((total_checked++))
            if [[ "$remote_count" -ne "$local_count" ]]; then
                echo "❌ Mismatch in $machine → Remote: $remote_count, Local: $local_count"
                ((mismatch_count++))
            else
                echo "✅ Match in $machine → Count: $remote_count"
            fi
        fi
    fi
done < "$CLIENT_OUTPUT_FILE"

echo "Total mismatches: $mismatch_count"