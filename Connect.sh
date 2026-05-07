#!/bin/bash
# Kafka Connect heartbeat - pure shell, no python, no jq
# Polls local Connect REST API and emits one JSON line per connector
# to a log file Splunk tails. Run from cron every minute.

CONNECT_URL="http://localhost:8083"
HEARTBEAT_LOG="/data/risknav/logs/kafka-connect-logs/connect-heartbeat.log"
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
HOSTNAME_SHORT=$(hostname -s)

# 1. Fetch connector list
CONNECTORS=$(curl -sf --max-time 5 "$CONNECT_URL/connectors" 2>/dev/null)

if [ -z "$CONNECTORS" ]; then
    echo "{\"timestamp\":\"$TIMESTAMP\",\"host\":\"$HOSTNAME_SHORT\",\"event\":\"heartbeat\",\"status\":\"REST_API_UNREACHABLE\"}" >> "$HEARTBEAT_LOG"
    exit 0
fi

# 2. Clean ["a","b","c"] into space-separated: a b c
CLEAN_CONNECTORS=$(echo "$CONNECTORS" | tr -d '[]"' | tr ',' ' ')

# 3. For each connector, fetch its status and extract states
for connector in $CLEAN_CONNECTORS; do
    STATUS=$(curl -sf --max-time 5 "$CONNECT_URL/connectors/$connector/status" 2>/dev/null)
    if [ -z "$STATUS" ]; then
        continue
    fi

    # Extract connector-level state from the "connector":{...} block
    CONNECTOR_STATE=$(echo "$STATUS" | grep -o '"connector":{[^}]*}' | grep -o '"state":"[^"]*"' | head -1 | sed 's/.*"state":"//;s/".*//')

    # Extract task ID:STATE pairs from the "tasks":[...] block
    # Each task has "id":N and "state":"X" - we need them paired
    TASKS_BLOCK=$(echo "$STATUS" | grep -o '"tasks":\[[^]]*\]')

    # Build "0:RUNNING,1:FAILED" by walking the tasks block
    TASK_STATES=""
    FAILED_COUNT=0

    # Split tasks block on },{  to get individual task entries
    # Then extract id and state from each
    TASK_ENTRIES=$(echo "$TASKS_BLOCK" | sed 's/},{/}\n{/g')

    while IFS= read -r task; do
        TASK_ID=$(echo "$task" | grep -o '"id":[0-9]*' | sed 's/"id"://')
        TASK_STATE=$(echo "$task" | grep -o '"state":"[^"]*"' | sed 's/.*"state":"//;s/".*//')

        if [ -n "$TASK_ID" ] && [ -n "$TASK_STATE" ]; then
            if [ -n "$TASK_STATES" ]; then
                TASK_STATES="${TASK_STATES},"
            fi
            TASK_STATES="${TASK_STATES}${TASK_ID}:${TASK_STATE}"

            if [ "$TASK_STATE" = "FAILED" ]; then
                FAILED_COUNT=$((FAILED_COUNT + 1))
            fi
        fi
    done <<< "$TASK_ENTRIES"

    # Defaults if extraction produced nothing
    [ -z "$CONNECTOR_STATE" ] && CONNECTOR_STATE="UNKNOWN"
    [ -z "$TASK_STATES" ] && TASK_STATES="NONE"

    echo "{\"timestamp\":\"$TIMESTAMP\",\"host\":\"$HOSTNAME_SHORT\",\"event\":\"heartbeat\",\"connector\":\"$connector\",\"connector_state\":\"$CONNECTOR_STATE\",\"task_states\":\"$TASK_STATES\",\"failed_tasks\":$FAILED_COUNT}" >> "$HEARTBEAT_LOG"
done
