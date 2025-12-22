#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

FAILPOINT_API="http://${CDC_HOST}:${CDC_PORT}/debug/failpoints"
FAILPOINT_API_TEST_NAME="github.com/pingcap/ticdc/utils/dynstream/FailpointAPITestValue"
FAILPOINT_API_TEST_VALUE="ds_memory_control"

function check_failpoint_enabled() {
	local name=$1
	if ! curl -s "$FAILPOINT_API" | grep -q "$name"; then
		echo "failpoint not found in list: $name"
		curl -s "$FAILPOINT_API"
		return 1
	fi
}

function check_failpoint_log_value() {
	local value=$1
	local log_file="$WORK_DIR/cdc.log"

	for ((i = 0; i < 60; i++)); do
		if [ -f "$log_file" ] && grep -q "failpoint api test value" "$log_file" && grep -q "$value" "$log_file"; then
			return 0
		fi
		sleep 1
	done

	echo "failpoint log value not found: $value"
	if [ -f "$log_file" ]; then
		tail -n 200 "$log_file"
	fi
	return 1
}

function run() {
	# Validate sink type is mysql since this test is mysql specific
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "skip sink_hang test for $SINK_TYPE"
		return 0
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cat <<EOF >"$WORK_DIR/server.toml"
[debug]
enable-failpoint-api = true
EOF

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE DATABASE ds_memory_control;"
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=ds_memory_control
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --config "$WORK_DIR/server.toml"

	enable_failpoint --name "github.com/pingcap/ticdc/utils/dynstream/PausePath" --expr "10%return(true)"
	check_failpoint_enabled "github.com/pingcap/ticdc/utils/dynstream/PausePath"
	enable_failpoint --name "$FAILPOINT_API_TEST_NAME" --expr "return(\"$FAILPOINT_API_TEST_VALUE\")"
	check_failpoint_enabled "$FAILPOINT_API_TEST_NAME"

	SINK_URI="mysql://normal:123456@127.0.0.1:3306"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	check_failpoint_log_value "$FAILPOINT_API_TEST_VALUE"
	disable_failpoint --name "$FAILPOINT_API_TEST_NAME"

	sleep 60
	run_sql "CREATE TABLE ds_memory_control.finish_mark_1 (a int primary key);"
	check_table_exists "ds_memory_control.finish_mark_1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 180
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 180

	# toggle failpoints dynamically.
	disable_failpoint --name "github.com/pingcap/ticdc/utils/dynstream/PausePath"
	enable_failpoint --name "github.com/pingcap/ticdc/utils/dynstream/PauseArea" --expr "10%return(true)"
	check_failpoint_enabled "github.com/pingcap/ticdc/utils/dynstream/PauseArea"


	go-ycsb run mysql -P $CUR/conf/workload-2 -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=ds_memory_control

	sleep 60
	run_sql "CREATE TABLE ds_memory_control.finish_mark_2 (a int primary key);"
	check_table_exists "ds_memory_control.finish_mark_2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	disable_failpoint --name "github.com/pingcap/ticdc/utils/dynstream/PauseArea"
	cleanup_process $CDC_BINARY
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
