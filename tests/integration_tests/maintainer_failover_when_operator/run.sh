#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_ADDRS=("127.0.0.1:8300" "127.0.0.1:8301" "127.0.0.1:8302")
FAILPOINT_NOT_READY_TO_CLOSE_DISPATCHER="github.com/pingcap/ticdc/downstreamadapter/dispatcher/NotReadyToCloseDispatcher"
FAILPOINT_BLOCK_CREATE_DISPATCHER="github.com/pingcap/ticdc/downstreamadapter/dispatchermanager/BlockCreateDispatcher"

function get_capture_id_by_addr() {
	local api_addr=$1
	local target_addr=$2
	curl -s "http://${api_addr}/api/v2/captures" | jq -r --arg addr "$target_addr" '.items[] | select(.address==$addr) | .id' | head -n1
}

function get_table_node_id() {
	local api_addr=$1
	local changefeed_id=$2
	local table_id=$3
	curl -s "http://${api_addr}/api/v2/changefeeds/${changefeed_id}/tables?keyspace=$KEYSPACE_NAME" |
		jq -r --argjson tid "$table_id" '.items[] | select(.table_ids | index($tid)) | .node_id' | head -n1
}

function get_maintainer_addr() {
	local api_addr=$1
	local changefeed_id=$2
	curl -s "http://${api_addr}/api/v2/changefeeds/${changefeed_id}?keyspace=$KEYSPACE_NAME" | jq -r '.maintainer_addr'
}

function wait_for_table_on_addr() {
	local api_addr=$1
	local changefeed_id=$2
	local table_id=$3
	local target_addr=$4
	for ((i = 0; i < 30; i++)); do
		local target_id
		target_id=$(get_capture_id_by_addr "$api_addr" "$target_addr")
		if [ -z "$target_id" ] || [ "$target_id" == "null" ]; then
			sleep 2
			continue
		fi
		local node_id
		node_id=$(get_table_node_id "$api_addr" "$changefeed_id" "$table_id")
		if [ "$node_id" == "$target_id" ]; then
			return 0
		fi
		sleep 2
	done
	echo "table $table_id not moved to $target_addr" >&2
	return 1
}

function wait_for_maintainer_move() {
	local api_addr=$1
	local changefeed_id=$2
	local old_addr=$3
	for ((i = 0; i < 30; i++)); do
		local new_addr
		new_addr=$(get_maintainer_addr "$api_addr" "$changefeed_id")
		if [ -n "$new_addr" ] && [ "$new_addr" != "null" ] && [ "$new_addr" != "$old_addr" ]; then
			echo "$new_addr"
			return 0
		fi
		sleep 2
	done
	echo "maintainer did not move from $old_addr" >&2
	return 1
}

function pick_addr_excluding() {
	local exclude1=$1
	local exclude2=$2
	for addr in "${CDC_ADDRS[@]}"; do
		if [ "$addr" != "$exclude1" ] && [ "$addr" != "$exclude2" ]; then
			echo "$addr"
			return 0
		fi
	done
	return 1
}

function ensure_table_on_addr() {
	local api_addr=$1
	local changefeed_id=$2
	local table_id=$3
	local target_addr=$4

	local target_id
	target_id=$(get_capture_id_by_addr "$api_addr" "$target_addr")
	if [ -z "$target_id" ] || [ "$target_id" == "null" ]; then
		echo "failed to get capture id for $target_addr" >&2
		return 1
	fi

	local current_node_id
	current_node_id=$(get_table_node_id "$api_addr" "$changefeed_id" "$table_id")
	if [ "$current_node_id" != "$target_id" ]; then
		move_table_with_retry "$target_addr" $table_id "$changefeed_id" 10
		wait_for_table_on_addr "$api_addr" "$changefeed_id" "$table_id" "$target_addr"
	fi
}

function enable_failpoint_on_all_addrs() {
	local failpoint_name=$1
	local expr=$2
	for addr in "${CDC_ADDRS[@]}"; do
		enable_failpoint --addr "$addr" --name "$failpoint_name" --expr "$expr"
	done
}

function disable_failpoint_on_all_addrs_best_effort() {
	local failpoint_name=$1
	set +e
	for addr in "${CDC_ADDRS[@]}"; do
		disable_failpoint --addr "$addr" --name "$failpoint_name"
	done
	set -e
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"

	# Disable balance scheduler to avoid unexpected auto split/move interfering with this test.
	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix 1 --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix 2 --addr "127.0.0.1:8301"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix 3 --addr "127.0.0.1:8302"
	export GO_FAILPOINTS=''

	TOPIC_NAME="ticdc-move-table-maintainer-failover-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	changefeed_id=$(cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" | grep '^ID:' | head -n1 | awk '{print $2}')
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql "CREATE DATABASE maintainer_failover_when_operator;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE maintainer_failover_when_operator.t1(id INT PRIMARY KEY, val VARCHAR(20));" ${UP_TIDB_HOST} ${UP_TIDB_PORT} # move
	run_sql "CREATE TABLE maintainer_failover_when_operator.t2(id INT PRIMARY KEY, val VARCHAR(20));" ${UP_TIDB_HOST} ${UP_TIDB_PORT} # split
	run_sql "CREATE TABLE maintainer_failover_when_operator.t3(id INT PRIMARY KEY, val VARCHAR(20));" ${UP_TIDB_HOST} ${UP_TIDB_PORT} # remove
	run_sql "split table maintainer_failover_when_operator.t2 between (1) and (100000) regions 20;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t1 VALUES (1, 'a'), (2, 'b');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t2 VALUES (1, 'a'), (2, 'b');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t3 VALUES (1, 'a'), (2, 'b');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists "maintainer_failover_when_operator.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists "maintainer_failover_when_operator.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists "maintainer_failover_when_operator.t3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	table_id_1=$(get_table_id "maintainer_failover_when_operator" "t1")
	table_id_2=$(get_table_id "maintainer_failover_when_operator" "t2")
	table_id_3=$(get_table_id "maintainer_failover_when_operator" "t3")

	api_addr=${CDC_ADDRS[0]}
	maintainer_addr=$(get_maintainer_addr "$api_addr" "$changefeed_id")
	if [ -z "$maintainer_addr" ] || [ "$maintainer_addr" == "null" ]; then
		echo "failed to get maintainer address" >&2
		exit 1
	fi

	origin_addr=$(pick_addr_excluding "$maintainer_addr" "")
	target_addr=$(pick_addr_excluding "$maintainer_addr" "$origin_addr")
	if [ -z "$origin_addr" ] || [ -z "$target_addr" ]; then
		echo "failed to select origin or target addr" >&2
		exit 1
	fi

	api_addr=$origin_addr
	ensure_table_on_addr "$api_addr" "$changefeed_id" "$table_id_1" "$origin_addr"
	ensure_table_on_addr "$api_addr" "$changefeed_id" "$table_id_2" "$origin_addr"
	ensure_table_on_addr "$api_addr" "$changefeed_id" "$table_id_3" "$origin_addr"

	enable_failpoint --addr "$origin_addr" --name "$FAILPOINT_NOT_READY_TO_CLOSE_DISPATCHER" --expr "return(true)"
	enable_failpoint_on_all_addrs "$FAILPOINT_BLOCK_CREATE_DISPATCHER" "pause"

	# Add operator: create new tables and insert some data while create-dispatcher is blocked.
	run_sql "CREATE TABLE maintainer_failover_when_operator.t4(id INT PRIMARY KEY, val VARCHAR(20));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE maintainer_failover_when_operator.t5(id INT PRIMARY KEY, val VARCHAR(20));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t4 VALUES (1, 'a'), (2, 'b');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t5 VALUES (1, 'a'), (2, 'b');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "maintainer_failover_when_operator.t4" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists "maintainer_failover_when_operator.t5" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	for table in t4 t5; do
		downstream_cnt=$(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -e "SELECT COUNT(*) FROM maintainer_failover_when_operator.${table};" | tail -n1)
		if [ "$downstream_cnt" != "0" ]; then
			echo "unexpected downstream row count for ${table} while create-dispatcher blocked: $downstream_cnt" >&2
			exit 1
		fi
	done

	# Split operator: issue split and keep it in-progress due to NotReadyToCloseDispatcher.
	set +e
	split_table_with_retry $table_id_2 "$changefeed_id" 1 0 &
	split_pid=$!
	set -e

	move_table_with_retry "$target_addr" $table_id_1 "$changefeed_id" 10 0 false
	# failpoint is enabled on origin, so the table should not move to target
	wait_for_table_on_addr "$api_addr" "$changefeed_id" "$table_id_1" "$origin_addr"

	# Remove operator: drop one table while dispatcher close is blocked.
	run_sql "DROP TABLE maintainer_failover_when_operator.t3;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Give operators some time to be sent and recorded by nodes.
	sleep 2

	maintainer_host=${maintainer_addr%:*}
	maintainer_port=${maintainer_addr#*:}
	maintainer_pid=$(get_cdc_pid "$maintainer_host" "$maintainer_port")
	kill_cdc_pid "$maintainer_pid"
	new_maintainer_addr=$(wait_for_maintainer_move "$api_addr" "$changefeed_id" "$maintainer_addr")
	echo "maintainer moved to $new_maintainer_addr"

	disable_failpoint --addr "$origin_addr" --name "$FAILPOINT_NOT_READY_TO_CLOSE_DISPATCHER"
	disable_failpoint_on_all_addrs_best_effort "$FAILPOINT_BLOCK_CREATE_DISPATCHER"

	set +e
	wait "$split_pid"
	set -e

	check_table_not_exists "maintainer_failover_when_operator.t3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300

	run_sql "ALTER TABLE maintainer_failover_when_operator.t1 ADD COLUMN c2 INT DEFAULT 0;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE maintainer_failover_when_operator.t1 SET c2 = 1 WHERE id = 1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t1 VALUES (3, 'c', 2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t2 VALUES (3, 'c');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t4 VALUES (3, 'c');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO maintainer_failover_when_operator.t5 VALUES (3, 'c');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
