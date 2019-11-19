#!/bin/bash -eExl
#
# Testing script for SparkUCX
#
# Copyright (C) Mellanox Technologies Ltd. 2019.  ALL RIGHTS RESERVED.
#
# Environment variables:
#  - NODELIST                 : Space separated list of nodes (default: localhost)
#  - UCX_LIB                  : Path to UCX installation (default: LD_LIBRARY_PATH)
#  - SPARK_UCX_JAR            : Path to SparkUCX jar (default in $PWD)
#  - PROCESSES_PER_INSTANCE   : Number of spark processes per instance (default: 2)
#  - EXECUTOR_NUMBER          : Unique number of jenkins executor, to run multiple tests on 1 machine
#  - JOB_ID                   : Unique job id for this test
#  - SCRATCH_DIRECTORY        : Separate directory for this test where configs will be added.
#                               For now it's required to be on NFS
#  - SPARK_LOCAL_DIRS         : Directory to use for map output files and RDDs that get stored on disk.
#                               This should be on a fast, local disk in your system.
#                               It can also be a comma-separated list of multiple directories on different disks.
#  - SPARK_WORKER_CORES       : Number of cores per spark process (default: 2)
#  - SPARK_WORKER_MEMORY      : Memory per spark process (default: 1g)
#  - SPARK_HOME               : Spark location on the file system (default: $PWD/spark). If not set, will be downloaded
#  - SPARK_VERSION            : Version of spark to test (default: 2.4.4). Used only in SPARK_HOME is not set
#  - RDMA_NET_IFACE           : Network interface to test

NODELIST=${NODELIST:="localhost"}

UCX_LIB=${UCX_LIB:=${LD_LIBRARY_PATH}}

SPARK_UCX_JAR=${SPARK_UCX_JAR:=$PWD/spark-ucx-1.0-for-spark-2.4-jar-with-dependencies.jar}

PROCESSES_PER_INSTANCE=${PROCESSES_PER_INSTANCE:=2}

if [ -n "$EXECUTOR_NUMBER" ] 
then
	AFFINITY="taskset -c $(( 2 * EXECUTOR_NUMBER ))","$(( 2 * EXECUTOR_NUMBER + 1))"
else
	AFFINITY=""
fi

EXECUTOR_NUMBER=${EXECUTOR_NUMBER:=$((RANDOM % `nproc`))}

JOB_ID=${SLURM_JOB_ID:="sparkucx-test-$(cat /proc/sys/kernel/random/uuid)"}

SCRATCH_DIRECTORY=${SCRATCH_DIRECTORY:=$PWD/${JOB_ID}}

SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS:="/scrap/sparkucxtest/sparkucx-${JOB_ID}"}

SPARK_WORKER_CORES=${SPARK_WORKER_CORES:=2}

SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:="1g"}

SPARK_HOME=${SPARK_HOME:=$PWD/spark}

SPARK_VERSION=${SPARK_VERSION:="2.4.4"}

UCX_BRANCH=${UCX_BRANCH:="v1.7.x"}

export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:=$(hostname -f)}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT:=$(( 2000 + 10 * ${EXECUTOR_NUMBER}))}
export SPARK_CONF_DIR=${SCRATCH_DIRECTORY}/conf

get_rdma_device_iface() {

	if [ ! -r /dev/infiniband/rdma_cm  ]
	then
		return
	fi

	if ! which ibdev2netdev >&/dev/null
	then
		return
	fi

	iface=`ibdev2netdev | grep Up | awk '{print $5}' | head -1`
	if [[ -n "$iface" ]]
	then
		ipaddr=$(ip addr show ${iface} | awk '/inet /{print $2}' | awk -F '/' '{print $1}')
	fi

	if [[ -z "$ipaddr" ]]
	then
		# if there is no inet (IPv4) address, escape
		return
	fi

	ibdev=`ibdev2netdev | grep ${iface} | awk '{print $1}'`
	node_guid=`cat /sys/class/infiniband/$ibdev/node_guid`
	if [[ ${node_guid} == "0000:0000:0000:0000" ]]
	then
		return
	fi

	SPARK_MASTER_HOST=$ipaddr
	echo ${iface}
}

RDMA_NET_IFACE=${RDMA_NET_IFACE:=`get_rdma_device_iface`}

download_spark() {
    curl -s -L -O https://www-eu.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
    mkdir -p ${SPARK_HOME}
    tar -xf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz -C ${SPARK_HOME} --strip-components=1
}

build_ucx() {
    git clone -b ${UCX_BRANCH} --depth=1 https://github.com/openucx/ucx.git && cd ucx
    ./autogen.sh
    mkdir build && cd build
    ../contrib/configure-release-mt --with-java --prefix=$PWD
    make -j `nproc`
    make install
    UCX_LIB=$PWD/lib/
}

setup_configuration() {
    mkdir -p ${SPARK_CONF_DIR}

    echo ${NODELIST} | tr -s ' ' '\n' >> "${SPARK_CONF_DIR}/slaves"

    cat <<-EOF > ${SPARK_CONF_DIR}/spark-defaults.conf
	spark.shuffle.manager org.apache.spark.shuffle.UcxShuffleManager
	spark.driver.extraClassPath ${SPARK_UCX_JAR}:${UCX_LIB}
	spark.executor.extraClassPath ${SPARK_UCX_JAR}:${UCX_LIB}
	spark.shuffle.ucx.driver.port $(( ${SPARK_MASTER_PORT} + 1 ))
	EOF

    cat <<-EOF > ${SPARK_CONF_DIR}/spark-env.sh
	export SPARK_LOCAL_IP=\`/sbin/ip addr show ${RDMA_NET_IFACE} | grep "inet\b" | awk '{print \$2}' | cut -d/ -f1\`
	export SPARK_WORKER_DIR=${SCRATCH_DIRECTORY}/work
	export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS}
	export SPARK_LOG_DIR=${SCRATCH_DIRECTORY}/logs
	export SPARK_CONF_DIR=${SPARK_CONF_DIR}
	export SPARK_MASTER_HOST=${SPARK_MASTER_HOST}
	export SPARK_MASTER_PORT=${SPARK_MASTER_PORT}
	export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
	export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
	export SPARK_IDENT_STRING=${JOB_ID}
	EOF

    cp ${SPARK_HOME}/conf/log4j.properties.template ${SPARK_CONF_DIR}/log4j.properties
}

start_cluster() {
    ${AFFINITY} ${SPARK_HOME}/sbin/start-master.sh

    # Make a script wrapper to propagate SPARK_CONF_DIR
    cat <<-EOF > ${SCRATCH_DIRECTORY}/sparkworker.sh
	#! /bin/bash
	export SPARK_CONF_DIR=${SPARK_CONF_DIR}
	export SPARK_WORKER_INSTANCES=${PROCESSES_PER_INSTANCE}
	export SPARK_IDENT_STRING=${JOB_ID}
	${AFFINITY} ${SPARK_HOME}/sbin/start-slave.sh "spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
	EOF

    SPARK_CONF_DIR=${SPARK_CONF_DIR} ${SPARK_HOME}/sbin/slaves.sh bash ${SCRATCH_DIRECTORY}/sparkworker.sh
}

run_groupby_test() {
    ${SPARK_HOME}/bin/run-example --verbose --master spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} \
        --jars "${SPARK_HOME}/examples/jars/*.jar" --executor-memory ${SPARK_WORKER_MEMORY} \
        org.apache.spark.examples.GroupByTest 100 100
}

run_tc_test() {
    ${SPARK_HOME}/bin/run-example --verbose --master spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} \
        --jars "${SPARK_HOME}/examples/jars/*.jar" --executor-memory ${SPARK_WORKER_MEMORY} \
        org.apache.spark.examples.SparkTC
}

run_tests() {
	if [[ ! -d ${SPARK_HOME} ]]
	then
		download_spark
	fi

	if [[ ! -d ${UCX_LIB} ]]
	then
		build_ucx
	fi

	trap stop_cluster EXIT;

	setup_configuration
	start_cluster
	run_groupby_test && run_tc_test
}

stop_cluster() {
     cat <<-EOF > ${SCRATCH_DIRECTORY}/stop-sparkworker.sh
        #! /bin/bash
	export SPARK_CONF_DIR=${SPARK_CONF_DIR}
	export SPARK_WORKER_INSTANCES=${PROCESSES_PER_INSTANCE}
	export SPARK_IDENT_STRING=${JOB_ID}
	${SPARK_HOME}/sbin/stop-slave.sh
	EOF
    
    chmod +x ${SCRATCH_DIRECTORY}/stop-sparkworker.sh
    # Stop all slaves
    ${SPARK_HOME}/sbin/slaves.sh ${SCRATCH_DIRECTORY}/stop-sparkworker.sh

    ${SPARK_HOME}/sbin/stop-master.sh
}
