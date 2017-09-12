#!/usr/bin/env bash

# NB! verify/change all variables before running the script
PG_MASTER_BINDIR=/usr/lib/postgresql/9.5/bin        # Ubuntu/Debian directories as samples here
PG_REPLICA_BINDIR=/usr/lib/postgresql/9.5/bin
PG_REPLICA_DATADIR=/var/lib/postgresql/9.5/main
TEMP_WAL_FOLDER=/tmp/parallel_basebackup_wal        # storage are of WAL/XLOG files gathered during the data copy
PG_HOST=localhost                       # remote master IP
PG_HOST_SOCKET=/var/run/postgresql      # master socket. can also be IP but communication over the unix socket is faster
PG_PORT=5432                            # master port
PG_USER=postgres    # user for master local basebackup. needs "replication" role. no password replication connections in pg_hba.conf or an .pgpass entry is assumed
REPLICA_USER=postgres   # user for running the streaming replica. no password replication connections in pg_hba.conf or an .pgpass entry is assumed
SSH_HOST=$PG_HOST       # master IP for SSH
SSH_USER=$USER          # assuming SSH keys have been already set up
SSH_PORT=22
START_REPLICA_AFTER_COPY=1
PIGZ_PROCESSES=2        # 2-4 threads should be sufficient, no linear gains after that

# NB! below REPLICA_*_CONF files should be created or copied over from master and modified (if needed) before running the script
EXTERNAL_CONFIG_FILE_PATHS=1        # if 0 then DATADIR is self-containing and below 2 configs will be copied into the DATADIR.
                                 # if 1, the postgres server will be started with the below REPLICA_POSTGRESQL_CONF path
REPLICA_POSTGRESQL_CONF=/etc/postgresql/9.5/main/postgresql.conf
REPLICA_HBA_CONF=/etc/postgresql/9.5/main/pg_hba.conf       # relevant only with EXTERNAL_CONFIG_FILE_PATHS=0
# NB! .pgpass on replica should include an entry for $REPLICA_USER
PRIMARY_CONNINFO="primary_conninfo = 'host=$PG_HOST user=$REPLICA_USER port=$PG_PORT sslmode=prefer sslcompression=1'"


function check_temp_wal_folder_empty() {
    if [ ! -d "$TEMP_WAL_FOLDER" ] ; then
        mkdir $TEMP_WAL_FOLDER
        if [ "$?" -ne 0 ] ; then
            echo "Failed to create temp WAL folder at: ${TEMP_WAL_FOLDER}"
            exit 1
        fi
    fi
    RET=$(ls ${TEMP_WAL_FOLDER})
    if [ -n "$RET" ] ; then
        echo "TEMP_WAL_FOLDER ($TEMP_WAL_FOLDER) is not empty!"
        exit 1
    fi
}

function check_datadir_folder_empty_or_create() {
    if [ ! -d "$PG_REPLICA_DATADIR" ] ; then
        echo "Creating the PG_REPLICA_DATADIR ($PG_REPLICA_DATADIR) ..."
        mkdir $PG_REPLICA_DATADIR
        if [ "$?" -ne 0 ] ; then
           echo "Failed to create PG_REPLICA_DATADIR at: ${PG_REPLICA_DATADIR}"
           exit 1
        fi
    else
        RET=$(ls ${PG_REPLICA_DATADIR})
        if [ -n "$RET" ] ; then
            echo "PG_REPLICA_DATADIR ($PG_REPLICA_DATADIR) is not empty!"
            exit 1
        fi
    fi
}

function check_config_file_exists() {
    if [ ! -f "$REPLICA_POSTGRESQL_CONF" ] ; then
      echo "Replica postgresql.conf file not found!"
      exit 1
    fi
}

function check_ssh() {
    RET=$(ssh -p $SSH_PORT $SSH_USER@$SSH_HOST date)
    if [ "$?" -ne 0 ] ; then
      echo "SSH connection failed"
      exit 1
    fi
}

function exec_sql() {
   RET=$(${PG_REPLICA_BINDIR}/psql -h ${PG_HOST} -p {PG_PORT} -U ${PG_USER} -qXAtc "$1" postgres)
    if [ "$?" -ne 0 ] ; then
      echo "Execution of SQL failed: $1"
      exit 1
    fi
   echo $RET
}

function check_tablespaces() {
   RET=$(exec_sql "select count(*) from pg_tablespace where spcname not in ('pg_default', 'pg_global')")
   if [ "$RET" -gt 0 ] ; then
     echo "Additional tablespaces found! Script not applicable"
     exit 1
   fi
}

function check_config_settings() {
   RET=$(exec_sql "select count(*) from pg_settings where (name = 'wal_level' and setting not in ('hot_standby', 'replica', 'logical')) or (name = 'max_wal_senders' and setting = '0')")
   if [ "$RET" -gt 0 ] ; then
     echo "Invalid server settings on master for creating replicas - check wal_level/max_wal_senders"
     exit 1
   fi
}

function check_pigz() {
    PIGZ=$(which pigz)
    if [ -z "$PIGZ" ] ; then
      echo "pigz utility not found!"
      exit 1
    fi
}

# TODO PG 10 xlog -> wal
function start_pg_receivexlog() {
    echo "Starting pg_receivexlog in background ..."
    ${PG_REPLICA_BINDIR}/pg_receivexlog -D ${TEMP_WAL_FOLDER} -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USER} --no-password >/dev/null &
    PROC_ID=$!
}

function stop_pg_receivewal() {
    echo "Stopping pg_receivexlog background process (PID = $PROC_ID) ..."
    kill $PROC_ID
    if [ "$?" -ne 0 ] ; then
      echo "Failed to kill pg_receivexlog background process: retcode = $?"
    fi
}

function move_received_wal_from_tmp_to_datadir() {
    rm ${TEMP_WAL_FOLDER}/*.partial
    mv ${TEMP_WAL_FOLDER}/* ${PG_REPLICA_DATADIR}/pg_xlog
    if [ "$?" -ne 0 ] ; then
      echo "Failed to move received WALs from ${TEMP_WAL_FOLDER} to WAL dir ${PG_REPLICA_DATADIR}/pg_xlog"
      exit 1
    fi
}

function correct_recovery_conf() {
    echo "Correcting recovery conf ..."
    cat ${PG_REPLICA_DATADIR}/recovery.conf | grep -v primary_conninfo > ${PG_REPLICA_DATADIR}/recovery.conf.tmp
    echo "$PRIMARY_CONNINFO" >> ${PG_REPLICA_DATADIR}/recovery.conf.tmp
    mv ${PG_REPLICA_DATADIR}/recovery.conf.tmp ${PG_REPLICA_DATADIR}/recovery.conf
}

function delete_old_logs() {
    echo "Deleting old server logs from master ..."
    rm ${PG_REPLICA_DATADIR}/pg_log/*
}

function do_parallel_basebackup_over_ssh() {
    echo "Streaming pg_basebackup tar output from master over SSH [compression threads = $PIGZ_PROCESSES] ..."
    START_TIME=$(date +%s)
    RET=$(ssh -q -p ${SSH_PORT} ${SSH_USER}@${SSH_HOST} "${PG_MASTER_BINDIR}/pg_basebackup -h ${PG_HOST_SOCKET} -p ${PG_PORT} -U ${PG_USER} \
    --checkpoint=fast -Ft -D- -R | pigz -c -p${PIGZ_PROCESSES}" | pigz -dc -p${PIGZ_PROCESSES} | tar -xf- -C ${PG_REPLICA_DATADIR})
    if [ "$?" -ne 0 ] ; then
      echo "SSH cmd returned exitcode: $?"
      exit 1
    fi
    END_TIME=$(date +%s)
    echo "Done in $(($END_TIME - $START_TIME)) seconds"
}

function start_server() {
    echo "Starting the server"
    if [ "$EXTERNAL_CONFIG_FILE_PATHS" -eq 1 ] ; then
      ${PG_REPLICA_BINDIR}/pg_ctl -D ${PG_REPLICA_DATADIR} -o "-c config_file=${REPLICA_POSTGRESQL_CONF}" start
    else
      ${PG_REPLICA_BINDIR}/pg_ctl -D ${PG_REPLICA_DATADIR} start
    fi
    sleep 1
}

function check_server_up() {
    echo "Checking status of replica ..."
    ${PG_REPLICA_BINDIR}/pg_ctl -D ${PG_REPLICA_DATADIR} status
    if [ "$?" -ne 0 ] ; then
      echo "Server not running :( See logs for a cause"
    fi
}

check_temp_wal_folder_empty

check_datadir_folder_empty_or_create

check_config_file_exists

check_ssh

check_tablespaces

check_config_settings

check_pigz

start_pg_receivexlog

do_parallel_basebackup_over_ssh

stop_pg_receivewal

move_received_wal_from_tmp_to_datadir

delete_old_logs

correct_recovery_conf

if [ "$EXTERNAL_CONFIG_FILE_PATHS" -eq 0 ] ; then
    cp $REPLICA_POSTGRESQL_CONF $REPLICA_HBA_CONF $PG_REPLICA_DATADIR
fi

if [ "$START_REPLICA_AFTER_COPY" -eq 1 ] ; then

    start_server

    check_server_up
fi

echo "Done"
