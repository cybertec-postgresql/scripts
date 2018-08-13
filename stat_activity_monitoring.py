#!/usr/bin/env python

import logging

import argparse
import time

import psycopg2
import psycopg2.extras
import sys
import os

ERROR_LIMIT = 10    # exiting after so many errors during the pg_stat_activity gathering loop

args = None
logger = None

sql_create = """
create {unlogged} table {table_name} as
select
  now() as snap_time,
  pid,
  usename,
  application_name,
  client_addr,
  client_port,
  backend_start,
  xact_start,
  query_start,
  state_change,
  wait_event_type,
  wait_event,
  state,
  ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g'))::varchar(500) as query
from
  pg_stat_activity
where
  false
"""

sql_truncate = """truncate table {table_name}"""

sql_stat_activity = """
insert into {table_name}
select
  now() as snap_time,
  pid,
  usename,
  application_name,
  client_addr,
  client_port,
  backend_start,
  xact_start,
  query_start,
  state_change,
  wait_event_type,
  wait_event,
  state,
  case
    when state != 'idle' then
      ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g'))::varchar(250)
    else
      null
  end as query
from
  pg_stat_activity
where
  datname = current_database()
  and pid != pg_backend_pid()
  and backend_type = 'client backend'
"""


def getDBConnection(autocommit=True):
    """ if password is in use, .pgpass must be used """
    conn = psycopg2.connect(host=args.host, port=args.port, database=args.dbname, user=args.username)
    if autocommit:
        conn.autocommit = True
    return conn


def closeConnection(conn):
    if conn:
        try:
            conn.close()
        except:
            logging.exception('failed to close db connection')


def execute(conn, sql, params=None, statement_timeout=None, silent=True):
    result = []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if statement_timeout:
            cur.execute("SET statement_timeout TO '{}'".format(
                statement_timeout))
        cur.execute(sql, params)
        if (cur.statusmessage.startswith('SELECT') and cur.statusmessage != 'SELECT 0') or cur.description:
            result = cur.fetchall()
        else:
            result = [{'rows_affected': str(cur.rowcount)}]
    except Exception as e:
        if not silent:
            logging.exception('failed to execute "{}"'.format(sql))
        return result, str(e)
    return result, None


def exitOnErrormsg(errmsg, extra_description=None):
    if errmsg:
        if extra_description:
            logger.error("%s: %s", extra_description, errmsg)
        else:
            logger.error(errmsg)
        sys.exit(1)


def main():
    argp = argparse.ArgumentParser(description='Script to store snapshots of pg_stat_activity to the monitored DB. Meant '
                                               'to be run as superuser and on directly on the DB host (at least for very short intervals)')
    argp.add_argument('-q', '--quiet', help='Errors only', action='store_true')
    argp.add_argument('--host', help='DB hostname [default: localhost]', default='localhost')
    argp.add_argument('--port', help='DB port [default: 5432]', type=int, default='5432')
    argp.add_argument('--username', help='DB Username [default: $USER]', default=os.getenv('USER'))
    argp.add_argument('--dbname', help='DB name')
    argp.add_argument('--interval-ms', help='pg_stat_activity snapshot interval in ms', type=int, required=True)
    argp.add_argument('--duration-s', help='How long to gather pg_stat_activity snapshots', type=int, required=True)
    argp.add_argument('--snapshot-storage-table', help='Table name where pg_stat_activity snapshot data wille be stored', required=True)
    argp.add_argument('--truncate', help='Do a truncate on the snapshot data table if tables is existing', action='store_true')
    argp.add_argument('--unlogged', help='Create the snapshot storage tables as an unlogged table, producing no extra WAL', action='store_true', default=False)

    global args
    args, unknown_args = argp.parse_known_args()

    logging.basicConfig(format='%(levelname)s %(message)s')
    global logger
    logger = logging.getLogger()
    logger.setLevel((logging.ERROR if args.quiet else logging.INFO))

    logger.info('checking connection / superuser rights to DB...')
    conn = getDBConnection()
    rolsuper, errmsg = execute(conn, "select rolsuper from pg_roles where rolname = session_user")
    exitOnErrormsg(errmsg)
    if rolsuper and rolsuper[0]['rolsuper']:
        logger.info('connection / user rights OK')
    else:
        exitOnErrormsg('--username should be superuser!')

    logger.info('creating the snapshot storage table "%s"...', args.snapshot_storage_table)
    _, errmsg = execute(conn, sql_create.format(unlogged=('unlogged' if args.unlogged else ''), table_name=args.snapshot_storage_table))
    if errmsg and errmsg.find('already exists') > 0 and args.truncate:
        logger.info('table already exist - truncating...')
        _, errmsg = execute(conn, sql_truncate.format(table_name=args.snapshot_storage_table))
        exitOnErrormsg(errmsg)
    else:
        logger.error('table "%s" already exists! use the "--truncate" flag to clean it automatically', args.snapshot_storage_table)

    err_count = 0
    start_time = time.time()

    while True:

        if time.time() - start_time >= args.duration_s:
            logger.info('reached --duration. stopping')
            sys.exit(0)

        _, errmsg = execute(conn, sql_stat_activity.format(table_name=args.snapshot_storage_table))
        if errmsg:
            err_count += 1
        if err_count == ERROR_LIMIT:
            logger.error('error limit of %s reached. exiting. last error was: %s', ERROR_LIMIT, errmsg)
            sys.exit(1)

        time.sleep(args.interval_ms / 1000.0)


if __name__ == '__main__':
    main()
