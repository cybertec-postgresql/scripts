#!/usr/bin/env python

import logging
import psycopg2

args = None
logger = None

sql_all_tables = """
select
  quote_ident(nspname) ||'.'|| quote_ident(relname)
from
  pg_class c
  join
  pg_namespace n on n.oid = c.relnamespace
where
  relkind = 'r'
  and not nspname like any(array[E'pg\\_%', 'information_schema'])
"""


sql_count_rows = """select count(*) from only %s"""


def getDB1Connection(autocommit=True):
    """ if password is in use, .pgpass must be used """
    conn = psycopg2.connect(host=args.host1, port=args.port1, database=args.dbname1, user=args.username1)
    if autocommit:
        conn.autocommit = True
    return conn

def getDB2Connection(autocommit=True):
    """ if password is in use, .pgpass must be used """
    conn = psycopg2.connect(host=args.host2, port=args.port2, database=args.dbname2, user=args.username2)
    if autocommit:
        conn.autocommit = True
    return conn

def closeConnection(conn):
    if conn:
        try:
            conn.close()
        except:
            logging.exception('failed to close db connection')

def execute(conn, sql, params=None, statement_timeout=None):
    result = []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if statement_timeout:
            cur.execute("SET statement_timeout TO '{}'".format(
                statement_timeout))
        cur.execute(sql, params)
        if cur.statusmessage.startswith('SELECT') or cur.description:
            result = cur.fetchall()
        else:
            result = [{'rows_affected': str(cur.rowcount)}]
    except Exception as e:
        logging.exception('failed to execute "{}"'.format(sql))
        return result, str(e)
    return result, None


def executeOnDB1(sql, params=None, statement_timeout=None):
    conn = None
    try:
        conn = getDB1Connection()
    except Exception as e:
        logging.error('could not connect to DB1')
        return [], e.message

    try:
        return execute(conn, sql, params, statement_timeout)
    finally:
        closeConnection(conn)
        
def executeOnDB2(sql, params=None, statement_timeout=None):
    conn = None
    try:
        conn = getDB2Connection()
    except Exception as e:
        logging.error('could not connect to DB2')
        return [], e.message

    try:
        return execute(conn, sql, params, statement_timeout)
    finally:
        closeConnection(conn)

def exitOnErrormsg(errmsg, extra_description=None):
    if errmsg:
        if extra_description:
            logger.error("%s: %s", extra_description, errmsg)
        else:
            logger.error(errmsg)
        sys.exit(1)


def main():
    argp = argparse.ArgumentParser(description='Script to compare table counts between two databases')
    argp.add_argument('-q', '--quiet', help='Errors only', action='store_true')
    
    argp.add_argument('--host1', help='DB hostname', default='localhost')
    argp.add_argument('--port1', help='DB port', type=int, default='5432')
    argp.add_argument('--dbname1', help='DB name', default='postgres')
    argp.add_argument('--username1', help='DB Username', default=os.getenv('USER'))
    
    argp.add_argument('--host2', help='DB hostname', default='localhost')
    argp.add_argument('--port2', help='DB port', type=int, default='5432')
    argp.add_argument('--dbname2', help='DB name', default='postgres')
    argp.add_argument('--username2', help='DB Username', default=os.getenv('USER'))    


    global args
    args, unknown_args = argp.parse_known_args()


    logging.basicConfig(format='%(levelname)s %(message)s')
    global logger
    logger = logging.getLogger()
    logger.setLevel((logging.ERROR if args.quiet else logging.INFO))

    logger.info('getting list of tables on DB1 ...')
    list_of_tables1, errmsg = executeOnDB1(sql_all_tables)
    exitOnErrormsg(errmsg)
	logger.info('found %s tables', len(data))

    logger.info('counting tables on DB2 ...')
    list_of_tables2, errmsg = executeOnDB2(sql_all_tables)
    exitOnErrormsg(errmsg)
    logger.info('found %s tables', len(data))


if __name__ == '__main__':
    main()
