import sys
import logging
from sqlalchemy import create_engine
from tenacity import retry, stop_after_attempt, wait_fixed
from psycopg2 import connect, OperationalError


# create a connection using SQLAlchemy
# @retry(wait=wait_fixed(10), stop=stop_after_attempt(10))
def postgresql_engine(user, pwd, host, port, dbname):
    sql_engine = create_engine('postgres://' + user + ':' + pwd + '@' + host + ':' + port + '/' + dbname,
                               echo=False, pool_pre_ping=True)
    return sql_engine


def retry_before_sleep(retry_state):
    if retry_state.attempt_number > 0:
        logging.warning('SQL connection retry number [{}]'.format(retry_state.attempt_number))


# create a connection using psycopg2
@retry(reraise=True, wait=wait_fixed(20), stop=stop_after_attempt(10), before_sleep=retry_before_sleep)
def psycopg2_connect(config_dict):
    try:
        conn = connect(user=config_dict['db_process_user'],
                       password=config_dict['db_process_password'],
                       host=config_dict['db_process_url'],
                       port=config_dict['db_process_port'],
                       database=config_dict['db_process_name'])
        return conn
    except OperationalError as err:
        logging.error("DB connection error")
        logging.error(format_psycopg2_exception(err))
        raise err


def elapsed_time_logging_string(start, stop):
    return " Elapsed time[{}] elasped in seconds[{}]".format(convert_seconds_to_string(start, stop),
                                                             str(int(stop) - int(start)))


def convert_seconds_to_string(start, stop):
    elapsed_seconds = int(stop) - int(start)
    min, sec = divmod(elapsed_seconds, 60)
    hour, min = divmod(min, 60)
    return "%d:%02d:%02d" % (hour, min, sec)


def format_psycopg2_exception(err):
    err_type, err_obj, traceback = sys.exc_info()
    line_num = traceback.tb_lineno
    m = f"psycopg2 ERROR: [{err}] on line number [{line_num}]"
    m = m + f"\npsycopg2 traceback: [{traceback}]  type: [{err_type}]"
    m = m + f"\npgerror: [{err.pgerror}]"
    m = m + f"\npgcode: [{err.pgcode}]"
    return m
