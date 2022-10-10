import logging
import os
from time import perf_counter
from tenacity import retry, stop_after_attempt, wait_fixed

from psycopg2 import Error, sql
from sqlalchemy import text

from src.data.db_util import psycopg2_connect, elapsed_time_logging_string, format_psycopg2_exception
from src import config


def build_tables(input_filepath, format_code, config_dict, train_score_flag):
    logging.info('Starting table creation for format[{}]'.format(format_code))

    fd = open(input_filepath, 'r')
    sql_file = fd.read()
    fd.close()
    format_code_research = format_code.lower()
    spins_filter = config.spins_filter_format['default']
    c_to_r = config.grc_adj_formats['default']['R']
    r_to_g = config.grc_adj_formats['default']['G']
    if format_code in config.no_research_formats.keys():
        format_code_research = config.no_research_formats[format_code].lower()
    if format_code in config.spins_filter_format.keys():
        spins_filter = config.spins_filter_format[format_code]
    if format_code in config.grc_adj_formats.keys():
        c_to_r = config.grc_adj_formats[format_code]['R']
        r_to_g = config.grc_adj_formats[format_code]['G']

    raw_text = text(sql_file).text
    query = raw_text.replace('{format_code}', format_code.lower()) \
        .replace('{start_date}', config_dict['start_date']) \
        .replace('%', '%%') \
        .replace('{train_score}', train_score_flag) \
        .replace('{score_date}', config_dict['scoring_date']) \
        .replace('{spins_filter}', str(spins_filter)) \
        .replace('{c_to_r}', c_to_r) \
        .replace('{r_to_g}', r_to_g) \
        .replace('{format_code_research}', format_code_research)

    sql_commands = query.split(';;')
    for i, command in enumerate(sql_commands):
        execute_sql(command, i, config_dict, format_code)
    logging.info('Completed table creation for format[{}]'.format(format_code))


def retry_before_sleep(retry_state):
    if retry_state.attempt_number > 0:
        logging.warning('SQL retry number [{}]'.format(retry_state.attempt_number))


# using native psycopg2
@retry(reraise=True, wait=wait_fixed(20), stop=stop_after_attempt(2), before_sleep=retry_before_sleep)
def execute_sql(sql_text, i, config_dict, format_code):
    connection = None
    start = perf_counter()
    try:
        connection = psycopg2_connect(config_dict)
        connection.set_session(autocommit=True)
        cursor = connection.cursor()
        print(sql_text)
        sql_statement = sql.SQL(sql_text)

        cursor.execute(sql_statement)
        stop = perf_counter()
        logging.info('SQL statement ' + str(i) + ' for format[{}] succeeded.  {}'.format(format_code,
                                                                                         elapsed_time_logging_string(
                                                                                             start, stop)))
    except (Exception, Error) as error:
        stop = perf_counter()
        logging.error('SQL statement ' + str(i) + ' for format[{}] FAILED.  {}'.format(format_code,
                                                                                       elapsed_time_logging_string(
                                                                                           start, stop)))
        logging.error(format_psycopg2_exception(error))
        raise RuntimeError('Fatal database error. Aborting format[{}]'.format(format_code)) from error
    finally:
        if connection:
            cursor.close()
            connection.close()


def make_input_path(base_dir):
    return os.path.join(base_dir, 'data', 'sql', 'features_parameter.sql')


if __name__ == '__main__':
    from src import mod, config
    config_dict = config.config_to_dict(config)
    print(config_dict)
    build_tables(
        input_filepath='/Users/girishhanumantha/Documents/Github/adds-rr-demo-modeling/data/sql/features_parameter.sql',
        format_code=config.format_codes[0].lower(), config_dict=config_dict, train_score_flag='train')
