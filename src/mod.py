import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
import multiprocessing_logging

from dateutil.relativedelta import relativedelta, SU

from src import config


def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def remove_file(path):
    if os.path.exists(path):
        os.remove(path)


def get_python_version():
    return "python version [{}]".format(str(sys.version_info.major) + "." + str(sys.version_info.minor))


def read_application_version():
    with open(os.path.join(config.base_dir, "version.txt"), "r") as file:
        return file.read()


def setup_logging_handler():
    multiprocessing_logging.install_mp_handler()

    base_dir = Path(__file__).parents[1]

    file_path = os.path.join(base_dir, 'logs/research-response.log')

    logging.basicConfig(filename=file_path,
                        format="[%(asctime)s] %(process)d %(levelname)s %(name)s:%(funcName)s:%(lineno)s - %(message)s",
                        level=logging.INFO)


try:
    base_dir = Path(__file__).parents[1]
    config.base_dir = base_dir
    log_directory = os.path.join(base_dir, 'logs')
    make_dir(log_directory)

    setup_logging_handler()
    logging.info(get_python_version())
    logging.info("application version [{}]".format(str(read_application_version())))

    config.model_version = 'adds_v' + read_application_version()

    parser = argparse.ArgumentParser(description='Run Research Response model.')
    parser.add_argument('--db_process_user', required=True, help='Postgres username')
    parser.add_argument('--db_process_password', required=True, help='Postgres password')
    parser.add_argument('--db_process_url', required=True, help='Postgres URL')
    parser.add_argument('--db_process_name', required=True, help='Postgres database name')
    parser.add_argument('--db_process_port', required=True, help='Postgres port')

    parser.add_argument('--db_write_user', required=True, help='Postgres username for results')
    parser.add_argument('--db_write_password', required=True, help='Postgres password for results')
    parser.add_argument('--db_write_url', required=True, help='Postgres URL for results')
    parser.add_argument('--db_write_name', required=True, help='Postgres database name for results')
    parser.add_argument('--db_write_port', required=True, help='Postgres port for results')

    parser.add_argument('--env', required=True, help='environment', choices=['DEV', 'PROD'])

    parser.add_argument('--scoring_date', required=False,
                        help='Scoring date.  If missing defaults to today. Format YYYY-MM-DD')
    parser.add_argument('--start_date', required=False,
                        help='Start date.  If missing defaults to today. Format YYYY-MM-DD')
    parser.add_argument('--format_codes', required=True,
                        help='Format code')
    parser.add_argument('--backfill_start', required=False,
                        help='Backfill start date.  Format YYYY-MM-DD')
    parser.add_argument('--backfill_end', required=False,
                        help='Backfill end date.  Format YYYY-MM-DD')
    parser.add_argument('--concurrency', required=False,
                        help='Number of concurrent processes')
    parser.add_argument('--notify', required=False, choices=['True', 'False'])
    parser.add_argument('--notify_host', required=False)
    parser.add_argument('--notify_role_arn', required=False)

    parser.add_argument('--write_to_dynamo', required=False, choices=['True', 'False'])

    args = parser.parse_args()
    print(args.db_process_user)

    if args.write_to_dynamo is not None:
        config.write_to_dynamo_enabled = True
        logging.info("write_to_dynamo [{}]".format(args.write_to_dynamo))
    else:
        config.write_to_dynamo_enabled = False
        logging.info("write_to_dynamo argument not supplied.  Will default to False")

    if args.notify is not None:
        config.scoring_notify_enabled = args.notify == 'True'
        logging.info("notify [{}]".format(args.notify))
    else:
        config.scoring_notify_enabled = False
        logging.info("notify argument not supplied.  Will default to False")
    # notifications to Musiclab upon scoring completion
    if config.scoring_notify_enabled:
        config.scoring_notify_enabled = True
        config.scoring_notify_host = args.notify_host
        logging.info("scoring_notify_host [{}]".format(args.notify_host) )
        config.scoring_notify_role_arn = args.notify_role_arn
        logging.info("scoring_notify_role_arn [{}]".format(args.notify_role_arn))
        if args.env == 'DEV':
            config.scoring_notify_env = 'qa'
        else:
            config.scoring_notify_env = 'prod'
        logging.info("Musiclab notification environment set to [{}]".format(config.scoring_notify_env))

    if args.concurrency is not None:
        config.concurrency = int(args.concurrency)
    logging.info("concurrency [{}]".format(config.concurrency))

    config.format_codes = args.format_codes.split(",")
    logging.info("format_codes {}".format(config.format_codes))

    if args.backfill_start is not None and args.backfill_end is not None:
        config.backfill_dates = [args.backfill_start, args.backfill_end]
        logging.info("backfill_start [{}]  backfill_end[{}]".format(config.backfill_dates[0], config.backfill_dates[1]))

    # start_date needs to be in format "2019-12-02"
    if args.start_date is None:
        d = datetime.now() - relativedelta(years=2)
        config.start_date = "{:%Y-%m-%d}".format(d)
    else:
        config.start_date = args.start_date

    # scoring_date needs to be in format "2019-12-02"
    if args.scoring_date is None:
        d = datetime.now() + relativedelta(weekday=SU(-1), weeks=-1)
        config.scoring_date = "{:%Y-%m-%d}".format(d)
        config.scoring_range = 0
    else:
        config.scoring_date = args.scoring_date
        config.scoring_range = 1

    config.env = args.env
    logging.info("env [{}]".format(config.env))

    config.db_process_user = args.db_process_user
    config.db_process_password = args.db_process_password
    config.db_process_port = args.db_process_port
    config.db_process_url = args.db_process_url
    config.db_process_name = args.db_process_name

    config.db_write_user = args.db_write_user
    config.db_write_password = args.db_write_password
    config.db_write_port = args.db_write_port
    config.db_write_url = args.db_write_url
    config.db_write_name = args.db_write_name

    logging.info("db_process_user [{}]".format(config.db_process_user))
    logging.info("db_process_port [{}]".format(config.db_process_port))
    logging.info("db_process_url [{}]".format(config.db_process_url))
    logging.info("db_process_name [{}]".format(config.db_process_name))

    logging.info("db_write_user [{}]".format(config.db_write_user))
    logging.info("db_write_port [{}]".format(config.db_write_port))
    logging.info("db_write_url [{}]".format(config.db_write_url))
    logging.info("db_write_name [{}]".format(config.db_write_name))


except:
    logging.error(
        "[ERROR] [ADDS-NOTIFY] Research Response application failed.  See Cloudwatch logs for details")
    logging.exception(
        "Uncaught exception:")
    raise


