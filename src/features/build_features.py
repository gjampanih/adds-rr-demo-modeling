import logging
import os

import numpy as np
import pandas as pd
from src import config
from src.data.db_util import postgresql_engine
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta, SU


def save_data(output_df, file_name, config_dict, format_code_upper):
    file_path = os.path.join(config_dict['base_dir'], 'data', 'processed', file_name)
    output_df.to_pickle(file_path + '.pkl', protocol=4)
    # output_df.to_csv(file_path + '.csv', index=False)
    logging.info("Saved [{}] feature rows to pickle for format[{}]".format(len(output_df), format_code_upper))
    return file_path


def delete_temp_tables(schema, format_code_lower, config_dict):
    engine = postgresql_engine(config_dict['db_process_user'],
                               config_dict['db_process_password'],
                               config_dict['db_process_url'],
                               config_dict['db_process_port'],
                               config_dict['db_process_name'])

    with engine.connect() as connection:
        for table in config_dict['temp_table_names']:
            logging.info("Dropping table " + schema + "." + table.format(format_code_lower))
            connection.execute("drop table if exists " + schema + "." + table.format(format_code_lower))


def generate_filter_text(config_dict=None, train_score_flag='train'):
    """
    Generate the sql text to filter data for training/scoring
    """

    filter_text = ' where week_dt<=' + "'" + config_dict['scoring_date'] + "' " + 'and ' + 'week_dt>' + "'" + \
                  config_dict['start_date'] + "' " + 'and ' + config_dict['target'][0] + ' notnull ' + config_dict[
                      'filter_rules']
    score_date_end = (datetime.now() + relativedelta(weekday=SU(-1), weeks=-1)).strftime('%Y-%m-%d')

    if train_score_flag == 'score':
        filter_text = ' where week_dt>=' + "'" + config_dict['scoring_date'] + "' " + config_dict['filter_rules']
        if config_dict['scoring_range'] == 0:
            score_date_end = (pd.to_datetime(config_dict['scoring_date']) + timedelta(days=6)).strftime('%Y-%m-%d')
        filter_text = filter_text + ' and week_dt<=' + "'" + score_date_end + "' "
    return filter_text


def generate_cols(df, config_dict=None):
    """
    Process Columns and return ID columns, feature columns and categorical columns
    """

    # Map breakouts to category and clean up breakout names
    df['segment'] = df['breakout_name'].apply(
        func=(lambda x: config_dict['breakout_map'][x] if (x in config_dict['breakout_map'].keys()) else None))
    df['demo_category'] = df['breakout_name'].apply(
        func=(
            lambda x: config_dict['breakout_category'][x] if x in config_dict['breakout_category'].keys() else None))

    id_cols = config_dict['id_cols']
    target_col = config_dict['target']
    exclude_cols = df.columns[df.columns.str.contains('|'.join(config_dict['exclude_cols_like']), regex=True)]

    cat_cols = set(df.columns[df.columns.str.contains('|'.join(config_dict['cat_cols_like']), regex=True)]) - set(
        id_cols) - set(exclude_cols)
    num_cols = set(df.select_dtypes(exclude=['object', 'datetime64']).columns) & set(
        df.columns[(df.columns.str.contains('|'.join(config_dict['num_cols_like']), regex=True))]) - set(id_cols) - set(
        cat_cols) - set(exclude_cols)
    feature_cols = list(set(list(num_cols) + list(cat_cols)))

    return id_cols, feature_cols, num_cols, cat_cols, target_col


def feature_create(format_code_lower=None, format_code_upper='None', config_dict=None, train_score_flag='train'):
    logging.info("Starting feature creation for format[{}]".format(format_code_upper))

    engine = postgresql_engine(config_dict['db_process_user'],
                               config_dict['db_process_password'],
                               config_dict['db_process_url'],
                               config_dict['db_process_port'],
                               config_dict['db_process_name'])

    filter_text = generate_filter_text(config_dict, train_score_flag)

    with engine.connect() as connection:
        with connection.begin():
            df = pd.read_sql_query('select * from adds_temp.demo_rr_features_' + format_code_lower + filter_text,
                                   con=connection)
    logging.info("Finished reading data for format[{}]".format(format_code_upper))
    df.fillna(np.nan, inplace=True)  # fill None values

    id_cols, feature_cols, num_cols, cat_cols,  target_col = generate_cols(df, config_dict)

    for c in num_cols:
        df[c] = pd.to_numeric(df[c])

    df_features = pd.get_dummies(df[id_cols + feature_cols + list(target_col)], columns=cat_cols)
    filename = "rr_demo_features_" + train_score_flag + '_' + format_code_lower
    save_data(df_features, filename, config_dict, format_code_upper)
    logging.info("Feature creation complete for format[{}]".format(format_code_upper))


if __name__ == '__main__':
    from src import mod

    try:
        feature_create(format_code_lower=config.format_codes[0].lower(),
                       format_code_upper=config.format_codes[0].upper(),
                       config_dict=config.config_to_dict(config))
    except:
        logging.exception(
            "Uncaught exception:")
