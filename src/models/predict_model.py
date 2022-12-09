import logging
import os
import pickle
from multiprocessing import Pool
from time import perf_counter

import numpy as np
import pandas as pd
from tenacity import retry, wait_exponential, stop_after_attempt

from src import config
from src.data import make_dataset
from src.data.db_util import elapsed_time_logging_string
from src.data.db_util import postgresql_engine
from src.features import build_features
from src.mod import setup_logging_handler


@retry(wait=wait_exponential(multiplier=1, min=5, max=10), stop=stop_after_attempt(10))
def save_scores(df, table_name, format_code, scoring_date, config_dict, grp):
    engine = postgresql_engine(config_dict['db_write_user'],
                               config_dict['db_write_password'],
                               config_dict['db_write_url'],
                               config_dict['db_write_port'],
                               config_dict['db_write_name'])

    df.columns = df.columns.str.lower()
    with engine.connect() as connection:
        # delete existing records for existing week/format
        if scoring_date:
            logging.info(
                "Deleting existing records for format[{}] in table[{}]...".format(format_code.upper(), table_name))
            connection.execute(
                "delete from dbo." + table_name + " where format=\'" + format_code.upper() + "\' and week_dt >= \'" +
                scoring_date[0] + "\' and week_dt <= \'" +
                scoring_date[1] + "\'" + " and demo_category =\'" + grp + "\'")
        logging.info(
            "Saving [{}] new records for format[{}] in table[{}]...".format(len(df), format_code.upper(), table_name))
        df.to_sql(table_name, con=connection, index=False, if_exists='append', schema='dbo', method='multi',
                  chunksize=10000)


def clean_path(path):
    return path.replace(' ', '_')


def prep_data_for_scoring(base_dir=None, format_code_lower=None, scoring_date=None):
    df_score = pd.read_pickle(os.path.join(base_dir, 'data', 'processed',
                                           "rr_demo_features_score_" + format_code_lower + '.pkl'))
    # filter for weeks only after score date
    df_score = df_score[df_score.week_dt >= pd.to_datetime(scoring_date)]
    df_score.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_score.replace(to_replace=[None], value=np.nan, inplace=True)
    return df_score


def score_demo_category(df_score, demo_category, base_dir, format_code_lower, format_code_upper, config_dict):
    """
    Score songs for input demographic category
    @param df_score: DataFrame with songs to be scored
    @param demo_category: Demographic category to be scored
    @param base_dir: Base directory path
    @param format_code_lower: format code in lowercase
    @param format_code_upper: format code in uppercase
    @param config_dict: configuration dictionary

    @return: None (modified df_score in place)
    """
    try:
        model_path = os.path.join(base_dir, 'models',
                                  "xgb_reg_model_" + demo_category + "_" + format_code_lower + ".pkl")
        final_model = pickle.load(open(os.path.join(model_path), "rb"))
        cols_when_model_builds = final_model[0].feature_names_in_
        # col_list = list(set().union(df_score.columns, cols_when_model_builds))

        idx = df_score[df_score['demo_category'] == demo_category][cols_when_model_builds].dropna().index

        # df_score.loc[idx] = df_score.loc[idx].reindex(columns=col_list, fill_value=np.nan)
        df_score['pop_predicted'] = pd.DataFrame(final_model[0].predict(df_score.loc[idx][cols_when_model_builds]).flatten(), index=idx)
        df_score['wobble_lower_threshold'] = pd.DataFrame(final_model[1].predict(
            df_score.loc[idx][cols_when_model_builds]).flatten(), index=idx)
        df_score['wobble_upper_threshold'] = pd.DataFrame(final_model[2].predict(
            df_score.loc[idx][cols_when_model_builds]).flatten(), index=idx)

        df_score['score_dt'] = pd.to_datetime("now").date().strftime('%Y-%m-%d')  # scoring_date
        df_score['format'] = format_code_upper  # format_code_lower
        df_score['model_version'] = config_dict['model_version']


    except FileNotFoundError:
        logging.info('File not found')


def prediction_output(format_code, config_dict):
    try:
        format_code_upper = format_code
        start = perf_counter()
        setup_logging_handler()
        base_dir = config_dict['base_dir']
        scoring_date = config_dict['scoring_date']
        format_code_lower = format_code.lower()
        start_date = config_dict['start_date']
        curr_date = pd.to_datetime("now").date().strftime('%Y-%m-%d')

        logging.info(
            "[ADDS-NOTIFY] Starting Research Response model scoring for format[{}]".format(format_code_lower.upper()))
        logging.info('start_date {}'.format(start_date))
        logging.info('scoring_date {}'.format(scoring_date))

        input_sql_path = make_dataset.make_input_path(base_dir)
        #make_dataset.build_tables(input_sql_path, format_code_upper, config_dict, train_score_flag='score')
        # build_features.feature_create(format_code_lower=format_code_lower, format_code_upper=format_code_upper,
        #                               config_dict=config_dict, train_score_flag='score')

        df_score = prep_data_for_scoring(base_dir, format_code_lower, scoring_date)
        df_score_grouped = df_score.groupby(['demo_category'])

        for grp in df_score_grouped.groups:
            if ~pd.isna(grp) and len(df_score_grouped.get_group(grp)) > 0 and grp != 'Total':
                df_temp = df_score_grouped.get_group(grp)
                if df_score_grouped.get_group(grp).shape[0] > 0:
                    score_demo_category(df_temp, grp, base_dir, format_code_lower, format_code_upper, config_dict)

            logging.info(list(df_score.columns))
            # save scores to DB
            save_scores(
                df_temp[config_dict['id_cols'][:-1] + config_dict['target'] + ['format', 'pop_predicted',
                                                                                'wobble_lower_threshold',
                                                                                'wobble_upper_threshold', 'score_dt',
                                                                                'model_version']],
                "rr_demo_scores_adds",
                format_code_lower,
                [scoring_date, curr_date], config_dict, grp)
            logging.info("[ADDS-NOTIFY] Research Response scoring for format [{}] complete.".format(
                format_code_lower.upper()))

            logging.info("[ADDS-NOTIFY] Starting Research Response scoring explainer for format [{}]".format(
                format_code_lower.upper()))

            # save copies locally
            df_temp[
                config_dict['id_cols'][:-1] + config_dict['target'] + ['format', 'pop_predicted',
                                                                                'wobble_lower_threshold',
                                                                                'wobble_upper_threshold', 'score_dt',
                                                                                'model_version']].to_pickle(
                os.path.join(base_dir, "data", "processed", "rr_scores" + "_" + format_code_lower + "_" + grp + ".pkl"), protocol=4)

        else:
            logging.info("[ERROR] Empty Scoring data for format [{}]".format(format_code_lower.upper()))

        build_features.delete_temp_tables("adds_temp", format_code_lower, config_dict)

        stop = perf_counter()
        logging.info("[ADDS-NOTIFY] Research Response scoring explainer for format [{}] complete".format(
            format_code_lower.upper()))
        logging.info('Format[{}] complete.  {}'.format(format_code, elapsed_time_logging_string(start, stop)))
    except:
        logging.exception("Uncaught exception for format[{}]:".format(format_code))
        logging.error(
            "[ERROR] [ADDS-NOTIFY] Research Response model scoring failed for format[{}].  "
            "See Cloudwatch logs for details".format(
                format_code))


def predict():
    # get all the properties in the config module as a dict.   Need to pass this to new processes
    config_dict = config.config_to_dict(config)

    pool = Pool(processes=config.concurrency)
    for format_code in config.format_codes:
        pool.apply_async(prediction_output, (format_code, config_dict))
    pool.close()
    pool.join()

    logging.info("[ADDS-NOTIFY] Research Response - scoring for all formats is complete.")


if __name__ == '__main__':
    from src import mod

    try:
        predict()
    except:
        logging.error(
            "[ERROR] [ADDS-NOTIFY] Research Response model scoring failed.  See Cloudwatch logs for details")
        logging.exception(
            "Uncaught exception:")
