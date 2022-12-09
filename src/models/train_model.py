import logging
import os
import pickle

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import RandomizedSearchCV, GroupKFold
from sklearn.metrics import make_scorer, mean_pinball_loss
from xgboost import XGBRegressor

from src import config
from src.data import make_dataset
from src.features import build_features
from multiprocessing import Pool

from src.mod import setup_logging_handler


def save_model(file_name, model_obj):
    path = os.path.join(config.base_dir, 'models')
    pickle.dump(model_obj, open(os.path.join(path, file_name), "wb"))
    logging.info("Saved model locally to [{}] as [{}]".format(path, file_name))
    return path


def save_model_train_date(format_code, score_date):
    path = os.path.join(config.base_dir, 'models')
    with open(os.path.join(path, 'train_dates_' + format_code + '.txt'), 'a+') as output:
        output.write(format_code + ', ' + score_date + '\n')
    return path


def prep_data_for_training(config_dict=None, format_code_lower=None, scoring_date=None, base_dir=None, target_col=None):
    try:
        df = pd.read_pickle(os.path.join(base_dir, 'data', 'processed',
                                         "rr_demo_features_train_" + format_code_lower + '.pkl'))

        # filter for weeks only prior to score date
        df = df[df.week_dt < pd.to_datetime(scoring_date)]
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.replace(to_replace=[None], value=np.nan, inplace=True)
        df.dropna(subset=target_col, inplace=True)
    except:
        logging.exception("Uncaught exception: prep_data_for_training()")
        logging.error(
            "[ERROR] [ADDS-NOTIFY] Research Response model training failed for format[{}]."
            " See Cloudwatch logs for details".format(
                format_code_lower))

    return df


def group_data_by_demo_category(df):
    return df.groupby(['demo_category'])


def final_model_fitting(format_code, config_dict):
    setup_logging_handler()
    format_code_upper = format_code
    if format_code_upper not in config.no_research_formats.keys():
        try:
            format_code_lower = format_code.lower()
            base_dir = config_dict['base_dir']
            scoring_date = config_dict['scoring_date']
            start_date = config_dict['start_date']
            target_col = config_dict['target']

            logging.info(
                "[ADDS-NOTIFY] Starting Research Response model training for format[{}]".format(format_code_upper))
            logging.info('training start_date [{}] for format[{}]'.format(start_date, format_code_upper))
            logging.info('training scoring_date [{}] for format[{}]'.format(scoring_date, format_code_upper))

            input_sql_path = make_dataset.make_input_path(base_dir)
            # make_dataset.build_tables(input_sql_path, format_code_upper, config_dict, train_score_flag='train')
            # build_features.feature_create(format_code_lower=format_code_lower, format_code_upper=format_code_upper,
            #                               config_dict=config_dict, train_score_flag='train')

            df = prep_data_for_training(config_dict, format_code_lower, scoring_date, base_dir,
                                        target_col)
            logging.info(pd.unique(df['demo_category']))
            df_grouped = group_data_by_demo_category(df)

            for grp in df_grouped.groups:
                print(grp)
                if ~pd.isna(grp) and len(df_grouped.get_group(grp)) > 0 and grp != 'Total':
                    df_grp = df_grouped.get_group(grp)
                    feature_cols = list(set(df_grp.columns) - set(config_dict['id_cols']) - set(target_col))

                    X = df_grp[feature_cols]
                    y = df_grp[config_dict['target']]
                    idx = X.dropna().index
                    df_temp = pd.DataFrame(X.apply(lambda x: x.isnull().sum() / (1.0 * len(x))), columns=['perc_avl'])
                    df_temp.sort_values(by=['perc_avl'], ascending=False, inplace=True)
                    # logging.info(grp)
                    # print(df_temp.reset_index().iloc[4].T)
                    #
                    # logging.info(len(X))
                    # logging.info(len(idx))
                    # continue

                    group_kfold = GroupKFold(n_splits=config_dict['cv'])

                    # use group based on SongID
                    cv = group_kfold.split(X.loc[idx], y.loc[idx], df.loc[idx][config_dict['id_cols'][0]])
                    param_grid = config_dict['param_grid']

                    # train model for mean pop score given features
                    cv = group_kfold.split(X.loc[idx], y.loc[idx], df.loc[idx][config_dict['id_cols'][0]])
                    model_mean = GradientBoostingRegressor(loss="squared_error")

                    rs_mean = RandomizedSearchCV(
                        model_mean,
                        param_grid,
                        n_iter=config_dict['n_iter'],
                        scoring='neg_mean_absolute_percentage_error',
                        cv=cv,
                        verbose=2,
                        random_state=0
                    )

                    rs_mean.fit(X.loc[idx], np.ravel(y.loc[idx]))
                    logging.info("Fitting for mean pop completed")

                    # train model for upper threshold given features
                    cv = group_kfold.split(X.loc[idx], y.loc[idx], df.loc[idx][config_dict['id_cols'][0]])
                    neg_mean_pinball_loss_high = make_scorer(
                        mean_pinball_loss,
                        alpha=config_dict['high_alpha'],
                        greater_is_better=False,  # maximize the negative loss
                    )

                    model_high_thresh = GradientBoostingRegressor(loss="quantile", alpha=config_dict['high_alpha'],
                                                                  random_state=0)

                    rs_high_thresh = RandomizedSearchCV(
                        model_high_thresh,
                        param_grid,
                        n_iter=config_dict['n_iter'],
                        scoring=neg_mean_pinball_loss_high,
                        cv=cv,
                        verbose=2,
                        random_state=0
                    )

                    rs_high_thresh.fit(X.loc[idx], np.ravel(y.loc[idx]))
                    logging.info("Fitting for upper wobble threshold completed")

                    # train model for lower threshold given features
                    cv = group_kfold.split(X.loc[idx], y.loc[idx], df.loc[idx][config_dict['id_cols'][0]])
                    neg_mean_pinball_loss_low = make_scorer(
                        mean_pinball_loss,
                        alpha=config_dict['low_alpha'],
                        greater_is_better=False,  # maximize the negative loss
                    )

                    model_low_thresh = GradientBoostingRegressor(loss="quantile", alpha=config_dict['low_alpha'],
                                                                 random_state=0)

                    rs_low_thresh = RandomizedSearchCV(
                        model_low_thresh,
                        param_grid,
                        n_iter=config_dict['n_iter'],
                        scoring=neg_mean_pinball_loss_low,
                        cv=cv,
                        verbose=2,
                        random_state=0
                    )

                    rs_low_thresh.fit(X.loc[idx], np.ravel(y.loc[idx]))
                    logging.info("Fitting for lower wobble threshold completed")

                    logging.info("Finished model training for Demographic Category" + grp + " of format[{}]".format(
                        format_code_upper))
                    best_score = [rs_mean.best_score_, rs_low_thresh.best_score_, rs_high_thresh.best_score_]
                    best_params = [rs_mean.best_params_, rs_low_thresh.best_params_, rs_high_thresh.best_params_]
                    best_model = [rs_mean.best_estimator_, rs_low_thresh.best_estimator_,
                                  rs_high_thresh.best_estimator_]
                    logging.info("Best score: [{}] for format[{}]".format(best_score, format_code_upper))
                    # logging.info("Best params for format[{}]: ".format(format_code_upper))
                    # for param_name in sorted(best_params.keys()):
                    # logging.info('%s: %r for format[{}]'.format(format_code_upper) % (param_name, best_params[param_name]))

                    save_model("xgb_reg_model_" + grp + "_" + format_code_lower + ".pkl", best_model)
                    save_model_train_date(format_code_lower, scoring_date)

            build_features.delete_temp_tables("adds_temp", format_code_lower, config_dict)

            logging.info(
                "[ADDS-NOTIFY] Completed Research Response model training for format[{}]".format(format_code_upper))
        except:
            logging.exception("Uncaught exception for format:".format(format_code_upper))
            logging.error(
                "[ERROR] [ADDS-NOTIFY] Research Response model training failed for format[{}]."
                " See Cloudwatch logs for details".format(
                    format_code_upper))
    else:
        logging.info("No Research for format[{}]".format(format_code_upper))


def train():
    # get all the properties in the config module as a dict.   Need to pass this to new processes
    config_dict = config.config_to_dict(config)
    pool = Pool(processes=config.concurrency)
    for format_code in config.format_codes:
        pool.apply_async(final_model_fitting, (format_code, config_dict))
    pool.close()
    pool.join()
    logging.info("=============== all formats finished =================")


if __name__ == '__main__':
    from src import mod

    try:
        train()
    except:
        logging.error(
            "[ERROR] [ADDS-NOTIFY] Research Response model training failed.  See Cloudwatch logs for details")
        logging.exception(
            "Uncaught exception:")
