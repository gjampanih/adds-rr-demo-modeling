# set in mod
db_process_user = None
db_process_password = None
db_process_url = None
db_process_port = None
db_process_name = None

#add separate DB for final write
db_write_user = None
db_write_password = None
db_write_url = None
db_write_port = None
db_write_name = None

env = None

start_date = None
scoring_date = None
base_dir = None
format_codes = None
backfill_dates = None
scoring_range = None
concurrency = 1
explainer = 1

# for notifying Musiclab upon scoring completion
scoring_notify_enabled = False
scoring_notify_role_arn = None
scoring_notify_host = None
scoring_notify_env = None

#dynamoDB model completion table
write_to_dynamo_enabled = False
model_execution_completion_table = 'adds-application-status'

# constants
num_cols_like = ['artist_count', 'feat_artist', 'feat_artist_song', 'mscore', 'spins','pop_prior',
                 'pop_artist_prior', 'song_age_weeks', 'song_last_test']
cat_cols_like = ['flag', '^genre','Market_Name', 'taa_quintile', 'segment', 'omt_co_flag']
target = ['pop_all']
id_cols = ['mediabase_id', 'station_id', 'week_dt', 'breakout_id', 'demo_category', 'pop_co', 'pop_omt','gcr','gcr_adj']

exclude_cols_like = ['date', 'song_last_test_omt_weeks', 'perc_diff_spins_artist_station_prior',
                     'std_pop_prior', 'std_pop_artist_prior']#,'_unv']#,'univ_spins', 'market_spins']
use_local_over_unv = False

# param_grid = dict(
#     learning_rate=[.2, .1, .05, .01, .005],
#     n_estimators=[10, 20, 50, 100, 150, 200],
#     max_depth=[2, 4, 6, 8, 10],
#     min_samples_leaf=[1, 5, 10, 20],
#     min_samples_split=[5, 10, 20, 30, 50]
# )


param_grid = dict(
    learning_rate=[.2, .1, .05],
    n_estimators=[5, 10, 15],
    max_depth=[2, 4, 6],
    min_samples_leaf=[5, 10, 20],
    min_samples_split=[5, 10, 20]
)

#column to base CRE tier on, gcr or gcr_adj
tiering_col = 'gcr_adj'
n_iter=50
cv=2

#what model to use when a format cannot be trained
no_research_formats = {'L2':'L1', 'U4':'U2'}
#what adjustment to make based on format
grc_adj_formats = {'default': {'R':'12 weeks', 'G':'24 months'},
                   'C1': {'R':'12 weeks', 'G':'36 months'},
                   'A1': {'R':'52 weeks', 'G':'24 months'},
                   'U2': {'R':'104 weeks', 'G':'60 months'},
                   'R2': {'R':'104 weeks', 'G':'36 months'},
                   'R3': {'R':'104 weeks', 'G':'36 months'}}
#spins filter by format
spins_filter_format = {'H1':300, 'C1':300, 'default':300}
model_version = None
#add stations to exclude for different formats to filter rules
filter_rules = '''and song_weeks_since_last_spins <=13 
and ((song_last_test_co_weeks <=26) 
or (song_last_test_omt_weeks <=104) 
or format_code in ('u4','l2')) 
--and (((station_test_1_plus=0 and station_test_1_id=1) or station_test_1_plus>0) or format_code in ('u4','l2'))
and (format_code<>'h1' or station_id<>3323403) 
and (format_code<>'c1' or station_id<>3322825) 
and (format_code<>'a2' or station_id<>3322799 or gcr<>'G') 
and taa_quintile is not null
'''

temp_table_names = ['demo_songs_{}', 'demo_song_subset_train_{}', 'demo_station_song_subset_train_{}',
                    'demo_song_subset_score_{}', 'demo_station_song_subset_score_{}',
                    'demo_song_subset_backfill_{}', 'demo_station_song_subset_backfill_{}', 'demo_song_subset_{}_train',
                    'demo_station_song_subset_{}_train',
                    'demo_song_subset_{}_score', 'demo_station_song_subset_{}_score', 'demo_song_subset_{}_backfill',
                    'demo_station_song_subset_{}_backfill', 'demo_mb_{}', 'demo_cm_{}',
                    'demo_rr_temp_{}']  # , 'rr_features']

# Create Mapping between breakout names and segments
breakout_category = {'*Core*': 'Core-Cume', '*Old*': 'Age', '*Young*': 'Age', 'Total': 'Total', 'White': 'Race',
                     'Non-Core': 'Core-Cume',
                     'Hispanic': 'Race', 'AA': 'Race', 'F': 'Gender', 'M': 'Gender', 'WAO': 'Race',
                     'F (25-29)': 'Gender', 'F (20-24)': 'Gender', 'F (18-29)': 'Gender', 'F (17-29)': 'Gender',
                     'F (20-23)': 'Gender',
                     'F (16-24)': 'Gender', 'F (30-34)': 'Gender', 'F (18-34)': 'Gender', 'F (24-29)': 'Gender',
                     'F (17-19)': 'Gender', 'F (15-26)': 'Gender', 'F (15-19)': 'Gender', 'F (15-24)': 'Gender',
                     'F (18-24)': 'Gender', 'F (20-29)': 'Gender', 'F (25-34)': 'Gender', 'F (Other)': 'Gender'}

breakout_map = {'*Core*': 'Core', '*Old*': 'Old', '*Young*': 'Young', 'Total': 'Total', 'White': 'White',
                'Non-Core': 'Non-Core',
                'Hispanic': 'Hispanic', 'AA': 'AA', 'F': 'Female', 'M': 'Male', 'WAO': 'White', 'F (25-29)': 'Female',
                'F (20-24)': 'Female', 'F (18-29)': 'Female', 'F (17-29)': 'Female', 'F (20-23)': 'Female',
                'F (16-24)': 'Female', 'F (30-34)': 'Female', 'F (18-34)': 'Female', 'F (24-29)': 'Female',
                'F (17-19)': 'Female',
                'F (15-26)': 'Female', 'F (15-19)': 'Female', 'F (15-24)': 'Female',
                'F (18-24)': 'Female_(18-24)', 'F (20-29)': 'Female', 'F (25-34)': 'Female',
                'F (Other)': 'Female_Other'}

low_alpha = 0.05
high_alpha = 0.95

def config_to_dict(module):
    context = {}
    for setting in dir(module):
        if (setting in ['concurrency', 'db_process_user', 'db_process_password', 'db_process_url', 'db_process_port',
                        'db_process_name', 'env', 'start_date', 'scoring_date', 'base_dir', 'num_cols_like',
                        'cat_cols_like',
                        'target', 'id_cols', 'param_grid', 'n_iter', 'cv', 'filter_rules', 'explainer',
                        'backfill_dates', 'model_version', 'scoring_range',
                        'db_write_user', 'db_write_password', 'db_write_url', 'db_write_port', 'db_write_name',
                        'env_results', 'tiering_col', 'exclude_cols_like', 'use_local_over_unv',
                        'scoring_notify_enabled', 'scoring_notify_role_arn', 'scoring_notify_host',
                        'scoring_notify_env', 'write_to_dynamo_enabled', 'model_execution_completion_table',
                        'temp_table_names', 'breakout_category', 'breakout_map', 'low_alpha', 'high_alpha']):
            context[setting] = getattr(module, setting)

    return context
