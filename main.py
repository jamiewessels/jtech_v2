import numpy as np
import pandas as pd
import random
import matplotlib
import matplotlib.pyplot as plt
from datetime import date
import sys
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from sklearn.cluster import KMeans
matplotlib.rc('xtick', labelsize=15) 
from yellowbrick.cluster import KElbowVisualizer
from sklearn.preprocessing import StandardScaler
import json
import requests
from google.cloud import storage
from io import StringIO
from read_in_data_v2 import *
from jtech_fxns_v2 import *
from tagging_fxns_v2 import * 
from ab_split_fxns_v2 import *
from test_vars import *
from past_behavior_query import *


def get_daily_strings():
    with open('keys_v2.json', 'r') as file:
        config = json.load(file)
        return config["jwt"], config["bucket_name"]



if __name__ == '__main__':
    #defined inputs:
    today = str.replace(str(date.today()),"-","")
    jwt, bucket_name = get_daily_strings()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    cadence_group = 'session15' ##remove this? 

    test_name = '20240371NewScriptTest'
    package_names, platforms, k_clusters_list, cohort_names, cohort_types, iap_exp, segment, array_cols, excluded_offers = get_test_variables(test_name) ##how can we make this cleaner? 

    #import summary tables into pd dataframe
    iap_all, pb_all, pl_all, pm_all = (read_in_all_csvs(table, bucket) for table in ['iap_stats_v6', 'past_behavior_v2', 'price_lookups_v2', 'price_mapping'])

    for package_name, platform, k_clusters in zip(package_names, platforms, k_clusters_list):
        print(f'******* Starting {package_name} {platform} ********')
        iap, pb, pl, pm = (clean_cols(df, package_name, platform) for df in [iap_all, pb_all, pl_all, pm_all])
        iap = add_iap_cols(iap).copy()
        pb = pb[~pb.offer.isin(excluded_offers)].copy() 

        df_jtech, df_agg_offers, df_agg_price, jtech_segments, offers, scores, jtech_array, array_cspayers = jtech(pb, iap, pm, k_clusters, array_cols, click_wt=-0.2, msgs_wt=-0.1, iap_exp=iap_exp)

        ab_lookup = fetch_existing_ab_cohorts(df_jtech, package_name, platform, bucket, cohort_names=cohort_names, illegal_chars=['.', '-'], test_name=test_name, segment=segment, jwt = jwt)

        output_df, df_new_cohorts = get_tags_from_ab_splits(df_jtech, ab_lookup, package_name, platform, storage_client, bucket_name=bucket, fxn=get_tags_cadence_test, cohort_names = cohort_names, cohort_types = cohort_types, test_name=test_name, segment=segment, cadence_type=cadence_group)

        output_df = merge_pb_iap_stats(pb, cadence_group, output_df)

        
        print(output_df.groupby(['ab_cohort','tag']).count()['user_id'])

        # print(output_df.groupby(['segment','tag']).count()['user_id'])

        cadence_tags, cadence_ids = generate_api_inputs_diff_cadences(pl, cohort_names, cohort_types, cadence_group, test_name)