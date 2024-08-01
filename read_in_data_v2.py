import numpy as np
import pandas as pd
import random
import openpyxl
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from io import StringIO
import json
from google.cloud import storage
from io import StringIO
import os

def read_in_all_csvs(table_name, bucket):
    data = bucket.blob(table_name).download_as_bytes()
    string_data = StringIO(data.decode('utf-8'))
    output_df = pd.read_csv(string_data, on_bad_lines='warn')
    print(f'{table_name} ingested to pandas dataframe...')
    return output_df


def clean_cols(df, package_name, platform):
    #remove blank looker columns w/ id and name columns consistently
    cleaned_cols = [str.lower(str.replace(col_name, " ", "_")) for col_name in df.columns]
    df.columns = cleaned_cols
    df = df[(df.package_name == package_name) & (df.platform == platform)].drop(columns=['package_name', 'platform'])
    if 'unnamed:_0' in df.columns:
        df = df.drop(columns=['unnamed:_0'])
    return df

def add_iap_cols(iap):
    iap['avg_trxn_amt_7d'] = iap.iap_usd_last_7d / iap.iap_trxns_last_7d
    iap['avg_cs_trxn_amt_7d'] = iap.cs_iap_last_7d / iap.cs_iap_trxns_last_7d
    iap['avg_trxn_amt_30d'] = iap.iap_usd_last_30d / iap.iap_trxns_last_30d
    iap['avg_cs_trxn_amt_30d'] = iap.cs_iap_last_30d / iap.cs_iap_trxns_last_30d
    iap['sent_to_click'] =( iap.cs_clicks / iap.cs_sent).round(3)
    iap['cs_click_freq_10d'] = (iap.cs_clicks_10d / iap.cs_sent_10d).round(3)
    iap.replace([np.inf, -np.inf], 0, inplace=True)
    return iap


def read_in_single_csv(table_name, package_name, platform, bucket, clean_cols=None, illegal_chars = []):
    for char in illegal_chars:
        table_name = str.replace(table_name, char, "")

    data = bucket.blob(table_name).download_as_bytes()
    string_data = StringIO(data.decode('utf-8'))
    output_df = pd.read_csv(string_data)
    # print(f'{table_name} ingested to pandas dataframe...')
    return output_df




def upload_to_gcb(df, storage_client, bucket_name, package_name, platform, name, folder=None, illegal_chars = ['.', '-']):
    csv_data = df.to_csv(index=False)

    #remove illegal chars from package name
    for char in illegal_chars:
        package_name = str.replace(package_name, char, "")

    #specify save location
    if folder:
        blob_name = f'{folder}/{name}{package_name}{platform}'
    else:
        blob_name = f'{name}{package_name}{platform}'


    # create a new blob and upload the file's content.
    blob = bucket_name.blob(blob_name)
    blob.upload_from_string(csv_data, content_type='text/csv')

    print(f"Uploaded CSV to {blob_name} in bucket {bucket_name}")

    return None


def cvr_by_cadence_group(pb, cadence_group):
    df = pb.groupby(['user_id', 'cadence_group', 'event_name']).sum()['cadence_stats'].reset_index()
    df_pivot = df.pivot_table('cadence_stats', ['user_id', 'cadence_group'], 'event_name').fillna(0).reset_index()
    df_pivot['ctr'] = (df_pivot['click'] / df_pivot['sent']).fillna(0)
    df_pivot.replace([np.inf, -np.inf], 0, inplace=True)
    
    ctr_output = df_pivot.pivot_table('ctr', ['user_id'], 'cadence_group').fillna(0).reset_index()[['user_id', cadence_group]]
    ctr_output.columns = ['user_id', 'group_ctr']
    iap_output = df_pivot.pivot_table('iap', ['user_id'], 'cadence_group').fillna(0).reset_index()[['user_id', cadence_group]]
    iap_output.columns = ['user_id', 'group_iap']

    return ctr_output, iap_output


def merge_pb_iap_stats(pb, cadence_group, output_df):
    ctr_output, iap_output = cvr_by_cadence_group(pb, cadence_group)
    df = pd.merge(output_df, ctr_output, on='user_id', how='left').fillna(0)
    df = pd.merge(df, iap_output, on='user_id', how='left').fillna(0)
    return df
