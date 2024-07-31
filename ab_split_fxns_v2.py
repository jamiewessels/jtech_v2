import numpy as np
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from datetime import date
from read_in_data_v2 import * 
from jtech_fxns_v2 import *
from tagging_fxns_v2 import * 

def fetch_existing_ab_cohorts(df_jtech, package_name, platform, bucket, cohort_names, clean_cols = None, illegal_chars=['.', '-'], test_name=None, segment='segment', jwt = ''):

    try: 
        ab_lookup = read_in_single_csv(f'ab_lookups/AbLookup{test_name}{package_name}{platform}', package_name, platform, bucket, illegal_chars=['.', '-']) #try to pull existing file from GCB
        ab_lookup = ab_lookup.drop_duplicates(subset=['user_id', 'ab_cohort']).reset_index()

    except:
        ab_lookup = determine_ab_group(df_jtech, cohort_names, segment)
        initial_ab_upload = ab_lookup[['user_id', 'ab_cohort']].copy().drop_duplicates()
        initial_ab_upload.columns = ['user_id', 'tag']
        # tag_users(jwt, f'{testname}InitialABUpload', initial_ab_upload, package_name, platform) #tag users with their A/B cohort
   
    return ab_lookup


def determine_ab_group(df, cohort_names, segment):
    num_cohorts = len(cohort_names)
    df['ab_cohort'] = ''

    for s in df[segment].unique():
        # filter users in the current segment
        segment_indices = df[df[segment] == s].index
        
        # random shuffle the segment users
        shuffled_indices = np.random.permutation(segment_indices)
        
        # calculate the size of each cohort
        cohort_size = len(shuffled_indices) // num_cohorts
        remainder = len(shuffled_indices) % num_cohorts
        
        start_idx = 0
        
        for i in range(num_cohorts):
            end_idx = start_idx + cohort_size + (1 if i < remainder else 0)
            df.loc[shuffled_indices[start_idx:end_idx], 'ab_cohort'] = cohort_names[i]
            start_idx = end_idx

    return df


# output_df, new_cohorts_df = get_tags_from_ab_splits(df_jtech, ab_lookup, matches, package_name, platform, storage_client, bucket_name=bucket, fxn=get_tags_sessions_split_v2, cohort_names = cohort_names, cohort_types = cohort_types, num_cohorts=num_cohorts, testname=testname, segment=segment, cadence_type=cadence_type)

def get_tags_from_ab_splits(df_jtech, ab_lookup, package_name, platform, storage_client, bucket_name, fxn, cohort_names = None, cohort_types = None, test_name=None, segment='segment', cadence_type=''): 
    today = str.replace(str(date.today()),"-","")
    df_jtech['ab_cohort'] = df_jtech['user_id'].map(ab_lookup.set_index('user_id')['ab_cohort']) #map AB cohort to each user

    df_no_cohort, new_cohorts = assign_no_cohorts(df_jtech, storage_client, bucket_name, package_name, platform, cohort_names,  segment, test_name, cadence_type)

    
    #filter to known AB cohorts
    df_cohort = df_jtech[~df_jtech.ab_cohort.isnull()].copy()
    
    #restack the dfs
    df_merged_out = pd.concat([df_cohort, df_no_cohort], axis=0) #merge known cohorts and new cohorts
    df_merged_out['dayofweek'] = date.today().weekday()

    #assign offers to send and tag up:
    df_merged_out['tag'] = df_merged_out.apply(lambda x: fxn(x, cadence_type, cohort_names, cohort_types), axis = 1) # based on A or B assign the jtech offer to send or the old logic
    
    #save new ab lookups
    ab_lookup_replacement = df_merged_out[['user_id', 'ab_cohort']]
    # upload_to_gcb(ab_lookup_replacement, storage_client, bucket_name, package_name, platform, f'{str(today)}{test_name}DailySavedCohorts', folder='ab_lookups_historical') #TODO UNCOMMENT
    # upload_to_gcb(ab_lookup_replacement, storage_client, bucket_name, package_name, platform, f'AbLookup{test_name}', folder='ab_lookups') #TODO UNCOMMENT

    #upload new tags
    # upload_to_gcb(df_merged_out, storage_client, bucket_name, package_name, platform, f'{str(today)}{test_name}{cadence_type}Tag', folder='saved_tags') #TODO UNCOMMENT
    print(f'User ID Count: {df_merged_out.user_id.nunique()}')
                                                                                    
    return df_merged_out, new_cohorts


def assign_no_cohorts(df_jtech, storage_client, bucket_name, package_name, platform, cohort_names, segment, test_name, cadence_type=None):
    today = str.replace(str(date.today()),"-","")
    df_no_cohort = df_jtech[df_jtech.ab_cohort.isnull()].copy() #figure out which users have not been assigned an AB cohort yet
    no_cohort_assignments = determine_ab_group(df_no_cohort, cohort_names, segment) 
    df_no_cohort['ab_cohort'] = df_no_cohort['user_id'].map(no_cohort_assignments.set_index('user_id')['ab_cohort']) #assign them to A or B

    new_cohorts = df_no_cohort[['user_id', 'ab_cohort']].copy().drop_duplicates()
    new_cohorts.columns = ['user_id', 'tag']
    
    # upload_to_gcb(new_cohorts, storage_client, bucket_name, package_name, platform, f'{str(today)}NewCohorts{test_name}{cadence_type}', folder='saved_tags') #TODO UNCOMMENT

    return df_no_cohort, new_cohorts


def tag_assignment_frequency_v0(row, today, cadence_type, extra_tag=''):
    if row['cs_click_freq_10d'] >= 0.5 or (row['cs_click_freq_10d'] >= 0.33 and row['cs_iap_last_7d'] >= 40): #send every day
        return str(today) + "_" + str(cadence_type) + str(extra_tag) + str(row[f'offer_to_send'])
    
    elif row['cs_sent'] > 20 and row['sent_to_click'] < 0.05:
        return str(today) + '_no_offer'

    elif row['dayofweek'] in [1, 3, 4, 5, 6]: 
        return str(today) + "_" + str(cadence_type) + str(extra_tag) + str(row[f'offer_to_send'])

    else:
        return str(today) + '_no_offer'


def tag_assignment_generic(row, today, cadence_type, extra_tag = ''):
    if row['cs_sent'] > 20 and row['sent_to_click'] < 0.03:
        return str(today) + '_no_offer'
    else: 
        return str(today) + "_" + str(cadence_type)  + str(extra_tag) + str(row[f'offer_to_send'])

def tag_assignment_single_cadence(today, cadence_type, extra_tag = ''):

    return str(today) + "_" + str(cadence_type) + str(extra_tag)




def get_tags_cadence_test(row, cadence_type, cohort_names, cohort_types):
    today = str.replace(str(date.today()),"-","")

    try: jtech_idx = cohort_types.index('jtech') 
    except: None
    
    try: ml_idx = cohort_types.index('ml') 
    except: None

    try: jtech_b_idx = cohort_types.index('jtech_b')
    except: None 

    try: ml_b_idx = cohort_types.index('ml_b')
    except: None
    
    ###jtech A###
    if 'jtech' in cohort_types:
        if row['ab_cohort'] == cohort_names[jtech_idx]: 
            label = cohort_names[jtech_idx]

            #session
            if cadence_type == 'session15': 
                return tag_assignment_frequency_v0(row, today, cadence_type)

            #pbo, lcc, alliance, episode, level_up
            else: 
                return tag_assignment_generic(row, today, cadence_type)

    ###jtech B###
    if 'jtech_b' in cohort_types:
        if row['ab_cohort'] == cohort_names[jtech_b_idx]: 
            label = cohort_names[jtech_b_idx]

            #session
            if cadence_type == 'session15': 
                return tag_assignment_frequency_v0(row, today, cadence_type, extra_tag = '_b') #if testing a diff session cadence for "b" groups

            #pbo, lcc, alliance, episode, level_up
            else: 
                return tag_assignment_generic(row, today, cadence_type)


     ###ml_purchase A### 
    if 'ml' in cohort_types:
        if row['ab_cohort'] == cohort_names[ml_idx]: 
            return tag_assignment_single_cadence(today, cadence_type)

    ###ml_purchase B###
    if 'ml_b' in cohort_types:
        if row['ab_cohort'] == cohort_names[ml_b_idx]: 
            if cadence_type == 'session15': 
                return tag_assignment_single_cadence(today, cadence_type, extra_tag = '_b')
            else: 
                return tag_assignment_single_cadence(today, cadence_type)
        
    else:
        return str(today) + "_" + str(cadence_type) + "_something_went_wrong"
    
