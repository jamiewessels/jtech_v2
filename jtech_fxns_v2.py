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
from scipy.spatial import distance
from read_in_data_v2 import *

def jtech(pb, iap, pm, k_clusters, array_cols, click_wt=-0.2, msgs_wt=-0.1, iap_exp=2):

    #jtech: get the clusters for jtech
    pb_grouped, array_cspayers, cluster_labels_cspayers, cs_payers_clusters_grouped, cs_payers_clusters_merged, all_users_clustered = jtech_transform_dfs_sim(pb, iap, pm, k_clusters, array_cols)

    #jtech: create arrays for getting jtech scores of each cluster/segment. then get scores for each cluster
    event_names, jtech_segments, offers, prices, jtech_array = jtech_create_arrays_cspayers(cs_payers_clusters_grouped, pm)
    scores = jtech_get_scores(click_wt, msgs_wt, iap_wt = (np.array(prices)**iap_exp), array=jtech_array, offers=offers)

    # jtech: use scores and weighted probability of the score to assign an offer for each user
    df_jtech, df_agg_offers, df_agg_price= jtech_assign_offers_df(scores, all_users_clustered, offers, jtech_segments, pm)

    df_jtech = pd.merge(df_jtech, iap[['user_id', 'cs_click_freq_10d', 'cs_iap_last_7d', 'sent_to_click', 'cs_sent']], on='user_id', how='left').fillna(0)

    return df_jtech, df_agg_offers, df_agg_price, jtech_segments, offers, scores, jtech_array, array_cspayers


def jtech_transform_dfs_sim(pb, iap, pm, k_clusters, array_cols):
    
    #map the offer to the price
    pb['price'] = pb['offer'].map(pm.set_index('offer')['price']) 
    #remove n/as for price
    pb = pb[pd.notna(pb.price)].copy()
    #group by msg/click/iap event for each offer id (note: we're treating all cadence groups the same here)
    pb_grouped = pb[(pb.event_name == 'sent')| (pb.event_name == 'iap')|(pb.event_name == 'click')].groupby(by=['user_id', 'offer', 'price', 'event_name']).sum().reset_index() 

    #cluster users who have purchased CS
    iap.cs_iap = iap.cs_iap.astype(float)
    segment_sub_payers = iap[iap.cs_iap >0].copy().fillna(0)
    array_cspayers, cluster_labels_cspayers = assign_clusters_kmeans(segment_sub_payers, array_cols, k_clusters, 'cspayers')
    
    #for non-CS payers find the most similar CS payers to determine segment
    all_users_clustered = get_segments_all_users(iap, cluster_labels_cspayers)
    
    #merge CS payer clusters with the original pb_grouped table to get a count for sent, click, and iap for each offer and each "segment"
    cs_payers_clusters_merged = pd.merge(cluster_labels_cspayers, pb_grouped, how = 'left', left_on='user_id'
                , right_on = 'user_id')[['user_id', 'offer', 'price','event_name', 'cadence_stats', 'segment_cspayers']].reset_index().copy().drop(columns=['index']).dropna()

    cs_payers_clusters_grouped = cs_payers_clusters_merged.groupby(by=['segment_cspayers', 'event_name', 'offer', 'price']).sum().reset_index()[['segment_cspayers', 'event_name', 'offer', 'price','cadence_stats']]

    return pb_grouped, array_cspayers, cluster_labels_cspayers, cs_payers_clusters_grouped, cs_payers_clusters_merged, all_users_clustered


def jtech_create_arrays_cspayers(cs_payers_clusters_grouped, pm):
    
    event_names = np.sort(np.unique(cs_payers_clusters_grouped['event_name']))  #save event names (example: "click")
    segments = np.sort(np.unique(cs_payers_clusters_grouped['segment_cspayers']))       # save cluster names
    iix = pd.MultiIndex.from_product([event_names, segments]) #create multi index
    
    
    cs_payers_clusters_pivot = cs_payers_clusters_grouped.pivot_table('cadence_stats', ['event_name', 'segment_cspayers'], 'offer', aggfunc='first').reindex(iix).fillna(0)

    offers = cs_payers_clusters_pivot.columns
    prices = pm.set_index('offer').loc[offers, 'price'].values
    
    #array shape: (events, num_clusters, num_offers)
    array = cs_payers_clusters_pivot.to_numpy().reshape(len(event_names),len(segments),-1)
    
    return event_names, segments, offers, prices, array

def assign_clusters_kmeans(segment_sub, array_cols, k_clusters, label):
    segment_labels = np.array(segment_sub['user_id'])
    array = np.array(segment_sub[array_cols])
    km = KMeans(k_clusters)
    scaler = StandardScaler()
    scaled_array = scaler.fit_transform(array)
    y=km.fit_predict(scaled_array)
    cluster_labels = pd.DataFrame({'user_id': segment_labels, f'segment_{label}': y}, columns=['user_id', f'segment_{label}'])
    cluster_labels[f'segment_{label}'] = cluster_labels[f'segment_{label}'].astype(str)
    return array, cluster_labels

def get_segments_all_users(iap, cluster_labels_cspayers):
    scaler = StandardScaler()
    iap = iap.fillna(0).copy()
    non_cs_cols = ['user_id', 'iap_all', 'max_trxn_amt', 'iap_trxns_last_30d', 'iap_usd_last_30d']

    non_cs_attributes_payer = iap[iap.cs_iap > 0][non_cs_cols].copy().set_index('user_id')
    payer_labels = non_cs_attributes_payer.index
    payer_array = non_cs_attributes_payer.to_numpy()
    payer_array = scaler.fit_transform(payer_array)
    
    non_cs_attributes_nonpayer = iap[iap.cs_iap == 0][non_cs_cols].set_index('user_id')
    nonpayer_labels = non_cs_attributes_nonpayer.index
    nonpayer_array = non_cs_attributes_nonpayer.to_numpy()
    nonpayer_array = scaler.transform(nonpayer_array)
    
    most_similar_idxs = get_closest_dist_idxs(payer_array, nonpayer_array)
    most_sim_payers =pd.DataFrame(payer_labels[most_similar_idxs])
    most_sim_payers['segment'] = most_sim_payers['user_id'].map(cluster_labels_cspayers.set_index('user_id')['segment_cspayers'])

    most_sim_segments = most_sim_payers['segment'].to_numpy()
    cluster_labels_noncspayers = pd.DataFrame(index=nonpayer_labels, data = most_sim_segments, columns = ['segment_cspayers']).reset_index()

    users_clustered = pd.concat([cluster_labels_cspayers, cluster_labels_noncspayers], axis = 0, ignore_index=True)
    users_clustered.columns = ['user_id', 'segment']
    return users_clustered

def get_closest_dist_idxs(array1, array2):

    distance_matrix = np.linalg.norm(array2[:, np.newaxis] - array1, axis=2)
    most_similar_idxs = np.argmin(distance_matrix, axis=1)
    return most_similar_idxs

def jtech_get_scores(click_wt, msgs_wt, iap_wt, array, offers):
    clicks = array[0] * click_wt
    msgs_sent = array[2] * msgs_wt
    iap_success = array[1] * iap_wt
    output = clicks+msgs_sent+iap_success
    return output #these are the scores for each cluster (shape is cluster, offer, and values are the score)

def jtech_assign_offers_df(scores, users_clustered, offers, jtech_segments, pm):
    #method to assign a "weight" for each score (subject to change). right now it's just delta from min score across offers within each cluster
    delta_from_min = scores - np.min(scores, axis=1).reshape(-1,1)
    delta_from_min_sum_reshaped = delta_from_min.sum(axis=1)[:, np.newaxis]
    weights_normalized = delta_from_min / delta_from_min_sum_reshaped #these are the weighted probabilities for each offer within each cluster
    
    df = users_clustered[['user_id', 'segment']].copy()
    df['offer_to_send'] = df['segment'].apply(lambda x: jtech_choose_offer_weighted_prob(x, offers, weights_normalized, jtech_segments, scores)) #for each user, randomly assign offer based on weighted probability for that price within that cluster
    df['price'] = df['offer_to_send'].map(pm.set_index('offer')['price']) 
    df_agg_offer = df.groupby(by=['segment', 'offer_to_send']).count().reset_index() 
    df_agg_price = df.groupby(by=['segment', 'price']).count().reset_index() 

    return df, df_agg_offer, df_agg_price

def jtech_choose_offer_weighted_prob(segment, offers, weights_normalized, jtech_segments, scores):
    # this is the row-level function for determining offer based on weighted probability for that price within the cluster/segment
    try:
        idx = np.where(jtech_segments == segment)[0][0]
    except:
        idx = 0
    return offers[np.where(scores[idx,:] == np.random.choice(scores[idx, :], p=weights_normalized[idx, :]))][0]


#elbow plot for viz
def elbow_plot(array):
    #run this to figure out best # of clusters
    model = KMeans()
    scaler = StandardScaler()
    scaled_array = scaler.fit_transform(array)
    visualizer = KElbowVisualizer(model, k=(2,14), timings= True)
    visualizer.fit(scaled_array)        # Fit data to visualizer
    visualizer.show()        # Finalize and render figure