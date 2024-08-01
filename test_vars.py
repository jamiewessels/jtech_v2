

def get_test_variables(test_name='20240371NewScriptTest'):
    if test_name == '20240371NewScriptTest':
        package_names = ['com.loop.match3d']
        platforms = ['ios']
        k_clusters_list = [6]
        cohort_names = ['jtechGroup1Test', 'jtechGroup2Test', 'mlGroup1Test']
        cohort_types = ['jtech', 'jtech_b', 'ml']
        iap_exp = 2
        segment = 'segment' # for if we run 2 versions of jtech against each other
        array_cols = ['iap_all', 'max_trxn_amt','iap_trxns_last_30d', 'max_cs_trxn_amt','cs_clicks', 'cs_iap_last_30d'] #for kmeans
        excluded_offers = ['match3d.candlestick.099', 'match3d.candlestick.29999']
    return package_names, platforms, k_clusters_list, cohort_names, cohort_types, iap_exp, segment, array_cols, excluded_offers
