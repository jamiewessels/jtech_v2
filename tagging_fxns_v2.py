
import numpy as np
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import json
from google.cloud import storage
from io import StringIO
from datetime import date
import requests


def tag_users(jwt, file_name, df_output, package_name, platform, illegal_chars = ['.', '-']):
    today = str.replace(str(date.today()),"-","")
    df_output['package_name'] = package_name
    df_output['platform'] = platform.upper()

    #remove illegal chars from package name for naming reqs prior to upload
    for char in illegal_chars:
        package_name = str.replace(package_name, char, "")

    #specify save location
    file_name = f'{str(today)}{file_name}{package_name}{platform}Upld.csv'
    df_output[['user_id', 'installation_id', 'tag', 'package_name', 'platform']].to_csv(file_name, index = False)

    url = 'https://admin.candlestick.com/api/campaigns-v2/admin/file-tasks/create'

    headers = {
        'Authorization': f'Bearer {jwt}',
        'User-Agent': 'PostmanRuntime/7.36.3'
    }

    file_path = file_name

    files = {
        'file': (file_name, open(file_path, 'rb'), '*/*')
    }

    files = {
        'file': (file_name, open(file_path, 'rb'), '*/*'),
        'data': ('data', json.dumps({
            "fileName": file_name,
            "type": "USER_TAGGING_IN_MULTIPLE_APPS"
        }), 'application/json')
    }


    response = requests.post(url, headers=headers, files=files)
    files['file'][1].close()

    print(f'Upload Tags:', response.json())
    return None





def generate_api_inputs_generic(pl, cohort_names, cohort_types, cadence_group='pbo', test_name=None):
    today = str.replace(str(date.today()),"-","")

    cadence_tags = []
    cadence_ids = []
    for cohort_name, cohort_type in zip(cohort_names, cohort_types):

        if cohort_type in ('jtech', 'jtech_b', 'jtech_c'):
        
            cadence_ids_jtech = [int(x) for x in pl[f'{cadence_group}_{cohort_type}']]
            tags_jtech = [str(today) + "_"+ str(cadence_group) + str(x) for x in pl.product_id] 
            cadences_jtech = []
            cadence_tags_dict = {}

            for cadence_id, tag in zip(cadence_ids_jtech, tags_jtech):
                if cadence_id not in cadence_tags_dict:
                    cadence_tags_dict[cadence_id] = []
                if tag not in cadence_tags_dict[cadence_id]: 
                    cadence_tags_dict[cadence_id].append(tag)


            for cadence_id, tag in cadence_tags_dict.items():
                cadences_jtech.append({
                    "cadenceId": int(cadence_id), 
                    "newTags": tag
                    })
            if test_name == None:
                output_data_jtech = {
                    "cadences": cadences_jtech
                    }
            else:
                output_data_jtech = {
                    "cadences": cadences_jtech, 
                    "abTestName": test_name
                    }

            cadence_tags_jtech = json.dumps(output_data_jtech, indent = 2)
            cadence_tags.append(cadence_tags_jtech)
            cadence_ids.append(list(set(cadence_ids_jtech)))
            print(f'cadence tags for {cadence_group}: {cadence_tags_jtech}')

        if cohort_type in ('ml', 'ml_b', 'ml_c'):
        
            cadence_ids_jtech = [int(x) for x in pl[f'{cadence_group}_{cohort_type}']]
            tags_jtech = [str(today) + "_"+ str(cadence_group)] 
            cadences_jtech = []
            cadence_tags_dict = {}

            for cadence_id, tag in zip(cadence_ids_jtech, tags_jtech):
                if cadence_id not in cadence_tags_dict:
                    cadence_tags_dict[cadence_id] = []
                if tag not in cadence_tags_dict[cadence_id]: 
                    cadence_tags_dict[cadence_id].append(tag)


            for cadence_id, tag in cadence_tags_dict.items():
                cadences_jtech.append({
                    "cadenceId": int(cadence_id), 
                    "newTags": tag
                    })
            if test_name == None:
                output_data_jtech = {
                    "cadences": cadences_jtech
                    }
            else:
                output_data_jtech = {
                    "cadences": cadences_jtech, 
                    "abTestName": test_name
                    }

            cadence_tags_jtech = json.dumps(output_data_jtech, indent = 2)
            cadence_tags.append(cadence_tags_jtech)
            cadence_ids.append(list(set(cadence_ids_jtech)))
            print(f'cadence tags for {cadence_group}: {cadence_tags_jtech}')


    return cadence_tags, cadence_ids



#TODO you need to work on a smarter way for this one...
def generate_api_inputs_diff_cadences(pl, cohort_names, cohort_types, cadence_group='pbo', test_name=None): 
    today = str.replace(str(date.today()),"-","")

    cadence_tags = []
    cadence_ids = []
    if cadence_group == 'session15':
        for cohort_name, cohort_type in zip(cohort_names, cohort_types):

            if cohort_type == 'jtech':
            
                cadence_ids_jtech = [int(x) for x in pl[f'{cadence_group}_{cohort_type}']]
                tags_jtech = [str(today) + "_"+ str(cadence_group) + str(x) for x in pl.product_id] 
                cadences_jtech = []
                cadence_tags_dict = {}

                for cadence_id, tag in zip(cadence_ids_jtech, tags_jtech):
                    if cadence_id not in cadence_tags_dict:
                        cadence_tags_dict[cadence_id] = []
                    if tag not in cadence_tags_dict[cadence_id]: 
                        cadence_tags_dict[cadence_id].append(tag)


                for cadence_id, tag in cadence_tags_dict.items():
                    cadences_jtech.append({
                        "cadenceId": int(cadence_id), 
                        "newTags": tag
                        })
                if test_name == None:
                    output_data_jtech = {
                        "cadences": cadences_jtech
                        }
                else:
                    output_data_jtech = {
                        "cadences": cadences_jtech, 
                        "abTestName": test_name
                        }

                cadence_tags_jtech = json.dumps(output_data_jtech, indent = 2)
                cadence_tags.append(cadence_tags_jtech)
                cadence_ids.append(list(set(cadence_ids_jtech)))
                print(f'cadence tags for {cadence_group}: {cadence_tags_jtech}')

            if cohort_type == 'jtech_b':
                
            
                cadence_ids_jtech = [int(x) for x in pl[f'{cadence_group}_{cohort_type}']]
                tags_jtech = [str(today) + "_"+ str(cadence_group) + "_b" + str(x) for x in pl.product_id] 
                cadences_jtech = []
                cadence_tags_dict = {}

                for cadence_id, tag in zip(cadence_ids_jtech, tags_jtech):
                    if cadence_id not in cadence_tags_dict:
                        cadence_tags_dict[cadence_id] = []
                    if tag not in cadence_tags_dict[cadence_id]: 
                        cadence_tags_dict[cadence_id].append(tag)


                for cadence_id, tag in cadence_tags_dict.items():
                    cadences_jtech.append({
                        "cadenceId": int(cadence_id), 
                        "newTags": tag
                        })
                if test_name == None:
                    output_data_jtech = {
                        "cadences": cadences_jtech
                        }
                else:
                    output_data_jtech = {
                        "cadences": cadences_jtech, 
                        "abTestName": test_name
                        }

                cadence_tags_jtech = json.dumps(output_data_jtech, indent = 2)
                cadence_tags.append(cadence_tags_jtech)
                cadence_ids.append(list(set(cadence_ids_jtech)))
                print(f'cadence tags for {cadence_group}: {cadence_tags_jtech}')

            if cohort_type == 'ml':
            
                cadence_ids_jtech = [int(x) for x in pl[f'{cadence_group}_{cohort_type}']]
                tags_jtech = [str(today) + "_"+ str(cadence_group)] 
                cadences_jtech = []
                cadence_tags_dict = {}

                for cadence_id, tag in zip(cadence_ids_jtech, tags_jtech):
                    if cadence_id not in cadence_tags_dict:
                        cadence_tags_dict[cadence_id] = []
                    if tag not in cadence_tags_dict[cadence_id]: 
                        cadence_tags_dict[cadence_id].append(tag)


                for cadence_id, tag in cadence_tags_dict.items():
                    cadences_jtech.append({
                        "cadenceId": int(cadence_id), 
                        "newTags": tag
                        })
                if test_name == None:
                    output_data_jtech = {
                        "cadences": cadences_jtech
                        }
                else:
                    output_data_jtech = {
                        "cadences": cadences_jtech, 
                        "abTestName": test_name
                        }

                cadence_tags_jtech = json.dumps(output_data_jtech, indent = 2)
                cadence_tags.append(cadence_tags_jtech)
                cadence_ids.append(list(set(cadence_ids_jtech)))
                print(f'cadence tags for {cadence_group}: {cadence_tags_jtech}')

            if cohort_type == 'ml_b':
            
                cadence_ids_jtech = [int(x) for x in pl[f'{cadence_group}_{cohort_type}']]
                tags_jtech = [str(today) + "_" + str(cadence_group) + "_b"] 
                cadences_jtech = []
                cadence_tags_dict = {}

                for cadence_id, tag in zip(cadence_ids_jtech, tags_jtech):
                    if cadence_id not in cadence_tags_dict:
                        cadence_tags_dict[cadence_id] = []
                    if tag not in cadence_tags_dict[cadence_id]: 
                        cadence_tags_dict[cadence_id].append(tag)


                for cadence_id, tag in cadence_tags_dict.items():
                    cadences_jtech.append({
                        "cadenceId": int(cadence_id), 
                        "newTags": tag
                        })
                if test_name == None:
                    output_data_jtech = {
                        "cadences": cadences_jtech
                        }
                else:
                    output_data_jtech = {
                        "cadences": cadences_jtech, 
                        "abTestName": test_name
                        }

                cadence_tags_jtech = json.dumps(output_data_jtech, indent = 2)
                cadence_tags.append(cadence_tags_jtech)
                cadence_ids.append(list(set(cadence_ids_jtech)))
                print(f'cadence tags for {cadence_group}: {cadence_tags_jtech}')

    else: 
        for cohort_name, cohort_type in zip(cohort_names, cohort_types):

            if cohort_type in ('jtech', 'jtech_b', 'jtech_c'):
            
                cadence_ids_jtech = [int(x) for x in pl[f'{cadence_group}_{cohort_type}']]
                tags_jtech = [str(today) + "_"+ str(cadence_group) + str(x) for x in pl.product_id] 
                cadences_jtech = []
                cadence_tags_dict = {}

                for cadence_id, tag in zip(cadence_ids_jtech, tags_jtech):
                    if cadence_id not in cadence_tags_dict:
                        cadence_tags_dict[cadence_id] = []
                    if tag not in cadence_tags_dict[cadence_id]: 
                        cadence_tags_dict[cadence_id].append(tag)


                for cadence_id, tag in cadence_tags_dict.items():
                    cadences_jtech.append({
                        "cadenceId": int(cadence_id), 
                        "newTags": tag
                        })
                if test_name == None:
                    output_data_jtech = {
                        "cadences": cadences_jtech
                        }
                else:
                    output_data_jtech = {
                        "cadences": cadences_jtech, 
                        "abTestName": test_name
                        }

                cadence_tags_jtech = json.dumps(output_data_jtech, indent = 2)
                cadence_tags.append(cadence_tags_jtech)
                cadence_ids.append(list(set(cadence_ids_jtech)))
                print(f'cadence tags for {cadence_group}: {cadence_tags_jtech}')

            if cohort_type in ('ml', 'ml_b', 'ml_c'):
            
                cadence_ids_jtech = [int(x) for x in pl[f'{cadence_group}_{cohort_type}']]
                tags_jtech = [str(today) + "_"+ str(cadence_group)] 
                cadences_jtech = []
                cadence_tags_dict = {}

                for cadence_id, tag in zip(cadence_ids_jtech, tags_jtech):
                    if cadence_id not in cadence_tags_dict:
                        cadence_tags_dict[cadence_id] = []
                    if tag not in cadence_tags_dict[cadence_id]: 
                        cadence_tags_dict[cadence_id].append(tag)


                for cadence_id, tag in cadence_tags_dict.items():
                    cadences_jtech.append({
                        "cadenceId": int(cadence_id), 
                        "newTags": tag
                        })
                if test_name == None:
                    output_data_jtech = {
                        "cadences": cadences_jtech
                        }
                else:
                    output_data_jtech = {
                        "cadences": cadences_jtech, 
                        "abTestName": test_name
                        }

                cadence_tags_jtech = json.dumps(output_data_jtech, indent = 2)
                cadence_tags.append(cadence_tags_jtech)
                cadence_ids.append(list(set(cadence_ids_jtech)))
                print(f'cadence tags for {cadence_group}: {cadence_tags_jtech}')


    return cadence_tags, cadence_ids

