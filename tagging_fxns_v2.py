
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
