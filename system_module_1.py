import json
import os
import re
import time

import pandas as pd
import requests

FILE_REGEX = re.compile(".csv$")
PATH_INI = './DATA_INI'

headers_post = {"Content-Type": "application/json"}


# noinspection PyBroadException
def run_system_module_1():
    # Lista de ficheros para cargar
    list_file_update_table_films = list(filter(FILE_REGEX.search, os.listdir(PATH_INI)))
    print('\nFile/s to process {0}\n'.format(len(list_file_update_table_films)))

    for each_file in list_file_update_table_films:
        start_time = time.time()
        print('+ Start ETL process - File {0}'.format(each_file))
        path_file = os.path.join(PATH_INI, each_file)

        # ETL Process...
        # Extract
        each_info = pd.read_csv(path_file)

        # Transform
        each_info = each_info[each_info['show_id'].notna()]
        each_info['release_year'] = each_info['release_year'].astype(int)
        each_info['release_year'] = each_info['release_year'].astype(str)
        each_info['info_ingesta'] = None
        each_info = each_info.replace({None: '-'})

        # Load
        total_read = each_info.shape[0]
        print('\t- Total items read: {0}'.format(total_read))

        for index_row, row in each_info.iterrows():
            url = 'http://project-api-engine-01.ue.r.appspot.com/insert_data'
            data_temp = {"show_id": row['show_id'],
                         "type": row['type'],
                         "title": row['title'],
                         "director": row['director'],
                         "cast": row['cast'],
                         "country": row['country'],
                         "release_year": row['release_year'],
                         "rating": row['rating'],
                         "duration": row['duration'],
                         "listed_in": row['listed_in'],
                         "description": row['description']
                         }

            response_api = requests.post(url, json=data_temp, headers=headers_post)
            my_response = json.loads(response_api.text)

            if my_response['_return'] == 'OK - 200':
                each_info['info_ingesta'] = 'OK'
            elif my_response['_return'] == 'KO - 202':
                each_info['info_ingesta'] = 'DUPLICATE'
            else:
                each_info['info_ingesta'] = 'KO'

        print('\t- Insert OK: {0}'.format(each_info[each_info['info_ingesta'] == 'OK'].shape[0]))
        print('\t- No insert by DUPLICATE: {0}'.format(
            each_info[each_info['info_ingesta'] == 'DUPLICATE'].shape[0]))
        print('\t- No insert by UNEXPECTED: {0}'.format(
            each_info[each_info['info_ingesta'] == 'KO'].shape[0]))
        print('+ End ETL process. Runtime {0} seconds\n'.format(str(round(time.time() - start_time, 3))))

        # Delete file
        # os.remove(path_file)


if __name__ == "__main__":
    run_system_module_1()
