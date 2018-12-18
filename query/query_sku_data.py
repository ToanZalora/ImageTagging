import sys
import os
import psycopg2
import json
import pandas as pd
import threading
import pandas.io.sql as sqlio
import urllib.request
import boto3


def download_sku_info(config):

    print('DOWNLOADING SKU INFO')

    s3 = boto3.resource('s3')
    s3.meta.client.download_file(config['out_file']['bucket'],
                                 config['out_file']['out_file_s3'],
                                 config['out_file']['out_file'])

    if os.path.isfile(config['out_file']['out_file']):
        old_results = pd.read_csv(config['out_file']['out_file'])
    else:
        old_results = None

    # connect to pitbull
    with open(config['sku_info']['pitbull_key_file'], 'r') as f_in:
        pitbull_credential = json.load(f_in)
    host, schema, userid, password, port = pitbull_credential
    pitbull_connection = psycopg2.connect(host=host,
                                          database=schema,
                                          user=userid,
                                          password=password,
                                          port=port)

    # query format
    if config['parameters']['customed_time'] == 1:
        with open(config['sku_info']['query_time_range_format_file'], 'r') as f_in:
            query_format = f_in.read()
            args = [config['time_info']['start_date'], config['time_info']['end_date']]
    else:
        with open(config['sku_info']['query_format_file'], 'r') as f_in:
            query_format = f_in.read()
            args = []

    # download data
    for country in config['country_info']['countries_to_run']:
        query = query_format.format(country, *args)
        data = sqlio.read_sql_query(query,
                                    pitbull_connection)
        if old_results is not None:
            tagged_skus = list(set(old_results[old_results['country'] == country]['sku']))
        else:
            tagged_skus = []
        data = data[~data['sku'].isin(tagged_skus)]
        print('\tDownloaded {} SKUs of {}'.format(len(data), country))
        data.to_csv(config['sku_info']['sku_info_format'].format(country))

def _download_sku_image(config, row):

    def _download_image_from(image_url, image_local_file):
        try:
            urllib.request.urlretrieve(image_url, image_local_file)
        except:
            return False
        return True
        
    sku = row['sku']
    id_catalog_config = row['id_catalog_config']
    country = row['venture_code']
    url_subpath = str(id_catalog_config)[::-1]
    url_subpath_1 = url_subpath[:2]
    url_subpath_2 = url_subpath[2:]
    for i in range(1, config['sku_image']['images_per_sku']+1):
        s3_image_url = config['sku_image']['image_url_format'].format(country,
                                                                   url_subpath_1,
                                                                   url_subpath_2,
                                                                   i)
        ak_image_url = row['image_url']
        image_local_file = config['sku_image']['image_format_file'].format(country,
                                                                           sku,
                                                                           id_catalog_config)
        if not _download_image_from(ak_image_url, image_local_file):
            _download_image_from(s3_image_url, image_local_file)


def _thread_download(config, counts, thread_index, chunk):
    for i, row in chunk.iterrows():
        counts[thread_index] += 1
        _download_sku_image(config, row)
        num_len = len(str(counts[-1]))
        """
        str_format = '\t' + ' - '.join(['{}/{}' for _ in range(config['sku_image']['n_threads'])]) + '\r'
        sys.stdout.write(str_format.format(*[str(count).zfill(5) for count in counts]))
        """
        str_format = '\t{}/{}\r'
        sys.stdout.write(str_format.format(str(sum(counts[:-1])).zfill(num_len),
                                           str(counts[-1]).zfill(num_len)))
        sys.stdout.flush()


def _split_data(data, n_threads):
    data_chunks = []
    chunk_size = int(len(data)/n_threads)
    start_points = [i*chunk_size for i in range(n_threads)] + [len(data)]
    for index in range(n_threads):
        data_chunks.append(data.iloc[start_points[index]:start_points[index+1]].reset_index(drop=True))
    return data_chunks


def download_sku_images(config, exclusion = False):
    print('DOWNLOADING SKU IMAGES')

    # for each country
    for country in config['country_info']['countries_to_run']:

        # get data info
        data_info_file = config['sku_info']['sku_info_format'].format(country)
        data = pd.read_csv(data_info_file)

        # split data into many chunks, each will be downloaded by a thread
        data_chunks = _split_data(data, config['sku_image']['n_threads'])

        # download images
        threads = []
        counts = []
        for _ in data_chunks:
            counts.append(0)
        counts.append(len(data))

        for thread_index, chunk in enumerate(data_chunks):
            thread = threading.Thread(target=_thread_download,
                                      args=(config,counts, thread_index, chunk))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        num_len = len(str(counts[-1]))
        str_format = '\t{}/{}\n'
        sys.stdout.write(str_format.format(str(sum(counts[:-1])).zfill(num_len),
                                           str(counts[-1]).zfill(num_len)))

