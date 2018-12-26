import yaml
import os
import datetime

def build_file_configuration():

    proj_dir = os.getcwd()
    today = datetime.datetime.now()

    params = {'customed_time': 1,
              'sku': 1,
              'images': 1, 'clean': 1, 
              'tag': 1, 'push': 1}
    time_info = {'start_date': '2018-05-15',
                 'default_end_date': str(today).split(' ')[0],
                 'end_date': None}
    country_info = {'countries': ['sg', 'hk', 'id', 'ph', 'my', 'tw'],
                    'countries_to_run': ['sg', 'hk', 'id', 'ph', 'my', 'tw']}
    sku_info = {'pitbull_key_file': os.path.join(proj_dir, 'keys', 'keys.json'),
                'query_format_file': os.path.join(proj_dir, 'query', 'query.sql'),
                'query_time_range_format_file': os.path.join(proj_dir, 'query', 'query_time_range.sql'),
                'sku_info_time_range_format': os.path.join(proj_dir, 'data', 'country:{}_from:{}_to:{}.csv'),
                'sku_info_format': os.path.join(proj_dir, 'data', 'new_skus_{}.csv')}
    sku_image = {'n_threads': 16,
                 'image_format_file': os.path.join(proj_dir, 'data', 'images', '{}_{}.jpg'),
                 'image_url_format': 'http://zalora-media-live-{}.s3.amazonaws.com/product/{}/{}/{}.jpg',
                 'images_per_sku': 1, 'old_image_folder': os.path.join(proj_dir, 'data', 'old_images')}
    model = {'model_dir': os.path.join(proj_dir, 'models'),
             'model_list': ['texture',
                            'color',
                            'sleeve'],
             'texture_classes': ['floral',
                                 'camouflage',
                                 'polka_dot',
                                 'plain',
                                 'paisley',
                                 'checked',
                                 'animal_print',
                                 'graphic',
                                 'stripe'],
             'sleeve_classes': ['cap',
                                'short',
                                'elbow',
                                'strapless',
                                'sleeveless',
                                'other',
                                'long',
                                'threequarters'],
             'color_classes': ['white',
                               'grey',
                               'gold',
                               'navy',
                               'black',
                               'red',
                               'blue',
                               'silver',
                               'beige',
                               'green',
                               'pink',
                               'yellow',
                               'purple',
                               'orange',
                               'brown']}
    out_file = {'out_file': os.path.join(proj_dir,'output', 'out.csv'),
                'bucket': 'zalora.ds-team',
                'out_file_s3': 'image_tagging_output/out.csv'}

    if time_info['end_date'] is None:
        time_info['end_date'] = time_info['default_end_date']

    configuration = {'parameters': params,
                     'time_info': time_info,
                     'country_info': country_info,
                     'sku_info': sku_info,
                     'sku_image': sku_image,
                     'model': model,
                     'out_file': out_file}

    with open(os.path.join(proj_dir, 'config', 'config_logs.yml'), 'w') as f_out:
        yaml.dump(configuration, f_out)

def load_file_configuration():
    proj_dir = os.getcwd()
    with open(os.path.join(proj_dir, 'config', 'config_logs.yml'), 'r') as f_in:
        configuration = yaml.load(f_in)
    return configuration
