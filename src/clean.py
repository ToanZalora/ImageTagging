import os
import glob

def clean_image(config):
    image_format_file = config['sku_image']['image_format_file']
    paths = image_format_file.split('/')
    image_folder = '/'.join(paths[:-1])
    files = glob.glob(os.path.join(image_folder, '*'))
    for image_file in files:
        os.remove(image_file)


def clean(config):
    clean_image(config)
