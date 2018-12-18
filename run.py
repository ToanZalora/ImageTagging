import sys
from query.query_sku_data import download_sku_info, download_sku_images
from config.config import load_file_configuration, build_file_configuration
from src.tag import tag_images
from src.clean import clean
from src.push import push

if __name__ == '__main__':

    # get configuration
    build_file_configuration()
    config = load_file_configuration()
    args = config['parameters']

    # download sku information
    if args['sku'] == 1:
        download_sku_info(config)

    # download sku images
    if args['images'] == 1:
        download_sku_images(config)

    # tag the downloaded images
    if args['tag'] == 1:
        tag_images(config)

    # push to s3
    if args['push'] == 1:
        push(config)
    # clean image
    if args['clean'] == 1:
        clean(config)
