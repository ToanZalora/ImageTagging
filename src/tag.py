import os
import numpy as np
import torch
import pandas as pd
from fastai import (
    error_rate,
    DatasetType)
from fastai.vision import (
    create_cnn,
    get_transforms,
    imagenet_stats,
    models,
    ImageItemList,
    ImageDataBunch)
from fastai.vision.data import (
    LabelList,
    CategoryList,
    DeviceDataLoader,
    DataLoader)


def _get_predictions(config, image_list, labels):
    predictions = []
    names = []
    for model_name in config['model']['model_list']:
        names += [model_name+'_1', model_name+'_2']
        classes = config['model']['{}_classes'.format(model_name)]
        model_dir = os.path.join(config['model']['model_dir'], model_name)
        data = ImageDataBunch.single_from_classes('',
                                                  classes,
                                                  tfms=get_transforms(),
                                                  size=224).normalize(imagenet_stats)
        learn = create_cnn(data, models.resnet50, metrics=error_rate)
        learn.load(model_dir)

        test_lbl = LabelList(x=image_list,
                             y=CategoryList(labels, classes=classes),
                             tfms=get_transforms()[1],
                             size=224)
        test_dl = DeviceDataLoader(DataLoader(test_lbl, batch_size=64), tfms = learn.data.valid_dl.tfms,device=torch.device('cuda'))
        learn.data.test_dl = test_dl
        probabilities, _ = learn.get_preds(ds_type=DatasetType.Test)
        ranking = np.asarray(probabilities.argsort()[:, -2:])
        current_predictions = [(classes[first], classes[second]) for second, first in ranking]
        predictions.append(current_predictions)
    return predictions, names

def _get_info(image_list):

    def _info(full_name):
        name = str(full_name).split('/')[-1]
        country_sku = name.split('.')[0]
        country, sku = country_sku.split('_')
        return [country, sku]

    return [_info(full_name) for full_name in enumerate(image_list.items)]

def tag_images(config, exclusion = False):
    print('TAGGING SKU IMAGES')

    if os.path.isfile(config['out_file']['out_file']):
        old_results = pd.read_csv(config['out_file']['out_file'])
    else:
        old_results = None

    # tagging new skus
    img_dir = '/'.join((config['sku_image']['image_format_file'].split('/')[:-1]))
    image_list = ImageItemList.from_folder(img_dir)
    if len(image_list) <= 1:
        return
    labels = [0 for _ in range(len(image_list))]
    info = _get_info(image_list)
    predictions, names = _get_predictions(config, image_list, labels)

    data = []
    for i in zip(info, *predictions):
        temp = []
        for j in i:
            temp += j
        data.append(temp)
    data = pd.DataFrame(data, columns=['country', 'sku']+names)
    if old_results is not None:
        final = pd.concat([old_results, data])
        final = final.reset_index(drop=True)
    else:
        final = data
    final.to_csv(config['out_file']['out_file'], index=False)
