import boto3


def push(config):
    print("PUSHING RESULTS")
    s3 = boto3.resource('s3')
    a = s3.meta.client.upload_file(config['out_file']['out_file'],
                               config['out_file']['bucket'],
                               config['out_file']['out_file_s3'])
