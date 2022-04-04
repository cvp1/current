import logging
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

my_config = Config(
    region_name = 'us-east-1',
    #signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)
#from aws_session import session

bucket_name = 'cvptest1068'

def get_buckets():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

def create_bucket(bucket_name):
    s3 = boto3.client('s3', config=my_config)
    response = s3.create_bucket(Bucket=bucket_name)
    print('Bucket created: ', bucket_name)
    response_public = s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        },
    )

def get_acl(bucket_name):
    s3 = boto3.client('s3')
    response = s3.get_bucket_acl(Bucket=bucket_name)
    print(response)

def delete_bucket(bucket_name):
    s3 = boto3.client('s3')
    try:
        response = s3.delete_bucket(Bucket=bucket_name)
        print('Deleted bucket: ', bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
    

def upload_file(file_name, bucket, object_name=None):
    s3 = boto3.client('s3')
    if object_name is None:
        object_name = file_name
    try:
        response = s3.upload_file(file_name, bucket, object_name)
        print('Uploaded: ', file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def delete_file(bucket_name, key):
    s3 = boto3.client('s3')
    try:
       s3.delete_object(Bucket=bucket_name, Key=key)
       print('Deleted: ', key)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def download_file(file_name, bucket_name, key):
    s3 = boto3.client('s3')
    with open(file_name, 'wb') as f:
        s3.download_fileobj(bucket_name, key, f)
        print('Downloaded: ', key)

def list_bucket(bucket_name):
    s3 = boto3.client('s3')
    response =  s3.list_objects_v2(Bucket=bucket_name, Delimiter = "/")
    for file in response['Contents']:
        print(file['Key'])

def s3_test():
    get_buckets()
    create_bucket(bucket_name)
    get_buckets()
    upload_file('test.txt', bucket_name, None)
    list_bucket(bucket_name)
    download_file('test.txt', bucket_name, 'test.txt')
    delete_file(bucket_name, 'test.txt')
    delete_bucket(bucket_name)
    get_buckets()


# listb, createb, uploadf, listb, downloadf, deletef, listb, deleteb, listb. 
if __name__ == '__main__':  
    s3_test()
    
    
