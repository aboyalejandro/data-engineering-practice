import boto3
import gzip
import os
import botocore
from dotenv import load_dotenv

def main():
    
    load_dotenv('.env')
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')

    session = boto3.Session( aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

    s3 = session.resource('s3')
    BUCKET_NAME = s3.Bucket('common-crawl-practice')
    PATH = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    FILE = 'wet.paths.gz'

    for object in BUCKET_NAME.objects.all():
    
        if FILE in object.key:

            BUCKET_NAME.download_file(object.key, 'wet.paths.gz')
            print(f"Saved {object.key} into local.")

            iteration = 0

            with gzip.open(FILE,'rt') as f:
                for line in f:
                    iteration=+1
                    print(line)
                    if iteration == 1:
                        break

        else:
            pass


if __name__ == "__main__":
    main()
