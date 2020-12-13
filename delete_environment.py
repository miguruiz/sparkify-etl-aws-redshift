import configparser
import boto3
import pandas as pd
import time
from botocore.exceptions import ClientError
from shutil import copyfile
import os
import json

# Create Environment
config = configparser.ConfigParser()
try:
    config.read_file(open('credentials.cfg'))

    KEY                = config.get('CREDENTIALS','KEY')
    SECRET             = config.get('CREDENTIALS','SECRET')
except:
    print("Remember to create 'credentials.cfg' with [CREDENTIALS] KEY & SECRET")
    exit()
    
# Create Environment
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

IAM_ROLE_NAME      = config.get("IAM_ROLE", "IAM_ROLE_NAME")
HOST               = config.get("CLUSTER","HOST")
ZONE               = config.get("PARAMS","ZONE")
ARN                = config.get("OTHER","ARN")


iam = boto3.client('iam',
                     region_name=ZONE,
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                  )

redshift = boto3.client('redshift',
                       region_name=ZONE,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

redshift.delete_cluster( ClusterIdentifier=HOST,  SkipFinalClusterSnapshot=True)
iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=IAM_ROLE_NAME)