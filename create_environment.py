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
    
# Reset dwh.cfg to its original state
try:
    os.remove("dwh.cfg")
except:
    pass
copyfile("dwh_original.cfg", "dwh.cfg")

# Create Environment
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

IAM_ROLE_NAME      = config.get("IAM_ROLE", "IAM_ROLE_NAME")

HOST               = config.get("CLUSTER","HOST")
DB                 = config.get("CLUSTER","DB_NAME")
DB_USER            = config.get("CLUSTER","DB_USER")
DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
PORT               = config.get("CLUSTER","DB_PORT")

LOG_DATA           = config.get("S3","LOG_DATA")
LOG_JSONPATH       = config.get("S3","LOG_JSONPATH")
SONG_DATA          = config.get("S3","SONG_DATA")

CLUSTER_TYPE       = config.get("PARAMS","CLUSTER_TYPE")
NUM_NODES          = config.get("PARAMS","NUM_NODES")
NODE_TYPE          = config.get("PARAMS","NODE_TYPE")
ZONE               = config.get("PARAMS","ZONE")

## STEP 1: Create Environment
### 1.1 Create clients: IAM, S3, REDSHIFT

print("Creating EC2, IAM, S3, REDSHIFT clientes")

ec2 = boto3.resource('ec2',
                       region_name=ZONE,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
                     
s3 = boto3.resource('s3',
                       region_name=ZONE,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )
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

### 1.2 Create empty IAM Role
print("Creating an empty IAM Role")

try:
    empty_Role = iam.create_role(
                        Path='/',
                        RoleName=IAM_ROLE_NAME,
                        Description = "Allows Redshift clusters to call AWS services on your behalf.",
                        AssumeRolePolicyDocument=json.dumps(
                            {'Statement': [{'Action': 'sts:AssumeRole',
                               'Effect': 'Allow',
                               'Principal': {'Service': 'redshift.amazonaws.com'}}],
                             'Version': '2012-10-17'})
    )    

except Exception as e:
    print(e)

### 1.3 Attach policy to IAM role & Save to dwh.cfg

print("Attaching 'S3 Read Only' policy to IAM Role")

iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

### 1.4 Create REDSHIFT Cluster
print(f"""Creating RESHIFT cluster with: {CLUSTER_TYPE} - {NODE_TYPE} - {NUM_NODES} nodes""")

try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=CLUSTER_TYPE,
        NodeType=NODE_TYPE,
        NumberOfNodes=int(NUM_NODES),

        #Identifiers & Credentials
        DBName=DB,
        ClusterIdentifier=HOST,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)
    
print("Time to grab a cup of coffee, this may take a up to five minutes...")

myClusterProps = redshift.describe_clusters(ClusterIdentifier=HOST)['Clusters'][0]

while myClusterProps['ClusterStatus'] != 'available':
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=HOST)['Clusters'][0]
    print(".", end = "")
    time.sleep(3)


print("Cluster is now available")

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])
prettyRedshiftProps(myClusterProps)

### 1.5 OPEN TCP PORT
print("Opening TCP port to enable jupyter connection...")

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(PORT),
        ToPort=int(PORT)
    )
except Exception as e:
    print(e)
    
### 1.6 Save to file ENDPOINT and ANR
with open('dwh.cfg', 'a') as f:
    f.write("\nARN="+myClusterProps['IamRoles'][0]['IamRoleArn'])
    f.write("\nENDPOINT="+myClusterProps['Endpoint']['Address'])
    
print("CAUTION! - Dont forget to manually delete created resources, or execute 'delete_resources.py")