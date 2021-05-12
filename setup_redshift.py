# Creating Redshift Cluster using the AWS python SDK 
## Infrastructure-as-code

import pandas as pd
import boto3
import json
import configparser
import time

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


## Create clients for EC2, S3, IAM, and Redshift

ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift', \
                       region_name = 'us-west-2', \
                       aws_access_key_id = KEY, \
                       aws_secret_access_key = SECRET \
                       )

print('## Configuring Redshift Cluster')

## STEP 1: IAM ROLE
# Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

try:
    print('1.1 Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path ='/', 
        RoleName = DWH_IAM_ROLE_NAME,
        Description = 'Allows Redshift cluster to call AWS on your behalf',
        AssumeRolePolicyDocument = json.dumps(
        {'Statement': [{ 'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service':'redshift.amazonaws.com'}}],
         'Version': '2012-10-17'})
    )
    
except Exception as e:
    print(e)

# Attach Policy
print('1.2 Attaching Policy')

iam.attach_role_policy(
    RoleName = DWH_IAM_ROLE_NAME,
    PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
)['ResponseMetadata']['HTTPStatusCode']


print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

## STEP 2:  Redshift Cluster

# Create a RedShift Cluster
# For complete arguments to `create_cluster`, see [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster)

# Attach Policy
print('1.4 Creating Redshift Cluster')

try:
    response = redshift.create_cluster(        
        # add parameters for hardware
        ClusterType= DWH_CLUSTER_TYPE,
        NodeType= DWH_NODE_TYPE,
        NumberOfNodes= int(DWH_NUM_NODES),

        # add parameters for identifiers & credentials
        DBName = DWH_DB,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        MasterUsername =DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD,
        
        # add parameter for role (to allow s3 access)
        IamRoles = [roleArn]
    )
    
except Exception as e:
    print(e)
    
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
time.sleep(15)

while myClusterProps['ClusterStatus']=="creating":
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    time.sleep(15)
    
# Take note of the cluster endpoint and role ARN
# Paste it on the dwh.cfg file
    
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

print("## Paste the following information in your dwh.cfg file: \n")
print("Host =", DWH_ENDPOINT)
print("ARN =", DWH_ROLE_ARN)