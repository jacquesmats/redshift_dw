# Cleaning up Redshift resources 
## Infrastructure-as-code

import boto3
import configparser
import time

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

redshift = boto3.client('redshift', \
                       region_name = 'us-west-2', \
                       aws_access_key_id = KEY, \
                       aws_secret_access_key = SECRET \
                       )

# Delete the created resources
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
time.sleep(15)

print("## Deleting Redshift Cluster...")

while myClusterProps['ClusterStatus']=="deleting":
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    time.sleep(15)

iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

print("## All resources cleaned")