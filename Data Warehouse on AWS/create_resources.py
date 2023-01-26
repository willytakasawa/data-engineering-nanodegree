import boto3
import configparser
import logging
import json

from botocore.exceptions import ClientError


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARS
DWH_IAM_ROLE_NAME = ''
DWH_CLUSTER_TYPE = ''
DWH_NODE_TYPE = ''
DWH_NUM_NODES = ''
DWH_DB = ''
DWH_CLUSTER_IDENTIFIER = ''
DWH_DB_USER = ''
DWH_DB_PASSWORD = ''



def create_iam_role():
    # Define funcition to create IAM role
    iam = boto3.client('iam', aws_access_key_id='KEY', aws_secret_access_key='SECRET', region_name='us-west-2')
    try:
        print('1.1 Creating a new IAM Role')
        dwh_role = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Allows Redshift clusters to call AWS services',
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [
                        {
                            'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}
                        }
                    ],
                    'Version': '2012-10-17'
                }
            )
        )
    except Exception as e:
        print(e)

    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    return role_arn


# Define Attaching Policy

# Get the IAM role ARN


def rds_create_cluster(role_arn):
    # Create a RedShift Cluster
    redshift = boto3.client(
        'redshift',
        aws_access_key_id='KEY',
        aws_secret_access_key='SECRET',
        region_name='us-west-2'
    )
    try:
        redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[role_arn]
        )
        logging.debug('Creating RedShift Cluster')
    except ClientError as e:
        logging.exception(e)


def main():
    role_arn = create_iam_role()
    rds_create_cluster(role_arn)


if __name__ == '__main__':
    main()




