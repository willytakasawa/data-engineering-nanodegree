import boto3
import configparser
import logging
import json
import time

from botocore.exceptions import ClientError


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARS
KEY = ''
SECRET = ''
DWH_IAM_ROLE_NAME = ''
DWH_CLUSTER_TYPE = ''
DWH_NODE_TYPE = ''
DWH_NUM_NODES = ''
DWH_DB = ''
DWH_CLUSTER_IDENTIFIER = ''
DWH_DB_USER = ''
DWH_DB_PASSWORD = ''
DWH_PORT = ''


def create_aws_resources():
    # Create all AWS resources
    iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
    redshift = boto3.client('redshift', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
    ec2 = boto3.resource('ec2', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
    s3 = boto3.resource('s3', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
    logging.debug("Create AWS Resources.")
    return iam, redshift, ec2, s3


def create_iam_role(iam):
    # Define funcition to create IAM role
    try:
        logging.debug('Creating a new IAM Role')
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

    # Define policy to access S3 bucket (ReadOnly)
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

    # Get the IAM role ARN
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logging.debug('IAM: {}, ARN: {} created'.format(DWH_IAM_ROLE_NAME, role_arn))
    return role_arn


def rds_create_cluster(redshift, role_arn):
    # Create a RedShift Cluster
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


def create_tcp(ec2, vpc_id):
    # Open an incoming TCP port to access the cluster endpoint
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.secutiryt_groups.all())[0]
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
        logging.debug('Opening TCP connection.')

    except ClientError as e:
        logging.exception(e)


def main():
    iam, redshift, ec2, s3 = create_aws_resources()
    role_arn = create_iam_role(iam)
    rds_create_cluster(redshift, role_arn)

    for x in range(int(60)):
        cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        if cluster_props['ClusterStatus'] == 'available':
            logging.debug('Redshift Cluster is available and created at {}'.format(cluster_props['Endpoint']))
            create_tcp(ec2, cluster_props['VpcId'])
            break
        logging.debug('Cluster status: {}. Retrying...'.format(cluster_props['ClusterStatus']))
        time.sleep(5)


if __name__ == '__main__':
    main()




