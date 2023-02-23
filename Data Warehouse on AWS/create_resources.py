import boto3
import configparser
import logging
import json
import time
import argparse

from botocore.exceptions import ClientError


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARS
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
TOKEN = config.get('AWS', 'TOKEN')
DWH_IAM_ROLE_NAME = config.get('CLUSTER', 'IAM_ROLE_NAME')
DWH_CLUSTER_TYPE = config.get('CLUSTER', 'CLUSTER_TYPE')
DWH_NODE_TYPE = config.get('CLUSTER', 'NODE_TYPE')
DWH_NUM_NODES = config.get('CLUSTER', 'NUM_NODES')
DWH_CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
DWH_DB = config.get('DB', 'DB_NAME')
DWH_DB_USER = config.get('DB', 'DB_USER')
DWH_DB_PASSWORD = config.get('DB', 'DB_PASSWORD')
DWH_PORT = config.get('DB', 'DB_PORT')


def create_aws_resources():
    """
    Create all AWS resources
    """

    iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2', aws_session_token=TOKEN)
    redshift = boto3.client('redshift', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2', aws_session_token=TOKEN)
    ec2 = boto3.resource('ec2', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2', aws_session_token=TOKEN)
    s3 = boto3.resource('s3', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2', aws_session_token=TOKEN)
    logging.debug("Create AWS Resources.")
    return iam, redshift, ec2, s3


def create_iam_role(iam):
    """Function to create and attach IAM role.

    Keyword arguments:
    iam -- iam resource
    """

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

    except ClientError as e:
        logging.exception(e)

    # Define policy to access S3 bucket (ReadOnly)
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

    # Get the IAM role ARN
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logging.debug('IAM: {}, ARN: {} created'.format(DWH_IAM_ROLE_NAME, role_arn))
    config.set('IAM_ROLE', 'ARN', str(role_arn))
    with open('dwh.cfg', 'w') as f:
        config.write(f)
        
    return role_arn


def rds_create_cluster(redshift, role_arn):
    """Function to create redshift cluster.

    Keyword arguments:
    redshift -- redshift resource
    role_arn -- ARN role defined
    """

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
    """Function to open an incoming TCP port to access the cluster endpoint.

    Keyword arguments:
    ec2 -- ec2 resource
    vpc_id -- Virtual Private Cloud ID
    """

    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
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


def delete_rds_cluster(redshift):
    """Function to delete Redshift Cluster.

    Keyword arguments:
    redshift -- redshift resource
    """

    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
        logging.debug('Deleting Redshift Cluster {}.'.format(DWH_CLUSTER_IDENTIFIER))

    except ClientError as e:
        logging.exception(e)


def delete_iam_role(iam):
    """Function to detach and delete IAM role.

    Keyword arguments:
    iam -- iam resource
    """

    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        logging.debug('Deleting IAM role {}.'.format(DWH_IAM_ROLE_NAME))

    except ClientError as e:
        logging.exception(e)


def main(argument):
    """
    - Create/delete all the resources
    - Wait until RDS cluster become available
    - Set cfg cluster host
    """

    iam, redshift, ec2, s3 = create_aws_resources()

    if argument.delete:
        delete_rds_cluster(redshift)
        delete_iam_role(iam)

    else:
        role_arn = create_iam_role(iam)
        rds_create_cluster(redshift, role_arn)

        for x in range(0,600,10):
            cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            if cluster_props['ClusterStatus'] == 'available':
                logging.debug('Redshift Cluster is available and created at {}'.format(cluster_props['Endpoint']))
                config.set('DB', 'HOST', str(cluster_props['Endpoint']['Address']))
                with open('dwh.cfg', 'w') as f:
                    config.write(f)
                create_tcp(ec2, cluster_props['VpcId'])
                break
            logging.debug('Cluster status: {}. Attempt: {} - Waiting...'.format(cluster_props['ClusterStatus'], round((x/10)+1),0))
            time.sleep(10)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', dest='delete', action='store_true', default=False)
    args = parser.parse_args()
    main(args)




