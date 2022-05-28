import boto3
import json
import time
from botocore.exceptions import ClientError
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES          = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE          = config.get('DWH', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB                 = config.get('DWH', 'DWH_DB')
DWH_DB_USER            = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD           = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT               = config.get('DWH', 'DWH_PORT')
DWH_IAM_ROLE_NAME      = config.get('DWH', 'DWH_IAM_ROLE_NAME')


def create_clients():
    '''
    Create an Identity and Access Management, a Redshift and an EC2 client
    
    Args:
    None
    
    Returns (list):
    iam: Identity and Access Management client
    redshift: Redshift client
    ec2: EC2 client
    '''

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

 


    print("Created Clients")
    return iam, redshift, ec2

def create_iam_role(iam):
    '''
    Create an Identity and Access Management Role
    
    Args:
    iam: Identity and Access Management client
    
    Returns:
    iam_arn: ARN of the created Identity and Access Management Role
    '''

    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
    print("Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    iam_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    return iam_arn


def create_redshift_cluster(iam, redshift, ec2):
    '''
    Create an Amazon Redshift Cluster
    
    Args:
    iam: Identity and Access Management client
    redshift: Redshift client
    ec2: EC2 client
    
    Returns:
    None
    '''

    try:
        roleArn = create_iam_role(iam)
        response = redshift.create_cluster(        
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )

    except Exception as e:
        print(e)

    # Wait until the cluster is available for further operations
    while True:
        response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            print("The cluster is ready")
            break
        time.sleep(10)

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    print("IAM Role ARN:  {}".format(myClusterProps['IamRoles'][0]['IamRoleArn']))
    print("Endpoint:  {}".format(myClusterProps['Endpoint']['Address']))


def main():

    iam, redshift, ec2 = create_clients()
    create_redshift_cluster(iam, redshift, ec2)


if __name__ == "__main__":
    main()