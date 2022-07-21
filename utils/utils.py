"""
Utilities functions used across notebooks. These functions do require the requisite IAM roles
to be associated with the compute environment on which this code is being run. This code was
tested with SageMaker Notebooks which had an IAM role with appropriate policies attached to it.
"""
from pathlib import Path
from io import StringIO
import sagemaker
import logging
import boto3
import json
import time
import os

# setup logging
logger = logging.getLogger('__name__')

# global constants
CONFIG_DIR = "../config"


def get_cfn_stack_outputs(stack_name, key=None):
    """
    Use the boto3 cloudformation client to access the "Outputs" section and then
    read the requested key/value pair. If the key is not specified then the entire
    Outputs section is returned. A None is returned if the specified key is not 
    found in the Outputs section.
    """
    val = None
    cf_client = boto3.client('cloudformation')
    try:
        response = cf_client.describe_stacks(StackName=stack_name)
    except Exception as e:
        logger.error(f"get_cfn_stack_outputs, exception={e}")
        return val
        
    logger.debug(response)
    outputs = response["Stacks"][0]["Outputs"]
    if key is not None:
        for output in outputs:
            if output['OutputKey'] == key:
                val = output['OutputValue']
                return val
        logger.error(f"{key} not found in the Outputs section")
        return val
    else:
        logger.info("no input key is specified, returning the entire Outputs section")
        val = outputs
    return val

def get_cfn_stack_parameters(stack_name, key=None):
    """
    Use the boto3 cloudformation client to access the "Parameters" section and then
    read the requested key/value pair. If the key is not specified then the entire
    Parameters section is returned. A None is returned if the specified key is not 
    found in the Parameters section.
    """
    val = None
    cf_client = boto3.client('cloudformation')
    try:
        response = cf_client.describe_stacks(StackName=stack_name)
    except Exception as e:
        logger.error(f"get_cfn_stack_parameters, exception={e}")
        return val
        
    logger.debug(response)
    params = response["Stacks"][0]["Parameters"]
    if key is not None:
        for param in params:
            if param['ParameterKey'] == key:
                val = param['ParameterValue']
                return val
        logger.error(f"{key} not found in the Parameters section")
        return val
    else:
        logger.info("no input key is specified, returning the entire Parameters section")
        val = params
    return val

def cast_object_to_string(df):
    """
    Look for columns of datatype "object" and casts them as string.
    The change works "in-place" and the dataframe does not need to be returned.
    
    """
    for label in df.columns:
        if df.dtypes[label] == 'object':
            df[label] = df[label].astype("str").astype("string")
            

def check_feature_group_status(feature_group):
    """
    Checks the status of a feature group, if the status is not "Creating"
    then waits for 5s and checks again. Does not return until the status
    remains "Creating".
    """
    status = feature_group.describe().get("FeatureGroupStatus")
    while status == "Creating":
        logger.info("Waiting for Feature Group to be Created")
        time.sleep(5)
        status = feature_group.describe().get("FeatureGroupStatus")
    logger.info(f"FeatureGroup {feature_group.name} successfully created.")
    
def write_param(param_name, param_value, config_dir=CONFIG_DIR):
    """
    Write the config param key/value pair to a file so that it can be read
    by the next stage.
    """
    # create the config dir if not present
    if Path(config_dir).is_dir() is False:
        logger.error(f"write_param, {CONFIG_DIR} does not exist, creating it now")
        os.makedirs(config_dir)
    
    # create a file with the param name and write the val to the file
    fpath = os.path.join(config_dir, param_name)
    logger.info(f"write_param, fpath={fpath}, writing {param_name}={param_value}")
    with open(fpath, "w") as f:
        f.write(param_value)
        
def read_param(param_name, config_dir=CONFIG_DIR):
    """
    Read parameter from file with same name as the param name
    from the config directory.
    """
    param_value = None
    fpath = os.path.join(config_dir, param_name)
    path = Path(fpath)
    if path.is_file():
        with open(fpath, "r") as f:
            param_value = f.read()
            logger.info(f"read_param, fpath={fpath}, read {param_name}={param_value}")
    return param_value

def upload_df_to_s3(df, data_bucket_name, bucket_fpath):
    """
    Upload a dataframe to an S3 bucket. The dataframe is NOT written to a local file
    before uploading, upload is done directly from a memomory buffer.
    """
    logger.info(f"upload_df_to_s3, going to upload df of shape={df.shape} to s3 object {os.path.join(data_bucket_name, bucket_fpath)}")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(data_bucket_name, bucket_fpath).put(Body=csv_buffer.getvalue())
    
    
def _escape_tag_chars(in_str):
    escaped_str = in_str.replace('$', '_D_')
    escaped_str = escaped_str.replace('?', '_Q_')
    escaped_str = escaped_str.replace('&', '_A_')
    escaped_str = escaped_str.replace('#', '_H_')
    return escaped_str

def _unescape_tag_chars(in_str):
    unescaped_str = in_str.replace('_D_', '$')
    unescaped_str = unescaped_str.replace('_Q_', '?')
    unescaped_str = unescaped_str.replace('_A_', '&')    
    unescaped_str = unescaped_str.replace('_H_', '#')
    return unescaped_str

sm_sess = sagemaker.Session()
default_bucket = sm_sess.default_bucket()

def describe_feature_group(fg_name):
    return sagemaker_client.describe_feature_group(FeatureGroupName=fg_name)

def _get_offline_details(fg_name, s3_uri=None):
    _data_catalog_config = describe_feature_group(fg_name)['OfflineStoreConfig']['DataCatalogConfig']
    _table = _data_catalog_config['TableName']
    _database = _data_catalog_config['Database']

    if s3_uri is None:
        s3_uri = f's3://{default_bucket}/offline-store'
    _tmp_uri = f'{s3_uri}/query_results/'
    return _table, _database, _tmp_uri

def get_historical_record_count(fg_name, s3_uri=None):
    _table, _database, _tmp_uri = _get_offline_details(fg_name, s3_uri)
    _query_string = f'SELECT COUNT(*) FROM "' +_table+ f'"'
    _tmp_df = _run_query(_query_string, _tmp_uri, _database, verbose=False)
    return _tmp_df.iat[0, 0]

def delete_all_objs_from_s3_bucket(bucket_name):
    """
    Deletes all objects from an S3 bucket
    """
    try:
        s3 = boto3.resource('s3')
        logger.info(f"delete_all_objs_from_s3_bucket, going to delete objects from bucket={bucket_name}")
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
        logger.info(f"delete_all_objs_from_s3_bucket, after deleting objects from bucket={bucket_name}")
    except Exception as e:
        logger.error(f"delete_all_objs_from_s3_bucket, exception \"{str(e)}\" occured while deleting objects from bucket={bucket_name}")
        
def delete_sagemaker_model_resources(ep_name):
    """
    Deletes sagemaker model end point, end point configuratio and finally the model itself
    """    
    try:
        # Specify your AWS Region
        aws_region = boto3.Session().region_name

        # Create a low-level SageMaker service client.
        sagemaker_client = boto3.client('sagemaker', region_name=aws_region)

        # Store DescribeEndpointConfig response into a variable that we can index in the next step.
        logger.info(f"delete_sagemaker_model_resources, ep_name={ep_name}")
        response = sagemaker_client.describe_endpoint_config(EndpointConfigName=ep_name)
        logger.info(json.dumps(response, indent=2, default=str))  
        # Endpoint config name
        ep_config_name = response['EndpointConfigName']

        # Model name
        model_name = response['ProductionVariants'][0]['ModelName']

        logger.info(f"delete_sagemaker_model_resources, going to delete ep_config_name={ep_config_name}, ep_name{ep_name}, model_name={model_name}")

        # Delete endpoint configuration
        sagemaker_client.delete_endpoint_config(EndpointConfigName=ep_config_name)                        

        # Delete endpoint
        sagemaker_client.delete_endpoint(EndpointName=ep_name)

        # Delete the model 
        sagemaker_client.delete_model(ModelName=model_name)
        
        logger.info(f"delete_sagemaker_model_resources, after deleting ep_config_name={ep_config_name}, ep_name{ep_name}, model_name={model_name}")
    except Exception as e:
        logger.error(f"delete_sagemaker_model_resources, exception={str(e)}")