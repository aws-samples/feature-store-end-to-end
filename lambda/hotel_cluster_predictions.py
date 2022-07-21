"""
Lambda function that gets triggered when new even is seen on the customer input kinesis stream.
This function uses the event data and joins it with data with additional feature data read
from an online feature store and then invokes a SageMaker endpoint to get prediction
for the hotel cluster. The results are written to a DynamoDB table.
"""
from datetime import datetime
from decimal import Decimal
import logging
import base64
import boto3
import json
import os


# Read environment variables
# these variables are overridden by the real-time inference notebook
# before streaming data on the customer input kinesis stream
ep_name = os.environ.get("ENDPOINT_NAME")
fg_name = os.environ.get("FG_NAME")
ddb_table_name = os.environ.get('DDB_TABLE_NAME')
online_feature_group_key = os.environ.get('ONLINE_FEATURE_GROUP_KEY')
feature_list_to_lookup = os.environ.get('ONLINE_FEATURE_GROUP_FEATURES_OF_INTEREST')
predicted_variable = os.environ.get('PREDICTED_VARIABLE')

# setup logging
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()

# Create session via Boto3
logger.info(f'boto3 version: {boto3.__version__}')
session = boto3.session.Session()

# sagemaker runtime for model endpoint invocation
sagemaker_runtime = boto3.client('runtime.sagemaker')
logger.info(f"boto3 session={session}, sagemaker_runtime={sagemaker_runtime}")

# feature store runtime for reading data from the online feature store
try:
    featurestore_runtime = boto3.Session().client(service_name='sagemaker-featurestore-runtime')
except Exception as e:
    logger.error('Failed to instantiate featurestore-runtime client, exception={e}')

logger.info(f'Lambda will call SageMaker ENDPOINT name: {ep_name}')
sagemaker_featurestore_client = session.client(service_name='sagemaker-featurestore-runtime',
                                               region_name = session.region_name)

# DynamoDB endpoint for writing prediction data
dynamodb = boto3.resource('dynamodb')

def decode_payload(event_data):
    """
    Decode base64 string read from kinesis envent into json
    """
    agg_data_bytes = base64.b64decode(event_data)
    decoded_data = agg_data_bytes.decode(encoding="utf-8") 
    event_payload = json.loads(decoded_data) 
    logger.info(f'decoded data from kinesis record: {event_payload}')
    return event_payload

def handler(event, context):
    """
    This handler is triggered by incoming Kinesis events,
    which contain a payload encapsulating the transaction data.
    The Lambda will then lookup corresponding records in the
    aggregate feature groups, assemble a payload for inference,
    and call the inference endpoint to generate a prediction.
    """
    logger.info('received event: {json.dumps(event, indent=2)}')

    records = event['Records']
    logger.info(f'event contains {len(records)} records')

    for rec in records:
        # Each record has separate eventID, etc.
        event_id = rec['eventID']
        event_source_arn = rec['eventSourceARN']
        logger.info(f'eventID: {event_id}, eventSourceARN: {event_source_arn}')
        logger.info(f"ep_name={ep_name}, fg_name={fg_name}, ddb_table_name={ddb_table_name}, "
                    f"online_feature_group_key={online_feature_group_key}, feature_list_to_lookup={feature_list_to_lookup}")

        kinesis = rec['kinesis']
        event_payload = decode_payload(kinesis['data'])
        # this is the json event as a string
        logger.info(event_payload)

        # we want to convert the list of key=value pairs into a comma separated
        # list that can be provided as a CSV input to the sagemaker endpoint
        # so we extract out the event values as a list which would later be merged
        # with the features we lookup from the feature store
        features_from_stream = [str(event_payload[k]) for k in event_payload.keys()]
        
        # a particular field in the event data is the key to be used for looking up
        # the online feature store, in effect we are joining the information read from the
        # event stream with additional information in the feature store
        key_val = str(event_payload[online_feature_group_key])
        logger.info(f"{online_feature_group_key}={key_val}")

        # lookup features of interest from the feature store for this key value
        # note that the features of interest is a csv string that needs to be convered
        # into a list (FeatureNames)
        fs_response = sagemaker_featurestore_client.get_record(FeatureGroupName=fg_name,
                                                               RecordIdentifierValueAsString=key_val,
                                                               FeatureNames=feature_list_to_lookup.split(","))
        logger.info(fs_response) 
        features_from_feature_store = [f['ValueAsString'] for f in fs_response['Record']]
        logger.info(f"features_from_feature_store={features_from_feature_store}")

        # at this point we have both the features from the real time stream and from the
        # feature store, so we are going to combine them now into a CSV string.
        # Note that the order in which the features in the stream and online feature store
        # are listed is very important and needs to be the same as what we used for training
        # the model. Here there should be checks that ensure that, since this is a demo
        # so am skipping that part (the whole feature list in the correct sequence (same as what
        # was used to train the model should be a function parameter)).
        csv_input_for_model = ",".join(features_from_stream + features_from_feature_store)
        logger.info(f"csv_input_for_model={csv_input_for_model}")
        # all set to invoke the sagemaker endpoint
        response = sagemaker_runtime.invoke_endpoint(EndpointName=ep_name,
                                                     ContentType='text/csv',
                                                     Body=csv_input_for_model)
        
        result = json.loads(response['Body'].read().decode())
        logger.info(f"resp from sagemaker = {result}")

        # write to dynamo db table 
        table = dynamodb.Table(ddb_table_name)
        # add the predicted variable to the dictionary
        event_payload['user_id'] = str(event_payload['user_id'])
        event_payload['date_time'] = str(datetime.utcnow().isoformat())
        event_payload[predicted_variable] = result
        event_payload = json.loads(json.dumps(event_payload), parse_float=Decimal)
        # put in the table
        table.put_item(Item=event_payload)
        logger.info(f"after writing result to ddb table = {ddb_table_name}")
        
    logger.info("all done, exiting")
    return
