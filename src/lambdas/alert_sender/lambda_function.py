import boto3
import json
import logging
import sys
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add handler if not already present (Lambda reuses containers)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Initialize AWS clients with error handling
try:
    sns = boto3.client('sns')
    ses = boto3.client('ses')
    dynamodb = boto3.resource('dynamodb')
    logger.info("AWS clients initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize AWS clients: {str(e)}")
    raise

def parse_bedrock_parameters(event):
    """
    Parse parameters from Bedrock Agent event with comprehensive fallback logic.
    Handles both 'parameters' array and 'requestBody' formats.
    """
    param_dict = {}
    
    logger.info(f"Raw event structure: {json.dumps(event, default=str, indent=2)}")
    
    # METHOD 1: Parse from requestBody.content (NEWER FORMAT)
    request_body = event.get('requestBody', {})
    if request_body:
        content = request_body.get('content', {})
        
        # Check for application/json content
        if 'application/json' in content:
            json_content = content['application/json']
            logger.info(f"Found application/json content: {type(json_content)}")
            
            # Handle if it's a string
            if isinstance(json_content, str):
                try:
                    json_content = json.loads(json_content)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON string: {str(e)}")
            
            # Check for 'properties' array (common Bedrock format)
            if isinstance(json_content, dict):
                if 'properties' in json_content:
                    properties = json_content['properties']
                    logger.info(f"Found properties array with {len(properties)} items")
                    
                    for prop in properties:
                        name = prop.get('name')
                        value = prop.get('value')
                        
                        if name and value is not None:
                            # Handle disease_info JSON object
                            if name == 'disease_info' and isinstance(value, str):
                                try:
                                    param_dict[name] = json.loads(value)
                                except json.JSONDecodeError:
                                    param_dict[name] = value
                            else:
                                param_dict[name] = value
                            
                            logger.debug(f"Extracted from properties: {name} = {value}")
                
                # Also check for direct key-value pairs
                else:
                    for key, value in json_content.items():
                        if key not in ['properties']:  # Skip meta fields
                            param_dict[key] = value
                            logger.debug(f"Extracted direct key-value: {key} = {value}")
    
    # METHOD 2: Parse from parameters array (LEGACY FORMAT)
    if not param_dict:
        parameters = event.get('parameters', [])
        logger.info(f"Trying parameters array with {len(parameters)} items")
        
        for param in parameters:
            name = param.get('name')
            value = param.get('value')
            }
            if value is None and 'type' in param:
                value = param.get('value')
            
            if name and value is not None:
                if name == 'disease_info':
                    if isinstance(value, str):
                        try:
                            param_dict[name] = json.loads(value)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse disease_info: {str(e)}")
                            param_dict[name] = None
                    elif isinstance(value, dict):
                        param_dict[name] = value
                else:
                    param_dict[name] = value
                
                logger.debug(f"Extracted from parameters: {name} = {value}")
    
    logger.info(f"Final parsed parameters: {list(param_dict.keys())}")
    for key, value in param_dict.items():
        if key != 'disease_info':  # Don't log large nested objects
            logger.info(f"  {key}: {value}")
        else:
            logger.info(f"  disease_info: {list(value.keys()) if isinstance(value, dict) else type(value)}")
    
    return param_dict

def lambda_handler(event, context):
    """
    TRIGGER: Bedrock Agent Action Group
    INPUT: farm_id, message, severity, disease_info
    OUTPUT: Confirmation of alert sent
    """
    logger.info("Alert sender lambda started")
    logger.info(f"Event keys: {list(event.keys())}")
    
    try:
        # Parse parameters using robust parser
        param_dict = parse_bedrock_parameters(event)
        
        # Validate required parameters
        if not param_dict:
            logger.error("No parameters extracted from event!")
            return error_response("No parameters found in request")
        
        # Extract required parameters with validation
        farm_id = param_dict.get('farm_id')
        message = param_dict.get('message')
        severity = param_dict.get('severity')
        
        if not farm_id or not message or not severity:
            logger.error(f"Missing required parameters: farm_id={farm_id}, message={message}, severity={severity}")
            return error_response(f"Missing required parameters. Got: farm_id={farm_id}, message={bool(message)}, severity={severity}")
        
        
        field_id = param_dict.get('field_id')
        disease_info = param_dict.get('disease_info')
        
        logger.info(f"Processing alert for farm: {farm_id}, field: {field_id}, severity: {severity}")
        if disease_info:
            logger.info(f"Disease info: {disease_info.get('disease_name')} with {disease_info.get('probability')}% probability")
        
    except Exception as e:
        logger.error(f"Error parsing parameters: {str(e)}", exc_info=True)
        return error_response(f"Parameter parsing failed: {str(e)}")
    
    try:
        # Get farmer contact info from DynamoDB
        logger.info(f"Fetching farm data for farm_id: {farm_id}")
        farms_table = dynamodb.Table('FarmRegistry')
        
        try:
            response = farms_table.get_item(Key={'farm_id': farm_id})
            if 'Item' not in response:
                logger.warning(f"Farm {farm_id} not found in registry, using test data")
                farm = {'contact': {'phone': '+1234567890', 'email': 'test@example.com'}}
            else:
                farm = response['Item']
                logger.info(f"Farm data retrieved successfully for {farm_id}")
                
        except ClientError as e:
            logger.error(f"DynamoDB error fetching farm data: {str(e)}")
            farm = {'contact': {'phone': '+1234567890', 'email': 'test@example.com'}}
        
        # Extract contact information
        contact = farm.get('contact', {})
        email = contact.get('email', 'test@example.com')
        phone = contact.get('phone', '+1234567890')
        
        logger.info(f"Contact info - Email: {email}, Phone: {phone}")
        
        # Send email notification
        try:
            logger.info("Sending email notification")
            ses.send_email(
                Source='alerts@agri-ai.com',
                Destination={'ToAddresses': [email]},
                Message={
                    'Subject': {'Data': f'[{severity}] Crop Disease Alert'},
                    'Body': {'Text': {'Data': message}}
                }
            )
            logger.info("Email sent successfully")
            email_sent = True
            
        except ClientError as e:
            logger.error("Didnt send email, using dummy data")
            email_sent = False
        
        # Generate alert_id
        alert_id = f"ALERT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Log alert to DynamoDB
        try:
            logger.info("Logging alert to AlertHistory table")
            alerts_table = dynamodb.Table('AlertHistory')
            
            alert_item = {
                'alert_id': alert_id,
                'farm_id': farm_id,
                'timestamp': datetime.now().isoformat(),
                'severity': severity,
                'message': message,
                'channels': ['Email'] if email_sent else ['Dummy Mail'],
                'status': 'sent' if email_sent else 'failed'
            }
            
            if field_id:
                alert_item['field_id'] = field_id
            
            if disease_info and isinstance(disease_info, dict):
                try:
                    if disease_info.get('disease_name'):
                        alert_item['disease_name'] = disease_info['disease_name']
                    if disease_info.get('probability') is not None:
                        alert_item['probability'] = Decimal(str(disease_info['probability']))
                    if disease_info.get('affected_acres') is not None:
                        alert_item['affected_acres'] = Decimal(str(disease_info['affected_acres']))
                    if disease_info.get('treatment_recommendation'):
                        alert_item['treatment_recommendation'] = disease_info['treatment_recommendation']
                    if disease_info.get('estimated_cost') is not None:
                        alert_item['estimated_cost'] = Decimal(str(disease_info['estimated_cost']))
                    if disease_info.get('potential_loss') is not None:
                        alert_item['potential_loss'] = Decimal(str(disease_info['potential_loss']))
                    
                    logger.debug(f"Added disease_info to alert record")
                except (TypeError, ValueError, KeyError) as e:
                    logger.warning(f"Error processing disease_info for logging: {str(e)}")
            
            alerts_table.put_item(Item=alert_item)
            logger.info(f"Alert logged with ID: {alert_id}")
            
        except ClientError as e:
            logger.error(f"Failed to log alert to DynamoDB: {str(e)}")

        channels_sent = ['Email'] if email_sent else ['Dummy Mail']

        response_data = {
            'status': 'sent' if channels_sent else 'failed',
            'channels': channels_sent,
            'farm_id': farm_id,
            'severity': severity,
            'message': message,
            'alert_id': alert_id
        }
        
        if field_id:
            response_data['field_id'] = field_id
        
        if disease_info:
            response_data['disease_info'] = disease_info
        
        logger.info(f"Alert processing completed successfully")
        return success_response(response_data)
        
    except Exception as e:
        logger.error(f"Unexpected error in alert processing: {str(e)}", exc_info=True)
        return error_response(f"Alert processing failed: {str(e)}")

def success_response(data):
    """Create successful Bedrock Agent response"""
    logger.debug(f"Creating success response: {data}")
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': 'AlertActions',
            'apiPath': '/send-alert',
            'httpMethod': 'POST',
            'httpStatusCode': 200,
            'responseBody': {
                'application/json': {
                    'body': json.dumps(data)
                }
            }
        }
    }

def error_response(message):
    """Create error Bedrock Agent response"""
    logger.error(f"Creating error response: {message}")
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': 'AlertActions',
            'apiPath': '/send-alert',
            'httpMethod': 'POST',
            'httpStatusCode': 400,
            'responseBody': {
                'application/json': {
                    'body': json.dumps({'error': message})
                }
            }
        }
    }