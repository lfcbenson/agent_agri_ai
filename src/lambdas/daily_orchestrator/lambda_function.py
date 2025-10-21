import boto3
import json
import logging
import sys
from datetime import datetime
import os
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
import time

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)
config = Config(connect_timeout=10, read_timeout=300, retries={'max_attempts': 0})

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add handler if not already present (Lambda reuses containers)
if not logger.handlers:
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler for local debugging
    try:
        # Create logs directory if it doesn't exist
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Create file handler with timestamp
        log_filename = f"daily_orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_filepath = os.path.join(log_dir, log_filename)
        
        file_handler = logging.FileHandler(log_filepath)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"File logging enabled: {log_filepath}")
    except Exception as e:
        logger.warning(f"Could not setup file logging: {str(e)}")

try:
    dynamodb = boto3.resource('dynamodb')
    logger.info("AWS clients initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize AWS clients: {str(e)}")
    raise

def lambda_handler(event, context):
    """
    TRIGGER: CloudWatch EventBridge - cron(0 6 * * ? *)
    PURPOSE: Start daily monitoring for all farms
    """
    logger.info("Daily orchestrator lambda started")
    logger.debug(f"Received event: {json.dumps(event, default=str)}")
    
    
    try:
        logger.info("Fetching all farms from FarmRegistry")
        farms_table = dynamodb.Table('FarmRegistry')
        
        try:
            response = farms_table.scan()
            farms = response.get('Items', [])
            logger.info(f"Retrieved {len(farms)} farms from registry")
            
            # Handle pagination if needed
            while 'LastEvaluatedKey' in response:
                logger.debug("Fetching additional farms (pagination)")
                response = farms_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                farms.extend(response.get('Items', []))
                
        except ClientError as e:
            logger.error(f"DynamoDB error fetching farms: {str(e)}")
            farms = [{'farm_id': 'test-farm-001'}]
            logger.warning("Using fallback test farm data")
        
               
        logger.info(f"Starting daily monitoring for {len(farms)} farms")
        
        successful_farms = 0
        failed_farms = 0
        
        for farm in farms:
            try:
                farm_id = farm.get('farm_id')
                if not farm_id:
                    logger.warning(f"Farm missing farm_id: {farm}")
                    continue
                
                logger.info(f"Processing farm: {farm_id}")
                fields = farm.get('fields', [])
                if not fields:
                    logger.warning(f"Farm {farm_id} has no fields, using default field")
                    field_ids = ['FIELD-A']  # Default field
                else:
                    field_ids = [field.get('field_id') for field in fields if field.get('field_id')]
                    if not field_ids:
                        logger.warning(f"Farm {farm_id} fields missing field_id, using default")
                        field_ids = ['FIELD-A']
                
                logger.info(f"Farm {farm_id} has {len(field_ids)} fields: {field_ids}")
                
                farm_success = True
                for field in fields:
                    field_id = field.get('field_id')
                    crop_type = field.get('crop_type', 'unknown')
                    growth_stage = field.get('growth_stage', 'unknown')
                    acres = field.get('acres', 0)
                                     
                    logger.info(f"Processing field: {field_id} for farm: {farm_id} (crop: {crop_type}, stage: {growth_stage}, acres: {acres})")
                    time.sleep(40)
                    try:
                        success = invoke_agent_for_farm(farm_id, field_id, crop_type, growth_stage, acres)
                    except Exception as e:
                        logger.error(f"Error invoking agent for farm {farm_id}, field {field_id}: {str(e)}", exc_info=True)
                        success = False
                    
                    if not success:
                        farm_success = False
                        logger.error(f"Failed to initiate monitoring for farm: {farm_id}, field: {field_id}")
                
                if farm_success:
                    successful_farms += 1
                    logger.info(f"Successfully initiated monitoring for all fields in farm: {farm_id}")
                else:
                    failed_farms += 1
                    logger.error(f"Failed to initiate monitoring for some/all fields in farm: {farm_id}")
                
            except Exception as e:
                failed_farms += 1
                logger.error(f"Error processing farm {farm.get('farm_id', 'unknown')}: {str(e)}", exc_info=True)
        
        response_data = {
            'statusCode': 200,
            'body': {
                'message': f"Daily monitoring initiated",
                'total_farms': len(farms),
                'successful': successful_farms,
                'failed': failed_farms,
                'timestamp': datetime.now().isoformat()
            }
        }
        
        logger.info(f"Daily orchestration completed: {response_data['body']}")
        return response_data
        
    except Exception as e:
        logger.error(f"Unexpected error in daily orchestrator: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'error': f"Daily orchestration failed: {str(e)}",
                'timestamp': datetime.now().isoformat()
            }
        }

def invoke_agent_for_farm(farm_id, field_id, crop_type, growth_stage, acres):
    session_id = f"daily-{farm_id}-{field_id}-{datetime.now().strftime('%Y%m%d%H%M')}"
    logger.debug(f"Using session ID: {session_id}")
    
    agent_id = os.environ.get('AGENT_ID')
    agent_alias_id = os.environ.get('AGENT_ALIAS_ID')
    
    if not agent_id or not agent_alias_id:
        logger.error("Missing required environment variables: AGENT_ID or AGENT_ALIAS_ID")
        return False
    
    logger.info(f"Invoking agent {agent_id} with alias {agent_alias_id}")
    
    
    input_text = f"Monitor farm {farm_id}, field {field_id} for disease threats. Field details: {acres} acres of {crop_type} at growth stage {growth_stage}. Analyze satellite data for anomalies, check weather conditions, query for matching diseases, and send alerts if threats are detected."
    logger.info(f"Agent input text: {input_text}")
    
    try:
        start_time = time.time()
        bedrock_agent_runtime = boto3.client('bedrock-agent-runtime', region_name='us-east-1', config=config)
        response = bedrock_agent_runtime.invoke_agent(
            agentId=agent_id,
            agentAliasId=agent_alias_id,
            sessionId=session_id,
            inputText=input_text,
#            enableTrace=True
        )
        completion = ""
        trace_data = []
        errors = []
        action_groups_invoked = []
        
        for event in response.get("completion", []):
            if "chunk" in event:
                chunk = event["chunk"]
                if "bytes" in chunk:
                    chunk_text = chunk["bytes"].decode("utf-8")
                    completion += chunk_text
                    logger.debug(f"Received chunk: {chunk_text[:100]}")
            # Handle trace events to track agent actions
            elif "trace" in event:
                trace = event["trace"]["trace"]
                trace_data.append(trace)
                
                if "orchestrationTrace" in trace:
                    orch_trace = trace["orchestrationTrace"]

                    if "invocationInput" in orch_trace:
                        invocation = orch_trace["invocationInput"]
                        if "actionGroupInvocationInput" in invocation:
                            action_group = invocation["actionGroupInvocationInput"].get("actionGroupName", "unknown")
                            action_groups_invoked.append(action_group)
                            logger.info(f"Action group invoked: {action_group}")
                    
                    if "modelInvocationOutput" in orch_trace:
                        output_str = orch_trace['modelInvocationOutput']['rawResponse']['content']
                        logger.info(f"Model invocation output: {output_str}")
                    
                    if "observation" in orch_trace:
                        observation = orch_trace["observation"]
                        if "actionGroupInvocationOutput" in observation:
                            output = observation["actionGroupInvocationOutput"]['text']
                            logger.info(f"Action group output: {output}")
                    
                    # if "rationale" in orch_trace:
                    #     model_thinking = orch_trace['rationale']['text']
                    #     logger.debug('thinking..')
                    #     logger.debug(model_thinking)

        execution_time = time.time() - start_time
        logger.info(f"Agent execution completed in {execution_time:.2f} seconds")

        if completion:
            logger.info(f"Agent completion text for farm {farm_id}, field {field_id}: {completion}")
        return True
    
    except Exception as e:
        logger.error(f"Unexpected error invoking agent for {farm_id}-{field_id}: {str(e)}", exc_info=True)
        return False

            
if __name__ == "__main__":
    try:
        logger.info("Starting test execution")
        # Set test environment variables
        os.environ['AGENT_ID'] = ""  ## enter your agent id here
        os.environ['AGENT_ALIAS_ID'] = "" ## enter your agent alias id here
        result = invoke_agent_for_farm("FARM-001", "FIELD-A", "corn", "V10", 120)
        logger.info(json.dumps(result, indent=2, default=str))
        logger.info("Test execution completed")
    except Exception as e:
        logger.error(f"Test execution failed: {str(e)}", exc_info=True)
        sys.exit(1)