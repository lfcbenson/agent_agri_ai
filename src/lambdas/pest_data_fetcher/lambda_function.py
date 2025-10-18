import boto3
import json
from decimal import Decimal
from datetime import datetime

dynamodb = boto3.resource('dynamodb')

# In-memory cache
DISEASE_KB_CACHE = None
CACHE_TIMESTAMP = None

def lambda_handler(event, context):
    """
    Query disease knowledge base from DynamoDB based on crop type and conditions
    
    Triggered by: Bedrock Agent (via Action Group)
    Input: crop_type, temperature, humidity, symptoms (optional)
    Output: List of matching diseases with characteristics
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Parse parameters
        parameters = event.get('parameters', [])
        param_dict = {p['name']: p['value'] for p in parameters}
        
        crop_type = param_dict.get('crop_type')
        temperature = float(param_dict.get('temperature', 0))
        humidity = float(param_dict.get('humidity', 0))
        symptoms_str = param_dict.get('symptoms', '')
        
        # Parse symptoms (comma-separated string to list)
        symptoms = [s.strip() for s in symptoms_str.split(',') if s.strip()] if symptoms_str else []
        
        print(f"Query params: crop={crop_type}, temp={temperature}, humidity={humidity}, symptoms={symptoms}")
        
        if not crop_type:
            return error_response("crop_type parameter is required")
        
        # Load disease knowledge base from DynamoDB
        disease_kb = load_disease_kb_from_dynamodb()
        
        # Filter diseases
        matching_diseases = find_matching_diseases(
            disease_kb, 
            crop_type, 
            temperature, 
            humidity, 
            symptoms
        )
        
        print(f"Found {len(matching_diseases)} matching diseases")
        
        return success_response(matching_diseases)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return error_response(f"Internal error: {str(e)}")

def load_disease_kb_from_dynamodb():
    """
    Load disease knowledge base from DynamoDB with in-memory caching
    """
    global DISEASE_KB_CACHE, CACHE_TIMESTAMP
    
    # Check cache (5 minute TTL)
    if DISEASE_KB_CACHE is not None and CACHE_TIMESTAMP is not None:
        age = (datetime.now() - CACHE_TIMESTAMP).total_seconds()
        if age < 300:  # 5 minutes
            print("Using cached disease KB")
            return DISEASE_KB_CACHE
    
    # Load from DynamoDB
    print("Loading disease KB from DynamoDB")
    table = dynamodb.Table('PestDiseaseKB')
    
    try:
        # Scan entire table
        response = table.scan()
        diseases = response['Items']
        
        # Handle pagination if more than 1MB of data
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            diseases.extend(response['Items'])
        
        # Convert DynamoDB Decimal to float for calculations
        diseases = convert_decimals(diseases)
        
        # Cache the result
        DISEASE_KB_CACHE = diseases
        CACHE_TIMESTAMP = datetime.now()
        
        print(f"Loaded {len(diseases)} diseases from DynamoDB")
        return diseases
        
    except Exception as e:
        raise Exception(f"Failed to load disease KB from DynamoDB: {str(e)}")

def convert_decimals(obj):
    """
    Convert DynamoDB Decimal types to float/int for JSON serialization
    """
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        # Convert to int if no decimal places, otherwise float
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj

def find_matching_diseases(diseases, crop_type, temperature, humidity, symptoms):
    """
    Filter diseases based on conditions
    
    Matching logic:
    1. Must affect the specified crop
    2. Current conditions fall within optimal range for disease
    3. (Optional) Symptoms match if provided
    """
    
    # Filter by crop type
    filtered = [d for d in diseases if d.get('crop_type', '').lower() == crop_type.lower()]
    
    print(f"Diseases for {crop_type}: {len(filtered)}")
    
    if len(filtered) == 0:
        return []
    
    # Calculate match score for each disease
    match_scores = []
    
    for disease in filtered:
        score = 0
        reasoning = []
        
        # Temperature match (0-40 points)
        # optimal_temp_range is a list: [min, max]
        temp_range = disease.get('optimal_temp_range', [0, 100])
        
        if not isinstance(temp_range, list) or len(temp_range) != 2:
            print(f"Warning: Invalid temp_range for {disease.get('name')}: {temp_range}")
            temp_min, temp_max = 0, 100
        else:
            temp_min = float(temp_range[0])
            temp_max = float(temp_range[1])
        
        if temp_min <= temperature <= temp_max:
            score += 40
            reasoning.append(f"Temperature {temperature}°F in optimal range [{temp_min}-{temp_max}°F]")
        elif temp_min - 5 <= temperature <= temp_max + 5:
            score += 20
            reasoning.append(f"Temperature {temperature}°F near optimal range [{temp_min}-{temp_max}°F]")
        else:
            reasoning.append(f"Temperature {temperature}°F outside optimal range [{temp_min}-{temp_max}°F]")
        
        # Humidity match (0-40 points)
        humidity_min = float(disease.get('optimal_humidity_min', 0))
        
        if humidity >= humidity_min:
            score += 40
            reasoning.append(f"Humidity {humidity}% above minimum {humidity_min}%")
        elif humidity >= humidity_min - 10:
            score += 20
            reasoning.append(f"Humidity {humidity}% near minimum threshold {humidity_min}%")
        else:
            reasoning.append(f"Humidity {humidity}% below optimal minimum {humidity_min}%")
        
        # Symptom match (0-20 points)
        if symptoms:
            # symptoms in DynamoDB is a list
            disease_symptoms = disease.get('symptoms', [])
            
            # Ensure disease_symptoms is a list
            if not isinstance(disease_symptoms, list):
                disease_symptoms = []
            
            # Find matching symptoms
            symptom_matches = [s for s in symptoms if s in disease_symptoms]
            
            if len(symptom_matches) > 0:
                symptom_score = (len(symptom_matches) / len(symptoms)) * 20
                score += symptom_score
                reasoning.append(f"Symptoms match ({len(symptom_matches)}/{len(symptoms)}): {symptom_matches}")
            else:
                reasoning.append(f"No symptom matches found")
        
        # Build result object
        result = {
            'disease_id': disease.get('disease_id', 'UNKNOWN'),
            'name': disease.get('name', 'Unknown Disease'),
            'scientific_name': disease.get('scientific_name', 'Unknown'),
            'match_score': round(score, 1),
            'growth_stages_vulnerable': disease.get('growth_stages_vulnerable', []),
            'optimal_temp_range': temp_range,
            'optimal_humidity_min': humidity_min,
            'symptoms': disease.get('symptoms', []),
            'treatment': disease.get('treatment', 'No treatment specified'),
            'match_reasoning': reasoning
        }
        
        # Add optional fields if present
        if 'cost_per_acre' in disease:
            result['cost_per_acre'] = float(disease['cost_per_acre'])
        if 'yield_loss_min' in disease:
            result['yield_loss_min'] = float(disease['yield_loss_min'])
        if 'yield_loss_max' in disease:
            result['yield_loss_max'] = float(disease['yield_loss_max'])
        if 'spread_rate' in disease:
            result['spread_rate'] = disease['spread_rate']
        if 'growth_stages_vulnerable' in disease:
            result['growth_stages_vulnerable'] = disease['growth_stages_vulnerable']
        
        match_scores.append(result)
    
    # Sort by match score (highest first)
    match_scores.sort(key=lambda x: x['match_score'], reverse=True)
    
    # Return top 5 matches
    top_matches = match_scores[:5]
    
    # Log top matches
    print(f"Top {len(top_matches)} diseases by match score:")
    for disease in top_matches:
        print(f"  - {disease['name']}: {disease['match_score']} points")
    
    return top_matches

def success_response(data):
    """
    Bedrock Agent response format
    """
    response = {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': 'PestActions',
            'apiPath': '/query-diseases',
            'httpMethod': 'POST',
            'httpStatusCode': 200,
            'responseBody': {
                'application/json': {
                    'body': json.dumps({
                        'matching_diseases': data,
                        'total_matches': len(data)
                    })
                }
            }
        }
    }
    
    print(f"Returning {len(data)} diseases")
    return response

def error_response(message):
    """
    Error response format
    """
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': 'PestActions',
            'apiPath': '/query-diseases',
            'httpMethod': 'POST',
            'httpStatusCode': 400,
            'responseBody': {
                'application/json': {
                    'body': json.dumps({'error': message})
                }
            }
        }
    }