# lambda_functions/weather_fetcher.py
import boto3
import requests
import json
import os
import logging
import sys
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
from decimal import Decimal

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
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')
    secrets = boto3.client('secretsmanager')
    logger.info("AWS clients initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize AWS clients: {str(e)}")
    raise

# Global variable for API key (lazy loading)
OPENWEATHER_API_KEY = None

def get_api_key():
    """Get API key from Secrets Manager with error handling"""
    global OPENWEATHER_API_KEY
    
    if OPENWEATHER_API_KEY:
        return OPENWEATHER_API_KEY
        
    try:
        logger.info("Retrieving OpenWeather API key from Secrets Manager")
        secret = secrets.get_secret_value(SecretId='openweather-api-key')
        secret_data = json.loads(secret['SecretString'])
        OPENWEATHER_API_KEY = secret_data['openweather-api-key']
        logger.info("API key retrieved successfully")
        return OPENWEATHER_API_KEY
        
    except ClientError as e:
        logger.error(f"Failed to retrieve API key from Secrets Manager: {str(e)}")
        raise Exception(f"Secrets Manager error: {str(e)}")
        
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Invalid secret format: {str(e)}")
        raise Exception(f"Secret parsing error: {str(e)}")

def lambda_handler(event=None, context=None):
    """
    TRIGGER: Bedrock Agent Action Group
    INPUT: farm_id (from agent)
    OUTPUT: Processed weather data
    """
    logger.info("Weather fetcher lambda started")
    logger.debug(f"Received event: {json.dumps(event, default=str)}")
    
    try:
        # Parse parameters from Bedrock Agent
        parameters = event.get('parameters', []) if event else []
        logger.info(f"Processing {len(parameters)} parameters")
        
        # Extract farm_id parameter
        farm_id = None
        for p in parameters:
            if isinstance(p, dict) and p.get('name') == 'farm_id':
                farm_id = p.get('value')
                break
        
        if not farm_id:
            logger.warning("Missing farm_id parameter, using default for testing")
            farm_id = "test-farm-001"
        
        logger.info(f"Fetching weather for farm: {farm_id}")
        
        # Cache checking disabled - always fetch fresh data
        cache_key = f"weather/{farm_id}/{datetime.now().strftime('%Y-%m-%d-%H')}.json"
        logger.debug(f"Cache disabled - would use key: {cache_key}")
        logger.info("Cache DISABLED - fetching fresh data from API")
        
        # Get farm location from DynamoDB
        logger.info(f"Fetching farm location from DynamoDB for farm_id: {farm_id}")
        farms_table = dynamodb.Table('FarmRegistry')
        
        try:
            response = farms_table.get_item(Key={'farm_id': farm_id})
            if 'Item' not in response:
                logger.warning(f"Farm {farm_id} not found in registry, using default location")
                lat, lon = Decimal('-3.0'), Decimal('-60.0')  # Amazon forest
            else:
                farm = response['Item']
                lat = farm['location']['lat']
                lon = farm['location']['lon']
                logger.info(f"Farm location retrieved: lat={lat}, lon={lon}")
                
        except ClientError as e:
            logger.error(f"DynamoDB error fetching farm location: {str(e)}")
            lat, lon = Decimal('-3.0'), Decimal('-60.0')  # Default location
        
        # Convert Decimal to float for API call
        lat_float = float(lat)
        lon_float = float(lon)
        
        # Get API key
        api_key = get_api_key()
        
        # Fetch weather data from OpenWeather API
        logger.info("Fetching weather data from OpenWeather API")
        weather_data = fetch_weather(lat_float, lon_float, api_key)
        
        # Cache writing disabled - skip S3 storage
        logger.info("Cache DISABLED - skipping S3 storage")
        logger.debug(f"Would cache to key: {cache_key}")
        
        logger.info("Weather data processing completed successfully")
        return success_response(weather_data)
        
    except Exception as e:
        logger.error(f"Unexpected error in weather fetcher: {str(e)}", exc_info=True)
        return error_response(f"Weather fetch failed: {str(e)}")

def fetch_weather(lat, lon, api_key):
    """
    Fetch 7-day forecast from OpenWeather with comprehensive error handling
    """
    logger.info(f"Fetching weather data for coordinates: {lat}, {lon}")
    
    url = "http://api.openweathermap.org/data/2.5/forecast"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': 'imperial',
        'cnt': 56  # 7 days × 8 (3-hour intervals)
    }
    
    logger.debug(f"API request params: {params}")
    
    try:
        logger.info("Making request to OpenWeather API")
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        
        raw_data = response.json()
        logger.info(f"API response received, status: {response.status_code}")
        logger.debug(f"API response contains {len(raw_data.get('list', []))} forecast entries")
        
        # Process the data
        processed_data = process_weather_data(raw_data, lat, lon)
        return processed_data
        
    except requests.exceptions.Timeout as e:
        logger.error(f"OpenWeather API timeout: {str(e)}")
        raise Exception(f"Weather API timeout after 15 seconds")
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"OpenWeather API HTTP error: {str(e)}")
        raise Exception(f"Weather API HTTP error: {response.status_code}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"OpenWeather API request error: {str(e)}")
        raise Exception(f"Weather API request failed: {str(e)}")
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse OpenWeather API response: {str(e)}")
        raise Exception(f"Invalid JSON response from weather API")
        
    except Exception as e:
        logger.error(f"Unexpected error fetching weather: {str(e)}")
        raise Exception(f"Weather fetch failed: {str(e)}")

def process_weather_data(raw_data, lat, lon):
    """
    Transform API response into disease-relevant metrics with error handling
    """
    logger.info("Processing weather data for disease analysis")
    
    try:
        forecasts = raw_data.get('list', [])
        if not forecasts:
            raise ValueError("No forecast data available")
            
        logger.debug(f"Processing {len(forecasts)} forecast entries")
        
        # Group by date
        daily_data = {}
        for forecast in forecasts:
            try:
                date = forecast['dt_txt'][:10]
                if date not in daily_data:
                    daily_data[date] = []
                
                daily_data[date].append({
                    'temp': forecast['main']['temp'],
                    'humidity': forecast['main']['humidity'],
                    'rain': forecast.get('rain', {}).get('3h', 0)
                })
            except (KeyError, TypeError) as e:
                logger.warning(f"Skipping malformed forecast entry: {str(e)}")
                continue
        
        logger.debug(f"Grouped data into {len(daily_data)} days")
        
        # Calculate daily summaries (what agent needs for disease prediction)
        daily_summaries = []
        for date, readings in list(daily_data.items())[:7]:
            try:
                if not readings:
                    logger.warning(f"No readings for date {date}, skipping")
                    continue
                    
                temps = [r['temp'] for r in readings if 'temp' in r]
                humidities = [r['humidity'] for r in readings if 'humidity' in r]
                
                if not temps or not humidities:
                    logger.warning(f"Insufficient data for date {date}, skipping")
                    continue
                
                daily_summaries.append({
                    'date': date,
                    'temp_high': max(temps),
                    'temp_low': min(temps),
                    'temp_avg': sum(temps) / len(temps),
                    'humidity_avg': sum(humidities) / len(humidities),
                    'rain_total_mm': sum(r.get('rain', 0) for r in readings),
                    'leaf_wetness_hours': sum(1 for r in readings if r.get('humidity', 0) > 90) * 3
                })
            except Exception as e:
                logger.warning(f"Error processing data for date {date}: {str(e)}")
                continue
        
        if not daily_summaries:
            raise ValueError("No valid daily summaries could be generated")
        
        logger.info(f"Generated {len(daily_summaries)} daily summaries")
        
        # Overall 7-day summary
        avg_temp = sum(d['temp_avg'] for d in daily_summaries) / len(daily_summaries)
        avg_humidity = sum(d['humidity_avg'] for d in daily_summaries) / len(daily_summaries)
        total_rain = sum(d['rain_total_mm'] for d in daily_summaries)
        
        result = {
            'location': {'lat': lat, 'lon': lon},
            'timestamp': datetime.now().isoformat(),
            'daily_forecast': daily_summaries,
            'summary_7day': {
                'avg_temp': round(avg_temp, 2),
                'avg_humidity': round(avg_humidity, 2),
                'total_rain_mm': round(total_rain, 2),
                'disease_favorable': avg_humidity > 70 and avg_temp > 60  # Basic heuristic
            }
        }
        
        logger.info("Weather data processing completed")
        return result
        
    except Exception as e:
        logger.error(f"Error processing weather data: {str(e)}")
        raise Exception(f"Weather data processing failed: {str(e)}")

def success_response(data):
    """Bedrock Agent response format"""
    logger.debug("Creating success response")
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': 'WeatherActions',
            'apiPath': '/get-weather',
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
            'actionGroup': 'WeatherActions',
            'apiPath': '/get-weather',
            'httpMethod': 'POST',
            'httpStatusCode': 400,
            'responseBody': {
                'application/json': {
                    'body': json.dumps({'error': message})
                }
            }
        }
    }

if __name__ == "__main__":
    try:
        logger.info("Starting test execution")
        result = lambda_handler()
        print(json.dumps(result, indent=2, default=str))
        logger.info("Test execution completed")
    except Exception as e:
        logger.error(f"Test execution failed: {str(e)}")
        sys.exit(1)

def process_weather_data(raw_data, lat, lon):
    """
    Transform API response into disease-relevant metrics
    """
    forecasts = raw_data['list']
    
    # Group by date
    daily_data = {}
    for forecast in forecasts:
        date = forecast['dt_txt'][:10]
        if date not in daily_data:
            daily_data[date] = []
        
        daily_data[date].append({
            'temp': forecast['main']['temp'],
            'humidity': forecast['main']['humidity'],
            'rain': forecast.get('rain', {}).get('3h', 0)
        })
    
    # Calculate daily summaries (what agent needs for disease prediction)
    daily_summaries = []
    for date, readings in list(daily_data.items())[:7]:
        temps = [r['temp'] for r in readings]
        humidities = [r['humidity'] for r in readings]
        
        daily_summaries.append({
            'date': date,
            'temp_high': max(temps),
            'temp_low': min(temps),
            'temp_avg': sum(temps) / len(temps),
            'humidity_avg': sum(humidities) / len(humidities),
            'rain_total_mm': sum(r['rain'] for r in readings),
            'leaf_wetness_hours': sum(1 for r in readings if r['humidity'] > 90) * 3
        })
    
    # Overall 7-day summary
    return {
        'location': {'lat': lat, 'lon': lon},
        'timestamp': datetime.now().isoformat(),
        'daily_forecast': daily_summaries,
        'summary_7day': {
            'avg_temp': sum(d['temp_avg'] for d in daily_summaries) / 7,
            'avg_humidity': sum(d['humidity_avg'] for d in daily_summaries) / 7,
            'total_rain_mm': sum(d['rain_total_mm'] for d in daily_summaries),
            'disease_favorable': True  # Agent will evaluate this
        }
    }

def success_response(data):
    """Bedrock Agent response format"""
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': 'WeatherActions',
            'apiPath': '/get-weather',
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
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': 'WeatherActions',
            'apiPath': '/get-weather',
            'httpMethod': 'POST',
            'httpStatusCode': 400,
            'responseBody': {
                'application/json': {
                    'body': json.dumps({'error': message})
                }
            }
        }
    }

if __name__ == "__main__":
    lambda_handler()
