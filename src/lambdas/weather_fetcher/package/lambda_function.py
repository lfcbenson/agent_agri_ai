# lambda_functions/weather_fetcher.py
import boto3
import requests
import json
import os
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')

# Get API key from Secrets Manager (not env var for security)
def get_api_key():
    secret = secrets.get_secret_value(SecretId='openweather-api-key')
    return json.loads(secret['SecretString'])['openweather-api-key']

OPENWEATHER_API_KEY = None  # Lazy load

def lambda_handler(event=None, context=None):
    """
    TRIGGER: Bedrock Agent Action Group
    INPUT: farm_id (from agent)
    OUTPUT: Processed weather data
    """
    
    # Parse parameters from Bedrock Agent
    print("fetching weather with event:", event)
    try:
        parameters = event.get('parameters', [])
    except Exception as e:
        parameters = []
    farm_id = next((p['value'] for p in parameters if p['name'] == 'farm_id'), None)
    
    if not farm_id:
        print("Missing farm_id parameter, using temporary farm_id for testing")
        farm_id = "test-farm-001"
        #return error_response("Missing farm_id parameter")
    
    print(f"Fetching weather for farm: {farm_id}")
    
    cache_key = f"weather/{farm_id}/{datetime.now().strftime('%Y-%m-%d-%H')}.json"
    
    try:
        cached = s3.get_object(Bucket='agri-ai-cache', Key=cache_key)
        weather_data = json.loads(cached['Body'].read())
        print("Cache HIT - returning cached weather")
        return success_response(weather_data)
    except:
        print("Cache MISS - fetching from API")
    
    # 2. Get farm location from DynamoDB
    farms_table = dynamodb.Table('FarmRegistry')
    try:
        farm = farms_table.get_item(Key={'farm_id': farm_id})['Item']
        lat = farm['location']['lat']
        lon = farm['location']['lon']
    except Exception as e:
        print("using temp location for testing")
        lat = 40.7128
        lon = -74.0060
        #return error_response(f"Farm {farm_id} not found in registry")
    
    # 3. Fetch from OpenWeather API
    global OPENWEATHER_API_KEY
    if not OPENWEATHER_API_KEY:
        OPENWEATHER_API_KEY = get_api_key()
    
    weather_data = fetch_weather(lat, lon, OPENWEATHER_API_KEY)
    
    # 4. Cache to S3 (6 hour TTL)
    s3.put_object(
        Bucket='agri-ai-cache',
        Key=cache_key,
        Body=json.dumps(weather_data),
        ContentType='application/json'
    )
    
    return success_response(weather_data)

def fetch_weather(lat, lon, api_key):
    """
    Fetch 7-day forecast from OpenWeather
    """
    url = "http://api.openweathermap.org/data/2.5/forecast"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': 'imperial',
        'cnt': 56  # 7 days × 8 (3-hour intervals)
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        raw_data = response.json()
    except Exception as e:
        raise Exception(f"OpenWeather API failed: {str(e)}")
    
    # Process into disease-relevant format
    return process_weather_data(raw_data, lat, lon)

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
