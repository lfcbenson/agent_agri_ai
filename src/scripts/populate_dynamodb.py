#Add code to create table if it doesnt exist

import boto3
import json
import logging
import sys
from decimal import Decimal
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('populate_dynamodb.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Initialize DynamoDB resource with error handling
try:
    dynamodb = boto3.resource('dynamodb')
    dynamodb_client = boto3.client('dynamodb')
    logger.info("DynamoDB resource initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize DynamoDB resource: {str(e)}")
    raise

def create_alert_history_table():
    """Create the AlertHistory table if it doesn't exist"""
    table_name = 'AlertHistory'
    logger.info(f"Checking if {table_name} table exists")
    
    try:
        # Check if table exists
        existing_tables = dynamodb_client.list_tables()['TableNames']
        
        if table_name in existing_tables:
            logger.info(f"{table_name} table already exists")
            return True
            
        logger.info(f"Creating {table_name} table")
        
        # Create the table
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'alert_id',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'alert_id',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Wait for table to be created
        logger.info(f"Waiting for {table_name} table to be created...")
        table.wait_until_exists()
        
        logger.info(f"{table_name} table created successfully")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"Error creating {table_name} table - Code: {error_code}, Message: {error_message}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error creating {table_name} table: {str(e)}", exc_info=True)
        return False

def create_farm_registry_table():
    """Create the FarmRegistry table if it doesn't exist"""
    table_name = 'FarmRegistry'
    logger.info(f"Checking if {table_name} table exists")
    
    try:
        # Check if table exists
        existing_tables = dynamodb_client.list_tables()['TableNames']
        
        if table_name in existing_tables:
            logger.info(f"{table_name} table already exists")
            return True
            
        logger.info(f"Creating {table_name} table")
        
        # Create the table
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'farm_id',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'farm_id',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Wait for table to be created
        logger.info(f"Waiting for {table_name} table to be created...")
        table.wait_until_exists()
        
        logger.info(f"{table_name} table created successfully")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"Error creating {table_name} table - Code: {error_code}, Message: {error_message}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error creating {table_name} table: {str(e)}", exc_info=True)
        return False

def create_pest_disease_kb_table():
    """Create the PestDiseaseKB table if it doesn't exist"""
    table_name = 'PestDiseaseKB'
    logger.info(f"Checking if {table_name} table exists")
    
    try:
        # Check if table exists
        existing_tables = dynamodb_client.list_tables()['TableNames']
        
        if table_name in existing_tables:
            logger.info(f"{table_name} table already exists")
            return True
            
        logger.info(f"Creating {table_name} table")
        
        # Create the table
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'disease_id',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'disease_id',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Wait for table to be created
        logger.info(f"Waiting for {table_name} table to be created...")
        table.wait_until_exists()
        
        logger.info(f"{table_name} table created successfully")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"Error creating {table_name} table - Code: {error_code}, Message: {error_message}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error creating {table_name} table: {str(e)}", exc_info=True)
        return False

def populate_farms():
    """Populate the FarmRegistry table with farm data"""
    logger.info("Starting farm data population")
    
    try:
        table = dynamodb.Table('FarmRegistry')
        logger.info("Connected to FarmRegistry table")
        
        farms = [
            
            {
                'farm_id': 'FARM-002',
                'farmer_name': 'Sarah Johnson',
                'location': {'lat': Decimal('41.8780'), 'lon': Decimal('-93.0977')},
                'fields': [
                    {
                        'field_id': 'FIELD-A',
                        'acres': 200,
                        'crop_type': 'soybeans',
                        'planting_date': '2024-05-01',
                        'growth_stage': 'R2'
                    }
                ],
                'contact': {
                    'phone': '+15559876543',
                    'email': 'sarah@example.com'
                }
            },
            {
                'farm_id': 'FARM-003',
                'farmer_name': 'Michael Chen',
                'location': {'lat': Decimal('39.7392'), 'lon': Decimal('-104.9903')},
                'fields': [
                    {
                        'field_id': 'FIELD-A',
                        'acres': 150,
                        'crop_type': 'wheat',
                        'planting_date': '2024-03-20',
                        'growth_stage': 'Feekes 9'
                    },
                    {
                        'field_id': 'FIELD-B',
                        'acres': 95,
                        'crop_type': 'wheat',
                        'planting_date': '2024-03-25',
                        'growth_stage': 'Feekes 8'
                    }
                ],
                'contact': {
                    'phone': '+15552345678',
                    'email': 'mchen@example.com'
                }
            },
            {
                'farm_id': 'FARM-004',
                'farmer_name': 'Emily Rodriguez',
                'location': {'lat': Decimal('42.0308'), 'lon': Decimal('-93.6319')},
                'fields': [
                    {
                        'field_id': 'FIELD-A',
                        'acres': 175,
                        'crop_type': 'corn',
                        'planting_date': '2024-04-20',
                        'growth_stage': 'V8'
                    }
                ],
                'contact': {
                    'phone': '+15553456789',
                    'email': 'emily.r@example.com'
                }
            },
            {
                'farm_id': 'FARM-005',
                'farmer_name': 'David Patterson',
                'location': {'lat': Decimal('40.5853'), 'lon': Decimal('-105.0844')},
                'fields': [
                    {
                        'field_id': 'FIELD-A',
                        'acres': 220,
                        'crop_type': 'soybeans',
                        'planting_date': '2024-05-10',
                        'growth_stage': 'V6'
                    },
                    {
                        'field_id': 'FIELD-B',
                        'acres': 130,
                        'crop_type': 'corn',
                        'planting_date': '2024-04-18',
                        'growth_stage': 'V12'
                    }
                ],
                'contact': {
                    'phone': '+15554567890',
                    'email': 'dpatterson@example.com'
                }
            },
            {
                'farm_id': 'FARM-007',
                'farmer_name': 'Robert Martinez',
                'location': {'lat': Decimal('40.2338'), 'lon': Decimal('-104.7219')},
                'fields': [
                    {
                        'field_id': 'FIELD-A',
                        'acres': 140,
                        'crop_type': 'wheat',
                        'planting_date': '2024-03-15',
                        'growth_stage': 'Feekes 10'
                    },
                    {
                        'field_id': 'FIELD-B',
                        'acres': 110,
                        'crop_type': 'corn',
                        'planting_date': '2024-04-22',
                        'growth_stage': 'V9'
                    },
                    {
                        'field_id': 'FIELD-C',
                        'acres': 75,
                        'crop_type': 'soybeans',
                        'planting_date': '2024-05-08',
                        'growth_stage': 'V4'
                    }
                ],
                'contact': {
                    'phone': '+15556789012',
                    'email': 'rmartinez@example.com'
                }
            }
        ]
        
        logger.info(f"Preparing to populate {len(farms)} farms")
        
        successful_farms = 0
        failed_farms = 0
        
        for farm in farms:
            try:
                farm_id = farm.get('farm_id', 'unknown')
                logger.debug(f"Processing farm: {farm_id}")
                
                table.put_item(Item=farm)
                successful_farms += 1
                logger.info(f"Successfully added farm: {farm_id}")
                
            except ClientError as e:
                failed_farms += 1
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                logger.error(f"DynamoDB error adding farm {farm.get('farm_id', 'unknown')} - Code: {error_code}, Message: {error_message}")
                
            except Exception as e:
                failed_farms += 1
                logger.error(f"Unexpected error adding farm {farm.get('farm_id', 'unknown')}: {str(e)}", exc_info=True)
        
        logger.info(f"Farm population completed - Success: {successful_farms}, Failed: {failed_farms}")
        return successful_farms, failed_farms
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"DynamoDB table access error - Code: {error_code}, Message: {error_message}")
        raise Exception(f"Failed to access FarmRegistry table: {error_message}")
        
    except Exception as e:
        logger.error(f"Unexpected error in populate_farms: {str(e)}", exc_info=True)
        raise

def populate_diseases():
    """Populate the PestDiseaseKB table with disease data"""
    logger.info("Starting disease data population")
    
    try:
        table = dynamodb.Table('PestDiseaseKB')
        logger.info("Connected to PestDiseaseKB table")
        
        diseases = [
            {
        'disease_id': 'DISEASE-001',
        'name': 'Gray Leaf Spot',
        'scientific_name': 'Cercospora zeae-maydis',
        'crop_type': 'corn',
        'optimal_temp_range': [70, 85],  # List: [min, max]
        'optimal_humidity_min': 75,
        "growth_stages_vulnerable": ["V8", "V10", "R1"],
        'symptoms': ['leaf_discoloration', 'ndvi_decline', 'lesions'],  # List
        'treatment': 'Pyraclostrobin + Fluxapyroxad',
        'cost_per_acre': 22,
        'yield_loss_min': 15,
        'yield_loss_max': 30,
        'spread_rate': 'moderate'
        },
        {
        'disease_id': 'DISEASE-002',
        'name': 'Southern Corn Rust',
        'scientific_name': 'Puccinia polysora',
        'crop_type': 'corn',
        'optimal_temp_range': [75, 90],
        'optimal_humidity_min': 80,
        "growth_stages_vulnerable": ["V9", "V12", "R1"],
        'symptoms': ['orange_pustules', 'rapid_decline', 'leaf_damage'],
        'treatment': 'Azoxystrobin',
        'cost_per_acre': 18,
        'yield_loss_min': 20,
        'yield_loss_max': 40,
        'spread_rate': 'fast'
        },
        {
        'disease_id': 'DISEASE-003',
        'name': 'Northern Corn Leaf Blight',
        'scientific_name': 'Exserohilum turcicum',
        'crop_type': 'corn',
        'optimal_temp_range': [60, 80],
        'optimal_humidity_min': 70,
        "growth_stages_vulnerable": ["V8", "V10", "V9", "R1"],
        'symptoms': ['cigar_shaped_lesions', 'ndvi_decline'],
        'treatment': 'Propiconazole',
        'cost_per_acre': 20,
        'yield_loss_min': 10,
        'yield_loss_max': 25,
        'spread_rate': 'moderate'
        },
        {
        'disease_id': 'DISEASE-004',
        'name': 'Common Corn Rust',
        'scientific_name': 'Puccinia sorghi',
        'crop_type': 'corn',
        'optimal_temp_range': [60, 77],
        'optimal_humidity_min': 65,
        "growth_stages_vulnerable": ["R2", "R1"],
        'symptoms': ['rust_pustules', 'leaf_yellowing'],
        'treatment': 'Tebuconazole',
        'cost_per_acre': 15,
        'yield_loss_min': 5,
        'yield_loss_max': 15,
        'spread_rate': 'slow'
        },
        {
        'disease_id': 'DISEASE-005',
        'name': 'Anthracnose Leaf Blight',
        'scientific_name': 'Colletotrichum graminicola',
        'crop_type': 'corn',
        'optimal_temp_range': [70, 85],
        'optimal_humidity_min': 80,
        "growth_stages_vulnerable": ["V10", "V12", "R1"],
        'symptoms': ['water_soaked_lesions', 'stalk_rot'],
        'treatment': 'Mancozeb',
        'cost_per_acre': 17,
        'yield_loss_min': 10,
        'yield_loss_max': 20,
        'spread_rate': 'moderate'
        },
        {
        'disease_id': 'DISEASE-006',
        'name': 'Sudden Death Syndrome',
        'scientific_name': 'Fusarium virguliforme',
        'crop_type': 'soybeans',
        'optimal_temp_range': [60, 75],
        'optimal_humidity_min': 70,
        "growth_stages_vulnerable": ["R2", "V10", "R1"],
        'symptoms': ['leaf_yellowing', 'root_rot', 'ndvi_decline'],
        'treatment': 'Fluopyram seed treatment',
        'cost_per_acre': 25,
        'yield_loss_min': 10,
        'yield_loss_max': 30,
        'spread_rate': 'slow'
        },
        {
        'disease_id': 'DISEASE-007',
        'name': 'White Mold',
        'scientific_name': 'Sclerotinia sclerotiorum',
        'crop_type': 'soybeans',
        'optimal_temp_range': [60, 75],
        'optimal_humidity_min': 85,
        "growth_stages_vulnerable": ["V4", "R1"],
        'symptoms': ['white_fungal_growth', 'stem_lesions'],
        'treatment': 'Prothioconazole',
        'cost_per_acre': 28,
        'yield_loss_min': 10,
        'yield_loss_max': 40,
        'spread_rate': 'moderate'
        },
        {
        'disease_id': 'DISEASE-008',
        'name': 'Frogeye Leaf Spot',
        'scientific_name': 'Cercospora sojina',
        'crop_type': 'soybeans',
        'optimal_temp_range': [70, 85],
        'optimal_humidity_min': 75,
        "growth_stages_vulnerable": ["R1"],
        'symptoms': ['circular_lesions', 'leaf_spots'],
        'treatment': 'Azoxystrobin + Propiconazole',
        'cost_per_acre': 23,
        'yield_loss_min': 5,
        'yield_loss_max': 20,
        'spread_rate': 'moderate'
        },
        {
        'disease_id': 'DISEASE-009',
        'name': 'Fusarium Head Blight',
        'scientific_name': 'Fusarium graminearum',
        'crop_type': 'wheat',
        'optimal_temp_range': [60, 80],
        'optimal_humidity_min': 80,
        "growth_stages_vulnerable": ["Feekes 8", "Feekes 9", "R1"],
        'symptoms': ['bleached_heads', 'shriveled_kernels'],
        'treatment': 'Tebuconazole + Prothioconazole',
        'cost_per_acre': 24,
        'yield_loss_min': 10,
        'yield_loss_max': 50,
        'spread_rate': 'fast'
        },
        {
        'disease_id': 'DISEASE-010',
        'name': 'Stripe Rust',
        'scientific_name': 'Puccinia striiformis',
        'crop_type': 'wheat',
        'optimal_temp_range': [50, 65],
        'optimal_humidity_min': 70,
        "growth_stages_vulnerable": ["Feekes 9", "R1"],
        'symptoms': ['yellow_striped_pustules', 'rapid_spread'],
        'treatment': 'Pyraclostrobin',
        'cost_per_acre': 19,
        'yield_loss_min': 10,
        'yield_loss_max': 30,
        'spread_rate': 'very_fast'
        },
        {
        'disease_id': 'DISEASE-011',
        'name': 'Powdery Mildew',
        'scientific_name': 'Blumeria graminis',
        'crop_type': 'wheat',
        'optimal_temp_range': [60, 72],
        'optimal_humidity_min': 60,
        "growth_stages_vulnerable": ["Feekes 8", "V10"],
        'symptoms': ['white_powdery_growth', 'leaf_coverage'],
        'treatment': 'Sulfur-based fungicide',
        'cost_per_acre': 12,
        'yield_loss_min': 5,
        'yield_loss_max': 20,
        'spread_rate': 'moderate'
        }
        ]
        
        logger.info(f"Preparing to populate {len(diseases)} diseases")
        
        successful_diseases = 0
        failed_diseases = 0
        
        for disease in diseases:
            try:
                disease_name = disease.get('name', 'unknown')
                logger.debug(f"Processing disease: {disease_name}")
                
                table.put_item(Item=disease)
                successful_diseases += 1
                logger.info(f"Successfully added disease: {disease_name}")
                
            except ClientError as e:
                failed_diseases += 1
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                logger.error(f"DynamoDB error adding disease {disease.get('name', 'unknown')} - Code: {error_code}, Message: {error_message}")
                
            except Exception as e:
                failed_diseases += 1
                logger.error(f"Unexpected error adding disease {disease.get('name', 'unknown')}: {str(e)}", exc_info=True)
        
        logger.info(f"Disease population completed - Success: {successful_diseases}, Failed: {failed_diseases}")
        return successful_diseases, failed_diseases
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"DynamoDB table access error - Code: {error_code}, Message: {error_message}")
        raise Exception(f"Failed to access PestDiseaseKB table: {error_message}")
        
    except Exception as e:
        logger.error(f"Unexpected error in populate_diseases: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    logger.info("Starting DynamoDB population script")
    
    try:
        # Create tables if they don't exist
        logger.info("Creating DynamoDB tables if they don't exist...")
        
        logger.info("Creating FarmRegistry table")
        if not create_farm_registry_table():
            logger.error("Failed to create FarmRegistry table")
            sys.exit(1)
        
        logger.info("Creating PestDiseaseKB table")
        if not create_pest_disease_kb_table():
            logger.error("Failed to create PestDiseaseKB table")
            sys.exit(1)
            
        logger.info("Creating AlertHistory table")
        if not create_alert_history_table():
            logger.error("Failed to create AlertHistory table")
            sys.exit(1)
        
        logger.info("All tables created successfully or already exist")
        
        logger.info("POPULATING FARMS")      
        farm_success, farm_failed = populate_farms()
        
        logger.info("POPULATING DISEASES")
        disease_success, disease_failed = populate_diseases()
        
        logger.info(f"Farms - Success: {farm_success}, Failed: {farm_failed}")
        logger.info(f"Diseases - Success: {disease_success}, Failed: {disease_failed}")
        
            
    except Exception as e:
        logger.error(f"Fatal error in population script: {str(e)}", exc_info=True)
        sys.exit(1)