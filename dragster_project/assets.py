import os
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from urllib.parse import quote_plus
from dagster import asset, AssetExecutionContext, MaterializeResult, ScheduleDefinition, define_asset_job
from pymongo import MongoClient
import redis
from dotenv import load_dotenv

load_dotenv()


def get_mongo_connection_string():
    """Build MongoDB connection string based on environment mode"""
    mode = os.getenv('MODE', 'LOCAL').upper()
    
    if mode == 'LOCAL':
        mongo_uri = os.getenv('LOCAL_MONGO_URI', 'mongodb://localhost:27017/development')
        db_name = os.getenv('LOCAL_MONGO_DATABASE', 'development')
        tls_config = {}
    elif mode == 'DEV':
        host = os.getenv('DEV_MONGO_HOST')
        username = os.getenv('DEV_MONGO_USERNAME')
        password = quote_plus(os.getenv('DEV_MONGO_PASSWORD'))
        db_name = os.getenv('DEV_MONGO_DATABASE', 'development')
        mongo_uri = f"mongodb+srv://{username}:{password}@{host}/{db_name}?retryWrites=true&w=majority"
        tls_config = {
            'tls': True,
            'tlsCAFile': os.path.join(os.path.dirname(__file__), '..', 'secrets', 'mgdb-bytescare-backend-development.pem')
        }
    elif mode == 'PROD':
        host = os.getenv('PROD_MONGO_HOST')
        username = os.getenv('PROD_MONGO_USERNAME')
        password = quote_plus(os.getenv('PROD_MONGO_PASSWORD'))
        db_name = os.getenv('PROD_MONGO_DATABASE', 'production')
        mongo_uri = f"mongodb+srv://{username}:{password}@{host}/{db_name}?retryWrites=true&w=majority"
        tls_config = {
            'tls': True,
            'tlsCAFile': os.path.join(os.path.dirname(__file__), '..', 'secrets', 'mgdb-bytescare-backend-production.pem')
        }
    else:
        raise ValueError(f"Invalid MODE: {mode}. Must be LOCAL, DEV, or PROD")
    
    return mongo_uri, db_name, tls_config


def get_redis_connection():
    """Build Redis connection based on environment mode"""
    mode = os.getenv('MODE', 'LOCAL').upper()
    
    if mode == 'LOCAL':
        return redis.Redis(
            host=os.getenv('LOCAL_REDIS_HOST', 'localhost'),
            port=int(os.getenv('LOCAL_REDIS_PORT', 6379)),
            db=int(os.getenv('LOCAL_REDIS_DB', 0)),
            decode_responses=True
        )
    else:  # DEV or PROD
        return redis.Redis(
            host=os.getenv('REDIS_HOST', '163.172.146.221'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            username=os.getenv('REDIS_USERNAME', 'bytescare'),
            password=os.getenv('REDIS_PASSWORD'),
            db=int(os.getenv('REDIS_DB', 0)),
            ssl=True,
            ssl_ca_certs=os.path.join(os.path.dirname(__file__), '..', 'secrets', 'SSL_redis-bytescare.pem'),
            decode_responses=True
        )


@asset(
    group_name="monthly_reporting_pipeline",
    description="Fetches client IDs from MongoDB where reporting is enabled"
)
def mongodb_clients(context: AssetExecutionContext) -> dict:
    """Fetch all clients from MongoDB where reporting is True"""
    mongo_uri, db_name, tls_config = get_mongo_connection_string()
    collection_name = os.getenv('CLIENTS_COLLECTION', 'clients')
    
    mode = os.getenv('MODE', 'LOCAL')
    context.log.info(f"Running in {mode} mode")
    context.log.info(f"Connecting to MongoDB: {mongo_uri.split('@')[-1] if '@' in mongo_uri else mongo_uri}")
    
    try:
        client = MongoClient(mongo_uri, **tls_config)
        db = client[db_name]
        collection = db[collection_name]
        
        # Query for clients where reporting is True
        query = {"reporting": True}
        clients = list(collection.find(query))
        
        context.log.info(f"Found {len(clients)} clients with reporting enabled")
        
        # Convert ObjectId to string for JSON serialization
        for c in clients:
            if '_id' in c:
                c['_id'] = str(c['_id'])
        
        client.close()
        
        return {
            "clients": clients,
            "count": len(clients),
            "fetched_at": datetime.now().isoformat()
        }
    
    except Exception as e:
        context.log.error(f"Error fetching clients from MongoDB: {e}")
        raise


@asset(
    group_name="monthly_reporting_pipeline",
    description="Publishes monthly reporting messages to Redis for each client",
    deps=[mongodb_clients]
)
def redis_monthly_reports(context: AssetExecutionContext, mongodb_clients: dict) -> MaterializeResult:
    """Push monthly reporting messages to Redis for each client"""
    mode = os.getenv('MODE', 'LOCAL')
    context.log.info(f"Running in {mode} mode")
    
    try:
        r = get_redis_connection()
        
        # Test connection
        r.ping()
        context.log.info("Redis connection successful")
        
        clients = mongodb_clients.get("clients", [])
        
        # Calculate date range for monthly report
        # Start date: first day of last month
        # End date: last day of last month
        now = datetime.now()
        # Get first day of current month, then subtract 1 month
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start_date = first_day_current_month - relativedelta(months=1)
        # End date is the last second of last month (one second before first day of current month)
        end_date = first_day_current_month - timedelta(seconds=1)
        
        published_count = 0
        
        for client in clients:
            client_id = client.get('id')
            
            if not client_id:
                context.log.warning(f"Client missing ID, skipping: {client}")
                continue
            
            # Create message payload
            message = {
                "clientId": str(client_id),
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "reportType": "monthly",
                "createdAt": datetime.now().isoformat()
            }
            
            # Push to Redis list
            queue_name = "monthly_reporting_queue"
            r.lpush(queue_name, json.dumps(message))
            
            context.log.info(f"Published message for client {client_id} to Redis queue: {queue_name}")
            published_count += 1
        
        # Get current queue length
        queue_length = r.llen("monthly_reporting_queue")
        
        return MaterializeResult(
            metadata={
                "clients_processed": published_count,
                "queue_name": "monthly_reporting_queue",
                "current_queue_length": queue_length,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "timestamp": datetime.now().isoformat()
            }
        )
    
    except redis.ConnectionError as e:
        context.log.error(f"Redis connection error: {e}")
        raise
    except Exception as e:
        context.log.error(f"Error publishing to Redis: {e}")
        raise


# Define a job that materializes both assets
monthly_reporting_job = define_asset_job(
    name="monthly_reporting_job",
    selection=["mongodb_clients", "redis_monthly_reports"],
    description="Monthly job to fetch clients and publish reporting messages to Redis"
)

# Define a monthly schedule that runs on the 1st day of each month at 2:00 AM
monthly_reporting_schedule = ScheduleDefinition(
    job=monthly_reporting_job,
    cron_schedule="0 2 1 * *",  # At 02:00 on day-of-month 1
    name="monthly_reporting_schedule",
    description="Runs monthly reporting job on the 1st of each month at 2:00 AM"
)
