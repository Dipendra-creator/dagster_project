import os
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dagster import asset, AssetExecutionContext, MaterializeResult, ScheduleDefinition, define_asset_job
from pymongo import MongoClient
import redis
from dotenv import load_dotenv

load_dotenv()


@asset(
    group_name="monthly_reporting_pipeline",
    description="Fetches client IDs from MongoDB where reporting is enabled"
)
def mongodb_clients(context: AssetExecutionContext) -> dict:
    """Fetch all clients from MongoDB where reporting is True"""
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/development')
    db_name = os.getenv('DATABASE_NAME', 'development')
    collection_name = os.getenv('CLIENTS_COLLECTION', 'clients')
    
    context.log.info(f"Connecting to MongoDB: {mongo_uri}")
    
    try:
        client = MongoClient(mongo_uri)
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
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_db = int(os.getenv('REDIS_DB', 0))
    
    context.log.info(f"Connecting to Redis: {redis_host}:{redis_port}")
    
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        
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
