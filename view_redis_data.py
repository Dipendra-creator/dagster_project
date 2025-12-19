"""
Script to view messages stored in the Redis monthly reporting queue
"""
import redis
import json
import os
from dotenv import load_dotenv

load_dotenv()

def view_redis_queue():
    """View all messages in the Redis monthly reporting queue"""
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_db = int(os.getenv('REDIS_DB', 0))
    queue_name = "monthly_reporting_queue"
    
    print(f"Connecting to Redis: {redis_host}:{redis_port}")
    
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        
        # Test connection
        r.ping()
        print("✓ Redis connection successful\n")
        
        # Get queue length
        queue_length = r.llen(queue_name)
        print(f"{'='*60}")
        print(f"Queue: {queue_name}")
        print(f"Total Messages: {queue_length}")
        print(f"{'='*60}\n")
        
        if queue_length == 0:
            print("No messages in the queue.")
            return
        
        # Get all messages from the queue (without removing them)
        messages = r.lrange(queue_name, 0, -1)
        
        for idx, message in enumerate(messages, 1):
            try:
                data = json.loads(message)
                print(f"Message {idx}:")
                print(f"  Client ID:    {data.get('clientId')}")
                print(f"  Report Type:  {data.get('reportType')}")
                print(f"  Start Date:   {data.get('startDate')}")
                print(f"  End Date:     {data.get('endDate')}")
                print(f"  Created At:   {data.get('createdAt')}")
                print()
            except json.JSONDecodeError:
                print(f"Message {idx}: {message}")
                print()
        
        print(f"{'='*60}")
        print(f"Total: {len(messages)} messages displayed")
        print(f"{'='*60}")
        
    except redis.ConnectionError as e:
        print(f"✗ Redis connection error: {e}")
        print("Make sure Redis is running on localhost:6379")
    except Exception as e:
        print(f"✗ Error: {e}")

def clear_redis_queue():
    """Clear all messages from the Redis queue"""
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_db = int(os.getenv('REDIS_DB', 0))
    queue_name = "monthly_reporting_queue"
    
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        r.ping()
        
        queue_length = r.llen(queue_name)
        if queue_length == 0:
            print("Queue is already empty.")
            return
        
        response = input(f"Are you sure you want to delete {queue_length} messages? (yes/no): ")
        if response.lower() in ['yes', 'y']:
            r.delete(queue_name)
            print(f"✓ Cleared {queue_length} messages from the queue.")
        else:
            print("Cancelled.")
            
    except Exception as e:
        print(f"✗ Error: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "clear":
        clear_redis_queue()
    else:
        view_redis_queue()
        print("\nTip: Run 'python view_redis_data.py clear' to clear the queue")
