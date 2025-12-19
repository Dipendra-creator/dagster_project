"""
Helper script to set up sample MongoDB data for testing the pipeline
"""
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection
mongo_uri = "mongodb://localhost:27017/development"
client = MongoClient(mongo_uri)
db = client["development"]
collection = db["clients"]

# Clear existing data
collection.delete_many({})

# Sample clients data
sample_clients = [
    {
        "clientId": "CLI001",
        "name": "Acme Corporation",
        "reporting": True,
        "email": "admin@acme.com",
        "created_at": datetime.now()
    },
    {
        "clientId": "CLI002",
        "name": "Tech Solutions Ltd",
        "reporting": True,
        "email": "contact@techsolutions.com",
        "created_at": datetime.now()
    },
    {
        "clientId": "CLI003",
        "name": "Global Enterprises",
        "reporting": False,  # This one should NOT appear in results
        "email": "info@globalent.com",
        "created_at": datetime.now()
    },
    {
        "clientId": "CLI004",
        "name": "Digital Innovations",
        "reporting": True,
        "email": "hello@digitalinn.com",
        "created_at": datetime.now()
    },
    {
        "clientId": "CLI005",
        "name": "Cloud Services Inc",
        "reporting": True,
        "email": "support@cloudservices.com",
        "created_at": datetime.now()
    }
]

# Insert data
result = collection.insert_many(sample_clients)
print(f"✓ Inserted {len(result.inserted_ids)} clients into MongoDB")

# Verify
reporting_clients = list(collection.find({"reporting": True}))
print(f"✓ Found {len(reporting_clients)} clients with reporting=True")

for c in reporting_clients:
    print(f"  - {c['name']} (ID: {c['clientId']})")

client.close()
print("\n✓ Setup complete! You can now run the Dagster pipeline.")
