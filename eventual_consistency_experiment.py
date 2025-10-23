"""
File: eventual_consistency_experiment.py
Description:
  Demonstrates eventual consistency behaviour in MongoDB (Part C.2).

  Covered concepts:
    1. Weak write concern (w=1)
    2. Local read concern (non-majority reads)
    3. Stale reads before replication completes
    4. Eventual synchronization (A > C in CAP theorem)
"""

from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError
import time
import sys
from pprint import pprint

# ---------------------------------------------------------------------
# Step 1. Connect to MongoDB Replica Set
# ---------------------------------------------------------------------
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"

print(f"\n[Step 1] Connecting to MongoDB replica set: {MONGO_URI}")
while True:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["userDB"]
        print("Connected to MongoDB replica set.\n")
        break
    except ServerSelectionTimeoutError:
        print("Waiting for MongoDB to become ready... retrying in 5 seconds.")
        time.sleep(5)

# ---------------------------------------------------------------------
# Step 2. Eventual Consistency Test (Write Concern = 1, ReadConcern = local)
# ---------------------------------------------------------------------
print("[Step 2] Testing Eventual Consistency (w=1, ReadConcern='local')")

coll = db.get_collection(
    "userProfile",
    write_concern=WriteConcern(1),       # Acknowledge only from primary
    read_concern=ReadConcern("local")    # May read unreplicated data
)

# Write a new document to the primary only
user_id = f"uid_{int(time.time())}"
doc = {
    "_id": user_id,
    "user_id": user_id,
    "username": "rose_eventual",
    "email": "rose_eventual@example.com",
    "inserted_for_eventual_test": True,
    "ts": time.time()
}

print(f"→ Writing document '{user_id}' with w=1 (fast, non-majority)...")
coll.insert_one(doc)
print("Write acknowledged by primary only (not guaranteed replicated yet).")

# Immediately read from secondary
print("\n→ Immediately reading from a secondary node (may see old data).")
result_initial = coll.with_options(read_preference=ReadPreference.SECONDARY).find_one({"_id": user_id})
print("Secondary read result (initial):")
pprint(result_initial)

# Poll until secondary catches up (demonstrate eventual consistency)
print("\nPolling every 1s until secondary catches up...")
for i in range(10):
    res = coll.with_options(read_preference=ReadPreference.SECONDARY).find_one({"_id": user_id})
    print(f"Read {i}: {res}")
    if res and res.get("user_id") == user_id:
        print("Secondary caught up — eventual consistency achieved.")
        break
    time.sleep(1)
else:
    print("Secondary did not catch up within 10 seconds (replication lag).")

# ---------------------------------------------------------------------
# Step 3. Observation Summary
# ---------------------------------------------------------------------
print("\nObservation:")
print("• The first read from the secondary may return None or old data.")
print("• After several seconds, the document becomes visible once replication completes.")
print("• Demonstrates **Eventual Consistency**, where all nodes converge over time.")
print("• Reflects **Availability prioritized over Consistency (A > C)** per CAP theorem.")
print("• Such design is acceptable for use cases like social media likes, view counts, or IoT sensor updates.")

print("\nEventual Consistency Experiment completed successfully.\n")
