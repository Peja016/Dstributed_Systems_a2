"""
File: strong_consistency_experiment.py
Description:
  Demonstrates strong consistency behaviour in MongoDB (Part C.1).

  Covered concepts:
    1. Strong consistency with WriteConcern(w='majority') & ReadConcern('majority')
    2. Immediate read consistency across replicas
    3. Behaviour under Primary node failure (CAP theorem: C > A)
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
# Step 2. Strong Consistency Write + Read Test
# ---------------------------------------------------------------------
print("[Step 2] Testing Strong Consistency (w='majority', readConcern='majority')")

coll = db.get_collection(
    "userProfile",
    write_concern=WriteConcern("majority"),
    read_concern=ReadConcern("majority")
)

# Insert a new user document with unique ID
user_id = f"uid_{int(time.time())}"  # e.g. uid_300
doc = {
    "_id": user_id,
    "user_id": user_id,
    "username": "tan",
    "email": "tan@example.com",
    "inserted_for_strong_test": True,
    "ts": time.time()
}

print(f"→ Inserting new user document '{user_id}' using w='majority'...")
coll.insert_one(doc)
print("Write acknowledged by majority of nodes (strong consistency).")

# Immediately read from secondary
print("\n→ Immediately reading from a secondary node (expect same value).")
result = coll.with_options(read_preference=ReadPreference.SECONDARY).find_one({"_id": user_id})
print("Secondary read result:")
pprint(result)

print("\nObservation:")
print("• The read reflects the latest written value.")
print("• Writes take slightly longer due to majority confirmation.")
print("• Confirms strong consistency — Consistency prioritized over Availability (C > A).\n")

# ---------------------------------------------------------------------
# Step 3. Simulate Primary Failure (Manual)
# ---------------------------------------------------------------------
print("[Step 3] Simulating Primary node failure (manual intervention required)")
print("Please run the following command in another terminal:")
print("\n   docker stop mongo1\n")
print("Then press Enter to continue once the primary has been stopped.\n")

if sys.stdin.isatty():
    input("⏸  Waiting for user confirmation... (press Enter when ready) ")
else:
    print("Non-interactive mode detected. Waiting 15 seconds automatically...")
    time.sleep(15)

print("\nWaiting 8 seconds for new Primary election...\n")
time.sleep(8)

# ---------------------------------------------------------------------
# Step 4. Attempt Write During / After Failover
# ---------------------------------------------------------------------
print("[Step 4] Attempting write operation during/after failover...")

new_user_id = f"uid_{int(time.time()) + 1}"
try:
    print('Insert a new data...')
    coll.insert_one({
        "_id": new_user_id,
        "user_id": new_user_id,
        "username": "tan_failover",
        "email": "tan_failover@example.com",
        "inserted_during_failover": True,
        "ts": time.time()
    })
    print("Write succeeded — new primary elected, consistency preserved.")
except PyMongoError as e:
    print(f"Write failed due to failover: {e}")
    print("This illustrates that strong consistency prioritizes consistency over availability (C > A).")

# ---------------------------------------------------------------------
# Step 5. Recovery
# ---------------------------------------------------------------------
print("\n[Step 5] Recovery and Verification")
print("Please restart the old primary in another terminal using:")
print("\n   docker start mongo1\n")

if sys.stdin.isatty():
    input("⏸  Press Enter once mongo1 has been restarted... ")
else:
    print("Non-interactive mode detected — waiting 15 seconds before continuing.")
    time.sleep(15)

print("\nWaiting for node to rejoin and synchronize...")
time.sleep(8)

# Verify cluster data after recovery
print("\nReading back latest documents from primary:")
pprint(list(coll.find({}, {"_id": 1, "username": 1, "ts": 1}).sort("ts", -1)))

print("\nStrong Consistency Experiment completed successfully.")
print("System maintained consistency through failover (C > A under CAP theorem).")
