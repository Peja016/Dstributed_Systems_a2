"""
File: consistency_causal.py
Description:
  Demonstrates MongoDB causal consistency using client sessions.
  Shows how causally related operations (A → B) are observed in order,
  even if concurrent operations are not necessarily synchronized.

  Concept: Causal Consistency (Optional / Bonus)
  CAP Perspective: Balances Consistency and Availability by preserving
  causal order without requiring full synchronization.
"""

from pymongo import MongoClient
import time
from pprint import pprint

# ---------------------------------------------------------------------
# [Step 1] Connect to the MongoDB Replica Set
# ---------------------------------------------------------------------
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"

print(f"\n[Step 1] Connecting to MongoDB replica set: {MONGO_URI}")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
client.admin.command("ping")
print("Connected to MongoDB replica set successfully.")

db = client["userDB"]
coll = db["causalTest"]

# ---------------------------------------------------------------------
# [Step 2] Demonstrate causal dependency between operations
# ---------------------------------------------------------------------
print("\n[Step 2] Demonstrating causal consistency using session...")

# Start a causally consistent session
session = client.start_session(causal_consistency=True)

# Simulate Client A writing the first document (causal origin)
with session.start_transaction():
    doc_a = {"_id": "msg1", "text": "Hello world", "author": "ClientA", "ts": time.time()}
    coll.insert_one(doc_a, session=session)
    print("Client A wrote 'Hello world'.")

# Simulate Client B reading msg1 and then replying (causally dependent operation)
doc_read = coll.find_one({"_id": "msg1"}, session=session)
if doc_read:
    doc_b = {
        "_id": "msg2",
        "text": "Nice post!",
        "author": "ClientB",
        "reply_to": "msg1",
        "ts": time.time()
    }
    coll.insert_one(doc_b, session=session)
    print("Client B wrote 'Nice post!' in response.")

# ---------------------------------------------------------------------
# [Step 3] Verify causally ordered results
# ---------------------------------------------------------------------
print("\n[Step 3] Reading documents within the same session (causal order preserved):")
results = list(coll.find({}, session=session))
pprint(results)

print("\nObservation:")
print("• The dependent document ('Nice post!') appears only after the original ('Hello world').")
print("• MongoDB ensures this ordering within the same causally consistent session.")
print("• Causal consistency guarantees that related operations are observed in order,")
print("  even across distributed replicas, without requiring full global synchronization.")
print("• This model provides a balance between consistency and availability in distributed systems.")

print("\nCausal Consistency Experiment completed successfully.\n")

session.end_session()
client.close()
