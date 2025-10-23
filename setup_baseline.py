import time
import pprint
from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.errors import ServerSelectionTimeoutError, DuplicateKeyError, PyMongoError

# ---------------------------------------------------------------------
# 0. Connection Setup
# ---------------------------------------------------------------------
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"

# ---------------------------------------------------------------------
# 1. Ensure replica set initialized
# ---------------------------------------------------------------------

print(f"\n[Step 1] Connecting to MongoDB replica set: {MONGO_URI}")

has_shown_instructions = False

while True:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, retryWrites=False)
        client.admin.command("ping")
        print("Replica Set connection established.")
        db = client["userDB"]
        print("MongoDB Replica Set is ready and accessible.")
        break  # Exit the loop when ready

    except ServerSelectionTimeoutError:
        if not has_shown_instructions:
            print("Unable to connect to the MongoDB replica set. It might not be initialized yet.")
            print("\nPlease run the following commands in another terminal:\n")
            print("""
                docker exec -it mongo1 mongosh --eval '
                    rs.initiate({
                    _id: "rs0",
                    members: [
                        { _id: 0, host: "mongo1:27017" },
                        { _id: 1, host: "mongo2:27017" },
                        { _id: 2, host: "mongo3:27017" }
                    ]
                })'
            """)
            print("After initialization, this script will automatically connect once MongoDB is ready.\n")
            has_shown_instructions = True

        print("Waiting for MongoDB to become ready... retrying in 5 seconds.")
        time.sleep(5)

# ---------------------------------------------------------------------
# 2. Insert a baseline document before failure
# ---------------------------------------------------------------------
print("\n[Step 2] Inserting baseline documents BEFORE failover...")

# Use a 'majority' write concern to ensure writes are replicated to most nodes
coll_base = db.get_collection("userProfile", write_concern=WriteConcern("majority"))
# Check how many documents currently exist in the collection
existing_count = coll_base.count_documents({})
print(f"Documents in userProfile before insert: {existing_count}")

# Display all existing documents for reference
if existing_count > 0:
    print(f"Detected {existing_count} existing documents in 'userProfile'.")
    print("Cleaning up old data to ensure a fresh baseline test...")
    coll_base.delete_many({}) 
    print("Collection cleared.\n")
else:
    print("Collection is currently empty.")

# Insert multiple baseline documents (u001–u005) before simulating failover
for i in range(1, 6):
    user_id = f"u{i:03}"  # Create IDs like u001, u002, ...
    try:
        coll_base.insert_one({
            "_id": user_id,
            "user_id": user_id,
            "username": f"Tan_{i}",
            "email": f"Tan_{i}@example.com",
            "inserted_before_failover": True,
            "last_login_time": time.time()
        })
        print(f"Inserted {user_id}")
        time.sleep(0.5)  # Optional: simulate staggered inserts to observe replication timing

    except DuplicateKeyError:
        print(f"{user_id} already exists — skipping")

    except Exception as e:
        print(f"Unexpected error while inserting {user_id}: {e}")

# Display all documents after insertion for verification
print("\nAll documents in userProfile after baseline insertion:")
pprint.pp(list(coll_base.find()))