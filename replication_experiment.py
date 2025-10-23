"""
File: replication_experiment.py
Description:
  Demonstrates MongoDB replication behavior for Part B (Replication Strategies).

  Covered concepts:
    1. Replication Factor (RF = 3)
    2. Write Concern & latency comparison (w:1 vs w:'majority' vs w:3)
    3. Leader–Follower (Primary–Backup) model
    4. Automatic leader election after primary failure
    5. Re-sync of old primary
"""
import sys
import time
import pprint
import statistics
from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.errors import ServerSelectionTimeoutError, DuplicateKeyError, PyMongoError

# ---------------------------------------------------------------------
# 0. Connection Setup
# ---------------------------------------------------------------------
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"

# ---------------------------------------------------------------------
# 1. Ensure replica set initialized
# ---------------------------------------------------------------------

print(f"\n[Step 1] Check if connecting to MongoDB replica set: {MONGO_URI}")

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
# 2. Check Data
# ---------------------------------------------------------------------
print("\n[Step 2] Checking for existing documents and inserting baseline data if needed...")

# Use a 'majority' write concern to ensure durability across the replica set
coll_base = db.get_collection("userProfile", write_concern=WriteConcern("majority"))

# Count existing documents
existing_count = coll_base.count_documents({})
print(f"Documents currently in 'userProfile': {existing_count}")

if existing_count == 0:
    print("No existing data detected. Inserting baseline documents (u001–u005)...\n")

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
            time.sleep(0.3)  # Slight delay for observation
        except DuplicateKeyError:
            print(f"{user_id} already exists — skipping.")
        except Exception as e:
            print(f"Unexpected error while inserting {user_id}: {e}")

else:
    print("Existing data found — baseline insertion skipped.")
    print("Existing documents (sample):")
    # pprint.pp(list(coll_base.find({}, {"_id": 0})))

# Display final collection status
final_count = coll_base.count_documents({})
print(f"\nTotal documents in 'userProfile': {final_count}")

# ---------------------------------------------------------------------
# 3. Compare write latency with different WriteConcerns
# ---------------------------------------------------------------------
print("\n[Step 3] Comparing write latency for w=1, w='majority', and w=3...")

latency_summary = []

for w in [1, "majority", 3]:
    latencies = []
    coll = db.get_collection("latencyTest", write_concern=WriteConcern(w=w))

    # Run 50 insert operations to get stable average latency
    for _ in range(50):
        start = time.time()
        coll.insert_one({"w": w, "ts": time.time()})
        latencies.append(time.time() - start)

    avg_latency = statistics.mean(latencies)
    std_latency = statistics.stdev(latencies)

    # Print detailed per-writeconcern results
    print(f"WriteConcern = {w:<9} → avg latency = {avg_latency:.4f}s, std dev = {std_latency:.4f}s")

    latency_summary.append((w, avg_latency, std_latency))

# Print final summary neatly
print("\nLatency comparison summary (average of 50 runs):")
for w, avg, std in latency_summary:
    print(f"w={w:<9} → {avg:.4f}s (±{std:.4f}s)")

# ---------------------------------------------------------------------
# 4. Simulate primary node failure
# ---------------------------------------------------------------------
print("\n[Step 4] Simulating PRIMARY node failure programmatically...")
print("Please run the following command in another terminal to stop the primary node:\n")
old_primary = client.primary[0]
print(f"docker stop {old_primary}\n")

if sys.stdin.isatty():
    # it mode
    input(f"Press Enter once you have stopped {old_primary}... ")
else:
    # no it mode
    print("Non-interactive mode detected — waiting 15 seconds before continuing automatically...\n")
    time.sleep(15)

print("Waiting for automatic re-election...")
time.sleep(8)

# ---------------------------------------------------------------------
# 5. Check new primary
# ---------------------------------------------------------------------
print("\n[Step 5] Waiting for new PRIMARY election...")

primary = None
max_wait = 60  # maximum waiting
start_time = time.time()

while not primary and (time.time() - start_time) < max_wait:
    try:
        status = client.admin.command("replSetGetStatus")
        for m in status["members"]:
            print(f"  - {m['name']} → {m['stateStr']}")
        primary = next(
            (m["name"] for m in status["members"] if m["stateStr"] == "PRIMARY"), None
        )
        if not primary:
            print("No PRIMARY yet, waiting 3 seconds...\n")
            time.sleep(3)
        else:
            print(f"\n New PRIMARY elected: {primary}")
    except PyMongoError as e:
        print(f"Error retrieving status: {e}")
        print("Retrying in 3 seconds...\n")
        time.sleep(3)

if not primary:
    print("Timeout: No PRIMARY elected within 60 seconds.")

# ---------------------------------------------------------------------
# 6. Insert new data after failover
# ---------------------------------------------------------------------
print("\n[Step 6] Inserting one new document after failover (w='majority')...")

# Find the last inserted user_id (numerically largest)
last_doc = coll_base.find_one(sort=[("user_id", -1)])
if last_doc:
    # Extract the numeric part and increment
    last_num = int(last_doc["user_id"].lstrip("u"))
    next_num = last_num + 1
else:
    # If collection is empty, start from 1
    next_num = 1

user_id = f"u{next_num:03}"

try:
    # Insert a new document with replication metadata
    coll_base.insert_one({
        "_id": user_id,
        "user_id": user_id,
        "username": f"user_{next_num}",
        "email": f"user_{next_num}@example.com",
        "inserted_after_failover": True,
        "last_login_time": time.time()
    })
    print(f"Inserted new document: {user_id}")

except Exception as e:
    print(f"Error inserting {user_id}: {e}")

# Display all documents for verification
print("\nAll documents in userProfile after post-failover insertion:")
pprint.pp(list(coll_base.find({}, {"_id": 0})))

# ---------------------------------------------------------------------
# 7. Restart old primary
# ---------------------------------------------------------------------
print(f"\n[Step 7] Restoring the old PRIMARY node ({old_primary})...")

print("Please run the following command in another terminal to start the stopped container:\n")
print(f"docker start {old_primary}\n")

# If running in interactive mode, wait for manual confirmation
if sys.stdin.isatty():
    # User is running this script manually (attached to a terminal)
    input(f"Press Enter once you have started {old_primary}... ")
else:
    # Non-interactive mode (e.g., running inside Docker without TTY)
    print("Non-interactive mode detected — waiting 15 seconds before continuing automatically...\n")
    time.sleep(15)

# Wait for the node to rejoin and sync with the replica set
print("Waiting 8 seconds for the node to fully recover and sync with the replica set...\n")
time.sleep(8)

# ---------------------------------------------------------------------
# 8. Verify data consistency
# ---------------------------------------------------------------------
print("\n[Step 8] Verifying data consistency across replica set...")

# check the current primary
ismaster = client.admin.command("isMaster")
print(f"\nCurrent primary node: {ismaster['primary']}")

# read data from primary
docs_primary = list(coll_base.find({}, {"_id": 0}))
print(f"\nDocuments from PRIMARY ({ismaster['primary']}):")
pprint.pp(docs_primary)

# read data from secondary
coll_secondary = coll_base.with_options(read_preference=ReadPreference.SECONDARY)
docs_secondary = list(coll_secondary.find({}, {"_id": 0}))
print(f"\nDocuments from SECONDARY (sample read):")
pprint.pp(docs_secondary)

print("\nReplication experiment completed successfully!")
print("All nodes should now be in sync with identical data.\n")
