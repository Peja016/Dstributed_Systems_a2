# Distributed Systems Lab 2 – MongoDB Replica Set & Consistency Experiments

### **Author:** Hsuan-Yu Tan

### **Course:** Distributed Systems – Lab 2

### **Topic:** MongoDB Replication, Consistency, and Distributed Transactions

---

## **Overview**

This lab explores key distributed database concepts using a **MongoDB replica set** deployed in Docker.  
The objectives are to:

- Configure and analyze a replicated MongoDB cluster.
- Observe **replication**, **failover**, and **consistency trade-offs**.
- Experimentally demonstrate **strong**, **eventual**, and **causal** consistency.
- Conceptually analyze **distributed transactions** via **ACID** and **Saga patterns**.

---

## **Repository Structure**

```
distributed-systems-a2/
│
├── docker-compose.yml                  # 3-node MongoDB replica set definition
│
├── setup_baseline.py                   # Inserts baseline test data (Part A)
├── replication_experiment.py           # Demonstrates replication factor & failover (Part B)
│
├── strong_consistency_experiment.py    # Demonstrates strong consistency (Part C.1)
├── eventual_consistency_experiment.py  # Demonstrates eventual consistency (Part C.2)
├── consistency_causal.py               # Demonstrates causal consistency (Part C.3, optional)
│
└── A2_report.pdf                       # Full report and analysis (this document)
```

---

## **Environment Setup**

### **1. Launch MongoDB Replica Set**

```bash
docker compose up -d
```

Check container status:

```bash
docker ps
```

### **2. Initialize Replica Set**

```bash
docker exec -it mongo1 mongosh --eval '
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017" },
    { _id: 1, host: "mongo2:27017" },
    { _id: 2, host: "mongo3:27017" }
  ]
})'
```

### **3. Verify Cluster Status**

```bash
docker exec -it mongo1 mongosh --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr}))"
```

---

## **Part A – Setup & Baseline**

**Goal:**  
Initialize a 3-node MongoDB cluster (`mongo1`, `mongo2`, `mongo3`) and insert baseline user data.

**Run:**

```bash
docker run -it --rm   -v $(pwd):/app   -w /app   --network distributed-systems-a2_mongo-net   python:3.11   bash -c "pip install pymongo && python setup_baseline.py"
```

**Result:**  
Initial dataset (`u001–u005`) successfully replicated across all nodes.

---

## **Part B – Replication & Failover**

**Experiment:**  
`replication_experiment.py` tests how write concerns (`w=1`, `w='majority'`, `w=3`) affect latency and durability, and simulates primary failover.

**Key Results:**
| Write Concern | Average Latency | Behavior |
|----------------|----------------|-----------|
| `w=1` | ~0.9 ms | Fastest, primary-only acknowledgment |
| `w='majority'` | ~2.6 ms | Requires 2 of 3 nodes to confirm |
| `w=3` | ~2.9 ms | Waits for all nodes, highest durability |

**Failover Test:**  
When `mongo1` (primary) was stopped, the cluster automatically re-elected `mongo2` as the new primary.  
After restarting `mongo1`, it rejoined as a synchronized secondary.

**Conclusion:**  
MongoDB’s replication follows the **Leader–Follower (Primary–Backup)** model — ensuring strong consistency, high availability, and durability.

---

## **Part C – Consistency Models**

### **1 Strong Consistency (`strong_consistency_experiment.py`)**

- **Config:** `WriteConcern('majority')`, `ReadConcern('majority')`
- **Behavior:** Writes confirmed by multiple nodes; reads always return the latest committed value.
- **CAP Trade-off:** **Consistency > Availability (C > A)**
- **Observation:** During failover, writes were temporarily blocked until re-election completed, preserving global consistency.

---

### **2 Eventual Consistency (`eventual_consistency_experiment.py`)**

- **Config:** `WriteConcern(1)`, `ReadConcern('local')`
- **Behavior:** Writes acknowledged by the primary only. Secondary initially returned `None`, but after ~1 s it caught up.
- **CAP Trade-off:** **Availability > Consistency (A > C)**
- **Use Cases:** Social media likes, IoT sensor readings, analytics logs.

---

### **3 Causal Consistency (`consistency_causal.py`)**

- **Config:** Session-based causal consistency (`start_session(causal_consistency=True)`).
- **Behavior:** “Hello world” (Client A) always appeared before “Nice post!” (Client B).
- **Insight:** Causally related events preserve order, while unrelated ones can execute concurrently.
- **Balance:** Provides a middle ground between strong and eventual consistency.

---

## **Part D – Distributed Transactions (Conceptual)**

### **Scenario:**

E-commerce workflow with `OrderService`, `PaymentService`, `InventoryService`.

### **Approach Comparison:**

| Aspect          | ACID Transactions       | Saga Pattern                |
| --------------- | ----------------------- | --------------------------- |
| Consistency     | Strong, atomic          | Eventual or causal          |
| Fault Tolerance | Low (blocking)          | High (compensating actions) |
| Complexity      | Centralized             | Distributed event-driven    |
| Performance     | Slow (two-phase commit) | Fast (asynchronous)         |
| Scalability     | Limited                 | High                        |
| Use Case        | Monolithic DB           | Microservices               |

**Conclusion:**  
ACID ensures strict consistency but fails to scale in distributed systems.  
Saga workflows achieve **eventual consistency**, improving **fault tolerance** and **availability**, aligning with real-world CAP trade-offs.

---

## **Summary**

This lab demonstrated:

- MongoDB’s **replica set** enables automatic failover and high availability.
- **Write concern** directly impacts replication latency and durability.
- **Consistency levels** (strong, eventual, causal) illustrate different CAP trade-offs.
- **Saga patterns** provide practical solutions for distributed transactions.

Overall, MongoDB’s architecture exemplifies **Consistency–Availability trade-offs** in distributed database design, aligning theoretical principles (CAP theorem) with real-world system behavior.
