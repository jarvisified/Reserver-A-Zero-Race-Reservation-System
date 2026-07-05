# 🚀 Reserver: Distributed Booking Engine & Async Waitlist

## 📖 Overview
**Reserver** is a highly concurrent, FAANG-grade distributed backend designed to handle massive traffic spikes during limited-inventory events (e.g., Spotify Exclusive Ticket Drops, Campus Placements, Flash Sales). 

When thousands of users attempt to book a single slot at the exact same millisecond, traditional databases crash or corrupt data (Race Conditions). Reserver solves this by implementing a **Distributed Concurrency Shield (Redis Mutex)**, routing overflow traffic into an **Asynchronous Message Broker**, and automatically promoting users via a **Background Worker Daemon**.

## 🏗️ System Architecture

### 1. The Concurrency Shield (Redis Distributed Lock)
Utilizes the `SET NX PX` algorithm to ensure strict mutual exclusion. Only one thread can query or update the PostgreSQL database at a time. This completely eliminates double-bookings and race conditions.

### 2. The Asynchronous Waitlist (Message Broker)
Instead of dropping rejected requests with a harsh `409 Conflict` error, the API gracefully routes overflow traffic to a Redis List (`RPUSH`) in a fraction of a millisecond, returning a `202 Accepted`. This provides a seamless user experience.

### 3. Background Worker Daemon
A continuous `asyncio` task monitors the database and waitlists. If a user cancels their booking, the worker pops (`LPOP`) the next user in line (strict FIFO) and automatically executes the transaction in the background, notifying them of their promotion.

### 4. Circuit Breaker & State Manager
Admins can shut down an event in real-time. Using the Redis `RENAME` command (an $O(1)$ operation), the entire waitlist is instantly migrated to a `rejected` state, blocking new traffic instantly with an `HTTP 410 Gone`.

## 💻 Tech Stack
* **Backend Framework:** Python (FastAPI)
* **Primary Database:** PostgreSQL (ACID-compliant storage via `asyncpg`)
* **In-Memory Datastore:** Redis (Locking, Queueing, and State via `redis.asyncio`)
* **Load Testing:** Python (`asyncio`, `aiohttp`, `argparse`)
* **Infrastructure:** Docker (Containerized DB and Cache)

---

## 🛠️ Installation & Setup

### 1. Prerequisites
* **Python 3.9+**
* **Docker Desktop** (To run PostgreSQL and Redis locally)

### 2. Clone the Repository
```bash
git clone [https://github.com/yourusername/reserver-distributed-engine.git](https://github.com/yourusername/reserver-distributed-engine.git)
cd reserver-distributed-engine
```

### 3. Spin up Docker Containers
You need PostgreSQL and Redis running in the background to handle the state and storage.
```bash
# Start PostgreSQL
docker run --name surgeslot-db -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres

# Start Redis
docker run --name reserver-redis -p 6379:6379 -d redis
```

### 4. Install Dependencies
Create a virtual environment and install the required packages:
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Mac/Linux
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install fastapi "uvicorn[standard]" asyncpg redis aiohttp pydantic
```

---

## 🚀 How to Run & Test (The Ultimate Demo)

This project includes a dynamic load generator (`attack_phase3.py`) to simulate a massive traffic spike for a **Spotify Exclusive Ticket Drop**.

### Step 1: Start the Server
In your first terminal, boot up the FastAPI server:
```bash
uvicorn main_phase7:app --reload
```
*You should see logs indicating the database is seeded and the Background Worker is monitoring the queue.*

### Step 2: Create a Dynamic Event
In a second terminal, act as the Admin and create an event with exactly **5 available tickets**:
```bash
curl -X POST "http://localhost:8000/admin/slots" -H "Content-Type: application/json" -d "{\"slot_id\": \"Spotify_Exclusive_Drop\", \"capacity\": 5}"
```

### Step 3: Unleash the Attack (Load Test)
Simulate **1,000 concurrent fans** trying to grab those 5 tickets at the exact same millisecond:
```bash
python attack_phase3.py --requests 1000 --slot Spotify_Exclusive_Drop
```
*Observe the terminal output: Exactly 5 successes, 995 safely waitlisted, and 0 rejected!*

### Step 4: Check Waitlist Status
The attack script will dynamically output a custom `curl` command at the bottom for a waitlisted user. Copy and run it:
```bash
curl http://localhost:8000/status/Spotify_Exclusive_Drop/<INSERT_REQUEST_ID>
```
*It will return: `{"status": "Waitlisted", "position_in_line": 1}`*

### Step 5: Trigger the Background Worker (Cancellation)
Simulate a user canceling their ticket to see the background daemon magically promote someone from the waitlist:
```bash
curl -X POST http://localhost:8000/cancel/Spotify_Exclusive_Drop
```
*Look at your server logs! Within 2 seconds, the worker spots the freed capacity and logs `🎉 WORKER PROMOTED`.* *Run your status check `curl` command from Step 4 again—the status will have changed to **Booked (Promoted from Waitlist)**!*

### Step 6: Close the Event (Circuit Breaker)
End the event to instantly mass-reject the remaining waitlist:
```bash
curl -X POST http://localhost:8000/close/Spotify_Exclusive_Drop
```
*Check your status `curl` one last time. It will now dynamically report **Rejected**.*
