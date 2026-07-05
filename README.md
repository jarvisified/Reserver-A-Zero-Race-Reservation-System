# Reserver: A Zero Race Reservation System
Distributed Booking Engine & Async Waitlist Orchestrator

## 🚀 Overview

A high-concurrency distributed system designed to handle massive traffic spikes during limited-inventory booking events (e.g., campus placements, flash sales). This engine eliminates database race conditions and double-booking using a distributed Redis Mutex, while ensuring a seamless user experience by routing overflow traffic into an asynchronous message queue for automated waitlist resolution.

## 🏗️ System Architecture

### 1. The Concurrency Shield (Redis Distributed Lock)

When 1,000 requests arrive at the exact same millisecond to book 1 slot, the API requires the thread to acquire a cryptographic lock via Redis (`SET NX PX`).

* Exactly **one** thread acquires the lock and safely updates the PostgreSQL database.

* The remaining 999 threads are instantly rejected at the cache layer, protecting the primary database from CPU exhaustion and race-condition data corruption.

### 2. The Asynchronous Waitlist (Message Broker)

Instead of outright failing the 999 rejected requests, the API seamlessly pushes them into an asynchronous Message Queue (Redis Lists/Streams) and returns a `202 Accepted (Waitlisted)` status.

* Users are placed in a strict FIFO (First-In, First-Out) queue.

* **Background Worker Pool:** A separate daemon process continuously monitors the queue. If a user cancels their booking, the worker instantly pops the next user off the waitlist and automatically executes the database booking transaction in the background.

## 💻 Tech Stack

* **Backend API:** Python (FastAPI)
* **Primary Database:** PostgreSQL (ACID compliant, Row-Level Locking)
* **Distributed Lock & Message Broker:** Redis
* **Simulation/Load Testing:** Python (`asyncio`, `aiohttp`)

## 🛣️ API Endpoints

* `POST /book` - Attempts to acquire the lock and book. If full/busy, pushes to waitlist queue.
* `POST /cancel` - Cancels an active booking, triggering the background worker to pop from the waitlist.
* `GET /status/{user_id}` - Checks if a user is BOOKED or WAITLISTED.
