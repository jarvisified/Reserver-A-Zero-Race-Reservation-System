from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import asyncpg
import asyncio
import logging
import redis.asyncio as redis
import uuid

#setting up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title = "Reserver: Dynamic Redis Shield API")

#database connection details
DB_URL = "postgresql://postgres:postgres@localhost:5432/postgres"

#global connection
db_pool = None
redis_client = None
worker_task = None

# Create slot
class SlotCreate(BaseModel):
    slot_id: str
    capacity: int

@app.on_event("startup")
async def startup():
    global db_pool, redis_client, worker_task
    #create an async connection pool to PostgreSQL and Redis
    db_pool = await asyncpg.create_pool(DB_URL)
    redis_client = redis.Redis(host = 'localhost', port=6379, db=0, decode_responses=True)

    await setup_database()
    
    # start the background worker daemon
    worker_task = asyncio.create_task(waitlist_worker())

    logger.info("Reserver Naive Server Running!")
    logger.info("Redis Mutex Shield is Active!")
    logger.info("Background Waitlist Worker is monitoring the queue...")

@app.on_event("shutdown")
async def shutdown():
    if worker_task:
        worker_task.cancel()
    await db_pool.close()
    await redis_client.aclose()

async def setup_database():
    # Clear all Redis state on startup
    await redis_client.flushdb() # Clears everything in the current Redis DB

    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS interview_slots")
        await conn.execute("""
            CREATE TABLE interview_slots (
                slot_id VARCHAR(50) PRIMARY KEY,
                available_capacity INT
            )
        """)
        logger.info("Clean database initialized. Ready for dynamic slots")

@app.post("/admin/slots", status_code=status.HTTP_201_CREATED)
async def create_slot(slot: SlotCreate):
    # Create a booking event
    async with db_pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO interview_slots (slot_id, available_capacity) VALUES ($1, $2)",
                slot.slot_id, slot.capacity
            )
            logger.info(f"Event Created: '{slot.slot_id}' with {slot.capacity} slots.")
            return {"message": f"Successfully created {slot.slot_id} with {slot.capacity} slots."}
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=400, detail="Slot ID already exists.")

async def waitlist_worker():
    # Daemon process that continuously monitors the waitlist and database
    while True:
        await asyncio.sleep(2)
        try:
            async for waitlist_key in redis_client.scan_iter("waitlist*"):
                slot_id = waitlist_key.split(":")[1] # Extract the dynamic slot name

                # stop processing if terminal is closed
                terminal_status = await redis_client.get(f"status:{slot_id}")
                if terminal_status == "closed":
                    continue

                # we have people in the line
                lock_key = f"lock:{slot_id}"
                acquired = await redis_client.set(lock_key, "background_worker", nx=True, px=5000)

                if acquired:
                    try:
                        async with db_pool.acquire() as conn:
                            row = await conn.fetchrow("SELECT available_capacity FROM interview_slots WHERE slot_id = $1", slot_id)
                            
                            if row and row['available_capacity'] > 0:
                                # we have capacity
                                promoted_request_id = await redis_client.lpop(waitlist_key)
                                
                                if promoted_request_id:
                                    #decrement capacity
                                    await conn.execute("UPDATE interview_slots SET available_capacity = available_capacity - 1 WHERE slot_id = $1", slot_id)
                                    
                                    # Mark them as promoted in Redis so the Status endpoint knows!
                                    await redis_client.set(f"promoted:{promoted_request_id}", "true")
                                    logger.info(f"WORKER PROMOTED {promoted_request_id[:8]} from Waitlist to Booked!")
                    finally:
                        # Release the lock so normal users can book again
                        await redis_client.delete(lock_key)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Worker error: {e}")

@app.post("/cancel/{slot_id}")
async def cancel_booking(slot_id: str):
    # user cancel there booking
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE interview_slots SET available_capacity = available_capacity + 1 WHERE slot_id = $1", slot_id)
        logger.warning("Someone cancelled! 1 slot has been freed up")
        return {"message": f"Booking canceled for {slot_id}. Slot freed."}

@app.post("/close/{slot_id}")
async def close_terminal(slot_id: str):
    # Admin endpoint to close the booking window and reject the waitlist
    await redis_client.set(f"status:{slot_id}", "closed")
    # waitlist -> rejected
    waitlist_exists = await redis_client.exists(f"waitlist:{slot_id}")
    if waitlist_exists:
        await redis_client.rename(f"waitlist:{slot_id}", f"rejected:{slot_id}")
    
    logger.warning("TERMINAL CLOSED. Waitlist has been rejected.")
    return {"message": f"Terminal {slot_id} closed."}


@app.post("/book/{slot_id}")
async def book_slot(slot_id: str):

    terminal_status = await redis_client.get(f"status:{slot_id}")
    if terminal_status == "closed":
        logger.warning("Booking blocked: Terminal is closed.")
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="The booking terminal is closed. No new bookings accepted."
        )

    # Generate a unique ID
    request_id = str(uuid.uuid4())
    lock_key = f"lock:{slot_id}"

    # Try to acquire the redis lock
    # nx=True: Only set the key if not exists
    #px=5000: auto-expire after 5 sec to prevent deadlocks

    acquired = await redis_client.set(lock_key, request_id, nx=True, px=5000)
    if not acquired:
        # we push the user to a redis list hence, waitlisting the user
        waitlist_key = f"waitlist:{slot_id}" 

        # RPUSH (Right Push) adds the user to the back of the queue
        await redis_client.rpush(waitlist_key, request_id)
        logger.info(f"Request {request_id[:8]} routed to waitlist")

        # 202 Accepted means "We got your request, but haven't finished processing it"
        return JSONResponse(
            status_code = status.HTTP_202_ACCEPTED,
            content = {
                "message": "Slot is busy. You have been placed on the waitlist.",
                "request_id": request_id,
                "status": "waitlisted"
            }
        )
    logger.info(f"Lock acquired by request {request_id[:8]}")
        
    try:
        async with db_pool.acquire() as conn:
            #check capacity
            row = await conn.fetchrow(
                "SELECT available_capacity FROM interview_slots WHERE slot_id = $1", slot_id
            )

            if not row:
                raise HTTPException(status_code=500, detail="Slot not found")
            
            capacity = row['available_capacity']

            #evalute is there a room
            if capacity > 0:
                #Race condition
                await asyncio.sleep(0.05)

                #subtract 1 from capacity
                await conn.execute(
                    "UPDATE interview_slots SET available_capacity = available_capacity - 1 WHERE slot_id = $1", slot_id
                )
                logger.info(f"{slot_id} Booked by {request_id[:8]}")
                return {"message": "Success! You booked the slot.", "request_id": request_id}
            else:
                waitlist_key = f"waitlist:{slot_id}"
                await redis_client.rpush(waitlist_key, request_id)
                logger.info(f"Request {request_id[:8]} routed to waitlist (Slot full in DB)")
                
                return JSONResponse(
                    status_code=status.HTTP_202_ACCEPTED,
                    content={
                        "message": "Slot is full. You have been placed on the waitlist.",
                        "request_id": request_id,
                        "status": "waitlisted"
                    }
                )
    finally:
        #Release the lock using lua script which ensure we delete the lock if we own it
        lua_script="""
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        await redis_client.eval(lua_script, 1, lock_key, request_id)
        logger.info(f"Lock released by request {request_id[:8]}")

@app.get("/status/{slot_id}/{request_id}")
async def check_status(slot_id: str, request_id: str):
    # Allows user to check there exact position
    waitlist_key = f"waitlist:{slot_id}"
    rejected_key = f"rejected:{slot_id}"

    # Check if the background worker promoted them!
    if await redis_client.get(f"promoted:{request_id}"):
        return {"status": "Booked (Promoted from Waitlist)"}

    # LPOS (Left Position) returns the 0-indexed position, or None if not found
    position = await redis_client.lpos(f"waitlist:{slot_id}", request_id)
    if position is not None:
        return {"status": "Waitlisted", "position_in_line": position + 1}
    
    if await redis_client.lpos(f"rejected:{slot_id}", request_id) is not None:
        return {"status": "Rejected"}
    
    return {"status": "Booked or Invalid"}