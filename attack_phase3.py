import asyncio
import aiohttp
import time
import argparse

async def make_request(session, url, req_id):
    try:
        async with session.post(url) as response:
            status = response.status
            try:
                data = await response.json()
                server_req_id = data.get("request_id", "Unknown")
            except:
                server_req_id = "Unknown"
            return req_id, status, server_req_id
    except Exception as e:
        return req_id, 500, str(e)

async def main(requests_count, slot_id):
    url = f"http://localhost:8000/book/{slot_id}"
    successes = 0 
    waitlisted = 0
    failures = 0 
    sample_waitlisted_id = None

    # We use aiohttp.ClientSession to manage connections efficiently
    async with aiohttp.ClientSession() as session:
        tasks = [make_request(session, url, i) for i in range (requests_count)]

        print(f"Firing {requests_count} concurrent requests at the exact same time...")
        start_time = time.time()
        results = await asyncio.gather(*tasks)

        for req_id, status, server_req_id in results:
            if status == 200:
                successes += 1
            elif status == 202:
                waitlisted += 1
                # Save one of the Waitlist IDs
                if sample_waitlisted_id is None:
                    sample_waitlisted_id = server_req_id
            else:
                failures += 1
        
        print(f"\nATTACK RESULTS")
        print(f"Total Time: {time.time() - start_time:.2f} seconds")
        print(f"Successful Bookings (HTTP 200): {successes}")
        print(f"Waitlisted (HTTP 202): {waitlisted}")
        print(f"Rejected Bookings (HTTP 409/500): {failures}")
        
        if sample_waitlisted_id:
            print(f"\nRun this exact command to check your waitlist position:")
            print(f"curl http://localhost:8000/status/{slot_id}/{sample_waitlisted_id}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Test the Reserver API")
    # Dynamic arguments passed via terminal
    parser.add_argument("--requests", type=int, default=100, help="Number of concurrent requests")
    parser.add_argument("--slot", type=str, default="Spotify_Interview", help="The Slot ID to attack")
    
    args = parser.parse_args()
    asyncio.run(main(args.requests, args.slot))