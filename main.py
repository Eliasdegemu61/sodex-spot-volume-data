import requests
import json
import os
import time

# Configuration
REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/main/registry.json"
BASE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
DATA_FILE = "spot_vol_data.json"

def save_to_file(data):
    """Helper to write current state to JSON file."""
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def run():
    # 1. Ensure the data file exists immediately to prevent Git errors
    if not os.path.exists(DATA_FILE):
        print("Creating new state file...")
        save_to_file({})
    
    with open(DATA_FILE, 'r') as f:
        try:
            state = json.load(f)
        except json.JSONDecodeError:
            state = {}

    # 2. Fetch Registry (Once per run)
    print(f"Fetching registry from {REGISTRY_URL}...")
    try:
        registry_res = requests.get(REGISTRY_URL, timeout=15)
        registry_res.raise_for_status()
        users = registry_res.json()
    except Exception as e:
        print(f"Failed to fetch registry: {e}")
        return

    # 3. Process Users
    counter = 0
    for user in users:
        u_id = user.get('userId')
        addr = user.get('address')
        
        if not u_id or not addr:
            continue

        # Get existing data or initialize
        user_data = state.get(addr, {"userId": u_id, "vol": 0.0, "last_ts": 0})
        last_checkpoint = user_data['last_ts']
        
        try:
            # First API call for the user
            response = requests.get(f"{BASE_URL}?account_id={u_id}&limit=1000", timeout=15)
            
            # Handle rate limiting (429) or server errors
            if response.status_code != 200:
                print(f"Skipping {u_id}: Status {response.status_code}")
                continue
                
            res_json = response.json()
            trades = res_json.get('data', [])
            
            if not trades:
                continue

            # OPTIMIZATION: Check if newest trade is same as last run
            newest_ts = trades[0]['ts_ms']
            if newest_ts <= last_checkpoint:
                print(f"User {u_id}: No new trades (Skipped)")
                continue

            # Process valid trades
            new_vol = 0.0
            current_cursor = res_json.get('meta', {}).get('next_cursor')

            # Page 1 logic
            for t in trades:
                if t['ts_ms'] <= last_checkpoint:
                    break
                new_vol += float(t.get('price', 0)) * float(t.get('quantity', 0))

            # Pagination logic (if more than 1000 new trades)
            while current_cursor and trades[-1]['ts_ms'] > last_checkpoint:
                time.sleep(0.3) # Small delay for pagination
                p_resp = requests.get(f"{BASE_URL}?account_id={u_id}&limit=1000&cursor={current_cursor}", timeout=15)
                if p_resp.status_code != 200: break
                
                p_data = p_resp.json()
                trades = p_data.get('data', [])
                if not trades: break
                
                for t in trades:
                    if t['ts_ms'] <= last_checkpoint:
                        break
                    new_vol += float(t.get('price', 0)) * float(t.get('quantity', 0))
                
                current_cursor = p_data.get('meta', {}).get('next_cursor')

            # Update state
            user_data['vol'] += new_vol
            user_data['last_ts'] = newest_ts
            state[addr] = user_data
            print(f"Updated User {u_id}: +{new_vol:,.2f} Vol (Total: {user_data['vol']:,.2f})")

        except Exception as e:
            print(f"Error processing user {u_id}: {e}")
            continue

        # Save progress every 50 users so we don't lose work on crash
        counter += 1
        if counter % 50 == 0:
            save_to_file(state)
            print("--- Checkpoint: Progress Saved ---")

        # Gentle delay between different users
        time.sleep(0.5)

    # Final Save
    save_to_file(state)
    print("Run completed successfully.")

if __name__ == "__main__":
    run()
