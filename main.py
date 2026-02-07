import requests
import json
import os

# Files & URLs
REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/main/registry.json"
BASE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
DATA_FILE = "spot_vol_data.json"

def run():
    # Load past state
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            state = json.load(f)
    else:
        state = {}

    # 1. Fetch Registry (One time per run)
    users = requests.get(REGISTRY_URL).json()
    
    for user in users:
        u_id = user['userId']
        addr = user['address']
        
        # Get existing checkpoint or start at 0
        user_data = state.get(addr, {"userId": u_id, "vol": 0.0, "last_ts": 0})
        last_checkpoint = user_data['last_ts']
        
        # 2. Fetch Latest Page
        res = requests.get(f"{BASE_URL}?account_id={u_id}&limit=1000").json()
        trades = res.get('data', [])
        
        if not trades:
            continue
            
        # Optimization: Check newest trade timestamp
        newest_ts = trades[0]['ts_ms']
        if newest_ts <= last_checkpoint:
            print(f"Skipping User {u_id}: No new trades.")
            continue

        # 3. Process Trades (Incremental Update)
        new_vol = 0.0
        current_cursor = res.get('meta', {}).get('next_cursor')
        
        # Calculate first page
        for t in trades:
            if t['ts_ms'] <= last_checkpoint: break
            new_vol += float(t['price']) * float(t['quantity'])
            
        # Paginate if necessary (only if the user traded > 1000 times since last run)
        while current_cursor and trades[-1]['ts_ms'] > last_checkpoint:
            res = requests.get(f"{BASE_URL}?account_id={u_id}&limit=1000&cursor={current_cursor}").json()
            trades = res.get('data', [])
            if not trades: break
            
            for t in trades:
                if t['ts_ms'] <= last_checkpoint: break
                new_vol += float(t['price']) * float(t['quantity'])
            
            current_cursor = res.get('meta', {}).get('next_cursor')

        # 4. Save progress
        user_data['vol'] += new_vol
        user_data['last_ts'] = newest_ts
        state[addr] = user_data
        print(f"Updated User {u_id}: +{new_vol:.4f} Vol")

    # Final Save to Repo
    with open(DATA_FILE, 'w') as f:
        json.dump(state, f, indent=2)

if __name__ == "__main__":
    run()
