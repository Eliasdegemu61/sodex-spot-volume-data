import requests
import json
import os
import time
from datetime import datetime

# Configuration
REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/main/registry.json"
BASE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
DATA_FILE = "spot_vol_data.json"
DAILY_FOLDER = "daily_stats"

def save_json(filepath, data):
    """Helper to write data to a JSON file."""
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def run():
    # 1. Setup Folders and Dates
    if not os.path.exists(DAILY_FOLDER):
        os.makedirs(DAILY_FOLDER)
    
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    daily_file_path = os.path.join(DAILY_FOLDER, f"{today_str}.json")

    # 2. Load Main State (Cumulative)
    if not os.path.exists(DATA_FILE):
        save_json(DATA_FILE, {})
    
    with open(DATA_FILE, 'r') as f:
        try:
            state = json.load(f)
        except json.JSONDecodeError:
            state = {}

    # 3. Load or Initialize Today's Specific File
    if os.path.exists(daily_file_path):
        with open(daily_file_path, 'r') as f:
            today_data = json.load(f)
    else:
        today_data = {}

    # 4. Fetch Registry
    print(f"Fetching registry...")
    try:
        registry_res = requests.get(REGISTRY_URL, timeout=15)
        registry_res.raise_for_status()
        users = registry_res.json()
    except Exception as e:
        print(f"Failed to fetch registry: {e}")
        return

    # 5. Process Users
    counter = 0
    for user in users:
        u_id = str(user.get('userId'))
        addr = user.get('address')
        
        if not u_id or not addr:
            continue

        # Get cumulative data
        user_total_data = state.get(addr, {"userId": u_id, "vol": 0.0, "last_ts": 0})
        last_checkpoint = user_total_data['last_ts']
        
        try:
            response = requests.get(f"{BASE_URL}?account_id={u_id}&limit=1000", timeout=15)
            if response.status_code != 200:
                continue
                
            res_json = response.json()
            trades = res_json.get('data', [])
            
            if not trades:
                continue

            newest_ts = trades[0]['ts_ms']
            if newest_ts <= last_checkpoint:
                # Even if no new trades, ensure the user exists in today's file with current total
                if addr not in today_data:
                    today_data[addr] = {
                        "total_volume": user_total_data['vol'],
                        "today_added_volume": 0.0
                    }
                print(f"User {u_id}: No new trades")
                continue

            # Calculate new volume
            new_vol = 0.0
            current_cursor = res_json.get('meta', {}).get('next_cursor')

            for t in trades:
                if t['ts_ms'] <= last_checkpoint: break
                new_vol += float(t.get('price', 0)) * float(t.get('quantity', 0))

            while current_cursor and trades[-1]['ts_ms'] > last_checkpoint:
                time.sleep(0.3)
                p_resp = requests.get(f"{BASE_URL}?account_id={u_id}&limit=1000&cursor={current_cursor}", timeout=15)
                if p_resp.status_code != 200: break
                p_data = p_resp.json()
                trades = p_data.get('data', [])
                if not trades: break
                for t in trades:
                    if t['ts_ms'] <= last_checkpoint: break
                    new_vol += float(t.get('price', 0)) * float(t.get('quantity', 0))
                current_cursor = p_data.get('meta', {}).get('next_cursor')

            # --- Update Logic ---
            # Update cumulative state
            user_total_data['vol'] += new_vol
            user_total_data['last_ts'] = newest_ts
            state[addr] = user_total_data

            # Update today's specific file
            if addr not in today_data:
                today_data[addr] = {"total_volume": 0.0, "today_added_volume": 0.0}
            
            today_data[addr]["total_volume"] = user_total_data['vol']
            today_data[addr]["today_added_volume"] += new_vol

            print(f"Updated {u_id}: +{new_vol:,.2f} | Total: {user_total_data['vol']:,.2f}")

        except Exception as e:
            print(f"Error user {u_id}: {e}")
            continue

        counter += 1
        if counter % 50 == 0:
            save_json(DATA_FILE, state)
            save_json(daily_file_path, today_data)

        time.sleep(0.5)

    # Final Save
    save_json(DATA_FILE, state)
    save_json(daily_file_path, today_data)
    print(f"Run completed. File saved: {daily_file_path}")

if __name__ == "__main__":
    run()
