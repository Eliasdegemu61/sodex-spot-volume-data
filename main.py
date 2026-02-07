import time # Make sure this is at the top

def run():
    # Load past state
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            state = json.load(f)
    else:
        state = {}

    # 1. Fetch Registry
    try:
        users = requests.get(REGISTRY_URL).json()
    except:
        print("Could not load registry. Exiting.")
        return
    
    for user in users:
        u_id = user['userId']
        addr = user['address']
        
        user_data = state.get(addr, {"userId": u_id, "vol": 0.0, "last_ts": 0})
        last_checkpoint = user_data['last_ts']
        
        # --- NEW ERROR HANDLING BLOCK ---
        try:
            response = requests.get(f"{BASE_URL}?account_id={u_id}&limit=1000", timeout=10)
            if response.status_code != 200:
                print(f"Skipping {u_id}: Server returned status {response.status_code}")
                continue
            res = response.json()
        except Exception as e:
            print(f"Skipping {u_id}: Request failed (likely rate limit or timeout).")
            # Save progress so far before moving to next run
            with open(DATA_FILE, 'w') as f:
                json.dump(state, f, indent=2)
            continue
        # --------------------------------
            
        trades = res.get('data', [])
        if not trades:
            continue
            
        newest_ts = trades[0]['ts_ms']
        if newest_ts <= last_checkpoint:
            print(f"Skipping User {u_id}: No new trades.")
            continue

        new_vol = 0.0
        current_cursor = res.get('meta', {}).get('next_cursor')
        
        # Calculate first page
        for t in trades:
            if t['ts_ms'] <= last_checkpoint: break
            new_vol += float(t['price'] or 0) * float(t['quantity'] or 0)
            
        # Paginate
        while current_cursor and trades[-1]['ts_ms'] > last_checkpoint:
            try:
                # Small sleep to be nice to the API during pagination
                time.sleep(0.5) 
                p_res = requests.get(f"{BASE_URL}?account_id={u_id}&limit=1000&cursor={current_cursor}", timeout=10).json()
                trades = p_res.get('data', [])
                if not trades: break
                
                for t in trades:
                    if t['ts_ms'] <= last_checkpoint: break
                    new_vol += float(t['price'] or 0) * float(t['quantity'] or 0)
                
                current_cursor = p_res.get('meta', {}).get('next_cursor')
            except:
                break # Stop paginating this user if API fails

        user_data['vol'] += new_vol
        user_data['last_ts'] = newest_ts
        state[addr] = user_data
        print(f"Updated User {u_id}: +{new_vol:.4f} Vol")
        
        # --- BE NICE TO THE SERVER ---
        time.sleep(0.5) # Wait 0.5 seconds between users

    # Final Save
    with open(DATA_FILE, 'w') as f:
        json.dump(state, f, indent=2)
