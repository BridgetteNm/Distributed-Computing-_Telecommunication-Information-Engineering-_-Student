import multiprocessing
import time
import random
import os
import json
import shutil
import threading
from datetime import datetime
from enum import Enum

# =================CONFIGURATION=================
NUM_NODES = 3
DATA_DIR = "dist_system_data"

class TxState(Enum):
    INIT = "INIT"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"

# =================HELPER FUNCTIONS=================
def setup_environment():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)

def write_log(node_id, tx_id, state, data=None):
    filename = f"{DATA_DIR}/node_{node_id}.log"
    entry = {"tx_id": tx_id, "state": state.value, "data": data, "timestamp": time.time()}
    with open(filename, "a") as f:
        f.write(json.dumps(entry) + "\n")

def read_last_state(node_id, tx_id):
    filename = f"{DATA_DIR}/node_{node_id}.log"
    if not os.path.exists(filename): return None
    last_state = None
    with open(filename, "r") as f:
        for line in f:
            try:
                entry = json.loads(line)
                if entry["tx_id"] == tx_id: last_state = entry
            except: continue
    return last_state

def update_db(node_id, key, value, history_entry=None):
    """Updates balance and appends to history."""
    filename = f"{DATA_DIR}/node_{node_id}_db.json"
    data = {"balance": 0, "history": []}
    
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            try: data = json.load(f)
            except: pass
            
    if key: data[key] = value
    if history_entry: data["history"].append(history_entry)
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def read_db(node_id):
    filename = f"{DATA_DIR}/node_{node_id}_db.json"
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            try: return json.load(f)
            except: pass
    return {"balance": 0, "history": []}

# =================EXTERNAL PAYMENT GATEWAY=================
class MockPaymentGateway:
    """Simulates External Providers (Visa, M-Pesa, PayPal)"""
    
    @staticmethod
    def process_card_payment(card_number, amount):
        print(f"  [Gateway] Verifying Card {card_number[-4:]}...")
        time.sleep(1.0)
        if len(card_number) != 16 or not card_number.isdigit():
            print(f"  [Gateway] ERROR: Invalid Card Number.")
            return False
        if random.random() < 0.1: # 10% decline chance
            print(f"  [Gateway] DECLINED by Issuer.")
            return False
        print(f"  [Gateway] APPROVED: ${amount} captured.")
        return True

    @staticmethod
    def process_withdrawal(method, details, amount):
        print(f"  [Gateway] Initiating transfer to {method} ({details})...")
        time.sleep(1.5)
        
        # Simulate validations
        if method == "M-Pesa" and not details.startswith("07"):
            print("  [Gateway] ERROR: Invalid Phone Number.")
            return False
        if method == "PayPal" and "@" not in details:
            print("  [Gateway] ERROR: Invalid Email.")
            return False
            
        print(f"  [Gateway] SENT: ${amount} transferred successfully.")
        return True

# =================PARTICIPANT NODE=================
class Participant(multiprocessing.Process):
    def __init__(self, node_id, pipe):
        super().__init__()
        self.node_id = node_id
        self.pipe = pipe
        self.is_flaky = False 
        self.locks = {} 
        # Init DB
        update_db(node_id, "balance", 1000)

    def run(self):
        while True:
            try:
                if self.pipe.poll(0.1):
                    msg = self.pipe.recv()
                    command = msg[0]
                    if command == "PREPARE": self.handle_prepare(msg)
                    elif command == "COMMIT": self.handle_commit(msg)
                    elif command == "ABORT": self.handle_abort(msg)
                    elif command == "SET_FAILURE": self.is_flaky = msg[1]
                    elif command == "KILL": break
            except EOFError: break

    def handle_prepare(self, msg):
        _, tx_id, operation, amount, details = msg
        current_bal = read_db(self.node_id)["balance"]
        
        # 1. LOCKING
        if "balance" in self.locks and self.locks["balance"] != tx_id:
            self.pipe.send(("NO", tx_id, "LOCKED"))
            return

        # 2. VALIDATION (Withdrawals)
        if operation == "WITHDRAW" and current_bal < amount:
            self.pipe.send(("NO", tx_id, "INSUFFICIENT_FUNDS"))
            return

        # 3. FAILURE SIMULATION
        if self.is_flaky and random.random() < 0.5:
            time.sleep(5) # Simulate timeout
            return 

        # 4. LOG & VOTE
        self.locks["balance"] = tx_id
        write_log(self.node_id, tx_id, TxState.PREPARED, {"op": operation, "amt": amount, "details": details})
        self.pipe.send(("YES", tx_id, "OK"))

    def handle_commit(self, msg):
        _, tx_id = msg
        log_entry = read_last_state(self.node_id, tx_id)
        
        if log_entry and log_entry["state"] == TxState.PREPARED.value:
            data = log_entry["data"]
            op = data["op"]
            amt = data["amt"]
            details = data["details"]
            
            curr = read_db(self.node_id)["balance"]
            new_bal = curr - amt if op == "WITHDRAW" else curr + amt
            
            # Create History Record
            hist = {
                "tx_id": tx_id,
                "type": op,
                "amount": amt,
                "details": details,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            update_db(self.node_id, "balance", new_bal, hist)
            write_log(self.node_id, tx_id, TxState.COMMITTED)
            
        if self.locks.get("balance") == tx_id: del self.locks["balance"]
        self.pipe.send(("ACK", tx_id))

    def handle_abort(self, msg):
        _, tx_id = msg
        write_log(self.node_id, tx_id, TxState.ABORTED)
        if self.locks.get("balance") == tx_id: del self.locks["balance"]

# =================COORDINATOR=================
def coordinator(client_pipe, node_pipes):
    tx_counter = 0

    while True:
        if client_pipe.poll(0.1):
            req = client_pipe.recv()
            command = req[0]

            if command == "STOP": break
            
            elif command == "SET_FAILURE":
                node_pipes[req[1]].send(("SET_FAILURE", req[2]))

            # --- CASE 1: DEPOSIT (Card -> Node) ---
            elif command == "DEPOSIT":
                _, dst, amt, card = req
                tx_id = f"DEP-{tx_counter}"
                tx_counter += 1
                
                print(f"\n[Coord] Processing Deposit {tx_id}...")
                
                # 1. Authorize External
                if MockPaymentGateway.process_card_payment(card, amt):
                    # 2. Prepare Internal
                    node_pipes[dst].send(("PREPARE", tx_id, "DEPOSIT", amt, f"From Card *{card[-4:]}"))
                    
                    # 3. Wait for Vote
                    vote = wait_for_vote(node_pipes[dst], tx_id)
                    
                    if vote == "YES":
                        node_pipes[dst].send(("COMMIT", tx_id))
                        print(f"[Coord] {tx_id} SUCCESS.")
                    else:
                        print(f"[Coord] {tx_id} FAILED (Internal Logic). Refunding Card.")
                        node_pipes[dst].send(("ABORT", tx_id))
                else:
                    print(f"[Coord] {tx_id} CANCELLED (Bank Declined).")

            # --- CASE 2: WITHDRAWAL (Node -> M-Pesa/PayPal) ---
            elif command == "WITHDRAW":
                _, src, amt, method, details = req
                tx_id = f"WDR-{tx_counter}"
                tx_counter += 1
                
                print(f"\n[Coord] Processing Withdrawal {tx_id} via {method}...")

                # 1. Prepare Internal (Lock funds FIRST)
                node_pipes[src].send(("PREPARE", tx_id, "WITHDRAW", amt, f"To {method}: {details}"))
                
                # 2. Wait for Vote (Do we have money?)
                vote = wait_for_vote(node_pipes[src], tx_id)
                
                if vote == "YES":
                    # 3. Execute External Transfer
                    if MockPaymentGateway.process_withdrawal(method, details, amt):
                        print(f"[Coord] Transfer Complete. Committing DB.")
                        node_pipes[src].send(("COMMIT", tx_id))
                    else:
                        print(f"[Coord] External Gateway Failed. Releasing Lock.")
                        node_pipes[src].send(("ABORT", tx_id))
                else:
                    print(f"[Coord] {tx_id} FAILED (Insufficient Funds/Locked).")
                    node_pipes[src].send(("ABORT", tx_id))

            # --- CASE 3: INTERNAL TRANSFER ---
            elif command == "TX":
                _, src, dst, amt = req
                tx_id = f"TX-{tx_counter}"
                tx_counter += 1
                print(f"[Coord] Internal Transfer {tx_id}: {src}->{dst}")
                
                node_pipes[src].send(("PREPARE", tx_id, "WITHDRAW", amt, f"Transfer to Node {dst}"))
                node_pipes[dst].send(("PREPARE", tx_id, "DEPOSIT", amt, f"Transfer from Node {src}"))
                
                votes = {}
                start = time.time()
                while len(votes) < 2 and time.time() - start < 2.0:
                    for nid in [src, dst]:
                        if nid not in votes and node_pipes[nid].poll():
                            r = node_pipes[nid].recv()
                            if r[1] == tx_id: votes[nid] = r[0]
                
                if len(votes) == 2 and all(v == "YES" for v in votes.values()):
                    for nid in [src, dst]: node_pipes[nid].send(("COMMIT", tx_id))
                    print(f"[Coord] {tx_id} COMMITTED.")
                else:
                    for nid in [src, dst]: node_pipes[nid].send(("ABORT", tx_id))
                    print(f"[Coord] {tx_id} ABORTED.")

def wait_for_vote(pipe, tx_id):
    start = time.time()
    while time.time() - start < 2.0:
        if pipe.poll():
            r = pipe.recv()
            if r[1] == tx_id: return r[0]
    return "NO"

# =================CLI=================
def print_help():
    print("\n--- BANKING MENU ---")
    print("  1. status <node_id>       : View Balance & History")
    print("  2. deposit <node> <amt>   : Add funds (Visa/MasterCard)")
    print("  3. withdraw               : Cash out (M-Pesa/PayPal/ATM)")
    print("  4. transfer <s <b> <amt>  : Internal Transfer")
    print("  5. fail <node> <on/off>   : Simulate Crash")
    print("  6. exit")

def run_cli():
    setup_environment()
    pipes = [multiprocessing.Pipe() for _ in range(NUM_NODES)]
    client_p, client_c = multiprocessing.Pipe()

    parts = [Participant(i, pipes[i][1]) for i in range(NUM_NODES)]
    for p in parts: p.start()
    
    coord = multiprocessing.Process(target=coordinator, args=(client_p, [p[0] for p in pipes]))
    coord.start()
    
    time.sleep(1)
    print("\n=== DISTRIBUTED BANKING CORE STARTED ===")
    print_help()

    try:
        while True:
            cmd = input("\nAction> ").strip().split()
            if not cmd: continue
            
            action = cmd[0].lower()

            if action == "exit": break
            
            elif action == "help": print_help()

            elif action == "status":
                if len(cmd) < 2: 
                    print("Usage: status <node_id> (or 'all')")
                    continue
                
                target = range(NUM_NODES) if cmd[1] == "all" else [int(cmd[1])]
                print("\n--- ACCOUNT STATEMENTS ---")
                for i in target:
                    data = read_db(i)
                    print(f"\n[NODE {i}] Balance: ${data['balance']}")
                    print("  Date                 | Type       | Amount | Details")
                    print("  " + "-"*55)
                    for h in data['history']:
                        print(f"  {h['timestamp']}  | {h['type']:<10} | ${h['amount']:<5} | {h['details']}")

            elif action == "deposit":
                try:
                    nid, amt = int(cmd[1]), int(cmd[2])
                    card = input("  Enter Card No: ").strip()
                    client_c.send(("DEPOSIT", nid, amt, card))
                    time.sleep(2)
                except: print("Usage: deposit <node_id> <amount>")

            elif action == "transfer":
                try:
                    client_c.send(("TX", int(cmd[1]), int(cmd[2]), int(cmd[3])))
                    time.sleep(2)
                except: print("Usage: transfer <src> <dst> <amt>")

            elif action == "fail":
                 try: client_c.send(("SET_FAILURE", int(cmd[1]), cmd[2]=="on"))
                 except: pass

            elif action == "withdraw":
                # Interactive Wizard
                try:
                    nid = int(input("  From Node ID: "))
                    amt = int(input("  Amount: $"))
                    
                    print("  Select Method: [1] M-Pesa  [2] PayPal  [3] ATM")
                    choice = input("  Selection: ")
                    
                    method = ""
                    details = ""
                    
                    if choice == "1":
                        method = "M-Pesa"
                        details = input("  Enter Phone (07...): ")
                    elif choice == "2":
                        method = "PayPal"
                        details = input("  Enter Email: ")
                    elif choice == "3":
                        method = "ATM"
                        details = "Terminal-884" # Auto-generated for ATM
                    else:
                        print("Invalid selection.")
                        continue

                    client_c.send(("WITHDRAW", nid, amt, method, details))
                    time.sleep(2.5)

                except ValueError: print("Invalid Input.")

    except KeyboardInterrupt: pass
    finally:
        client_c.send(("STOP",))
        coord.join()
        for i, p in enumerate(parts):
            pipes[i][0].send(("KILL",))
            p.join()

if __name__ == "__main__":
    run_cli()