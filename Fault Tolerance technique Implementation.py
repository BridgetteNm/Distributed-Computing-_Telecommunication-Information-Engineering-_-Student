import random
import time
import threading
from statistics import mean

class BTS:
    def __init__(self, bts_id, max_users, neighbors):
        self.bts_id = bts_id
        self.max_users = max_users   # full capacity
        self.active_users = 0
        self.neighbors = neighbors   # overlapping cells
        self.status = "OK"           # OK, DEGRADED, DOWN
        self.fail_prob = 0.01        # probability of failure per cycle
        self.repair_prob = 0.05      # probability of recovery per cycle

    def apply_faults(self):
        """Simulates hardware faults and recoveries."""
        if self.status == "OK":
            if random.random() < self.fail_prob:
                self.status = "DEGRADED"
                print(f"[FAULT] BTS {self.bts_id} degraded (reduced capacity).")

        elif self.status == "DEGRADED":
            # degraded can go fully down
            if random.random() < self.fail_prob * 0.5:
                self.status = "DOWN"
                print(f"[FAIL] BTS {self.bts_id} has gone DOWN completely.")

            # also has chance to recover
            elif random.random() < self.repair_prob:
                self.status = "OK"
                print(f"[RECOVERY] BTS {self.bts_id} recovered to OK.")

        elif self.status == "DOWN":
            # chance to recover partially
            if random.random() < self.repair_prob:
                self.status = "DEGRADED"
                print(f"[RECOVERY] BTS {self.bts_id} restored to DEGRADED mode.")

    def effective_capacity(self):
        if self.status == "OK":
            return self.max_users
        elif self.status == "DEGRADED":
            return max(3, self.max_users // 3)  # degraded mode
        return 0  # DOWN state

    def connect_user(self):
        """Attempt to connect user to this BTS."""
        if self.active_users < self.effective_capacity():
            self.active_users += 1
            return True
        return False

    def release_users(self):
        """Simulate users leaving the BTS randomly."""
        if self.active_users > 0:
            self.active_users -= random.randint(0, min(2, self.active_users))


class Network:
    def __init__(self):
        self.bts_nodes = {}
        self.dropped_users = 0
        self.recovered_users = 0

    def add_bts(self, bts):
        self.bts_nodes[bts.bts_id] = bts

    def connect_user_to_network(self, user_id, primary_bts_id):
        """Try primary BTS, then neighbors if fault occurs."""
        primary = self.bts_nodes[primary_bts_id]

        # 1) Try primary BTS
        if primary.connect_user():
            return primary.bts_id

        # 2) Try neighbors (coverage overlap)
        for nb in primary.neighbors:
            if self.bts_nodes[nb].connect_user():
                self.recovered_users += 1
                return nb

        # 3) Otherwise user dropped
        self.dropped_users += 1
        return None

    def simulate_cycle(self, n_new_users=40):
        """Simulate one time-step of network operation."""
        # Fault injection across all BTSs
        for bts in self.bts_nodes.values():
            bts.apply_faults()

        # New users arrive at random BTS
        for user in range(n_new_users):
            bts_choice = random.choice(list(self.bts_nodes.keys()))
            self.connect_user_to_network(user, bts_choice)

        # Users leaving
        for bts in self.bts_nodes.values():
            bts.release_users()

    def print_status(self):
        print("\n===== NETWORK STATUS =====")
        for bts in self.bts_nodes.values():
            print(
                f"BTS {bts.bts_id}: status={bts.status}, "
                f"load={bts.active_users}/{bts.effective_capacity()}"
            )
        print(f"Dropped users: {self.dropped_users}")
        print(f"Recovered via neighbor cells: {self.recovered_users}")
        print("============================\n")


def run_simulation(cycles=20):
    network = Network()

    # Define BTS topology with overlapping coverage
    b1 = BTS("BTS1", max_users=30, neighbors=["BTS2"])
    b2 = BTS("BTS2", max_users=35, neighbors=["BTS1", "BTS3"])
    b3 = BTS("BTS3", max_users=25, neighbors=["BTS2"])

    network.add_bts(b1)
    network.add_bts(b2)
    network.add_bts(b3)

    print("\nStarting BTS Fault Tolerance Simulation...\n")

    for cycle in range(cycles):
        print(f"---- Cycle {cycle+1} ----")
        network.simulate_cycle()
        network.print_status()
        time.sleep(0.6)

    print("Simulation complete.\n")
    print(f"TOTAL dropped users = {network.dropped_users}")
    print(f"TOTAL recovered users (handover via neighbors) = {network.recovered_users}")


if __name__ == "__main__":
    run_simulation(cycles=20)
