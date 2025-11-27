import hashlib
import random

# ------------------------------
# Simple hash function
# ------------------------------
def hash_key(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16)


# ------------------------------
# Shard Manager
# ------------------------------
class ShardedSubscriberDB:
    def __init__(self, num_shards):
        self.num_shards = num_shards
        self.shards = {i: {} for i in range(num_shards)}

    def get_shard_id(self, imsi):
        """Maps IMSI to a shard using modulo hashing."""
        return hash_key(imsi) % self.num_shards

    def insert_subscriber(self, imsi, record):
        shard_id = self.get_shard_id(imsi)
        self.shards[shard_id][imsi] = record
        print(f"INSERT: IMSI {imsi} stored in SHARD {shard_id}")

    def get_subscriber(self, imsi):
        shard_id = self.get_shard_id(imsi)
        record = self.shards[shard_id].get(imsi)
        print(f"LOOKUP: IMSI {imsi} queried from SHARD {shard_id}")
        return record

    def print_distribution(self):
        print("\n=== SHARD DISTRIBUTION ===")
        for shard, data in self.shards.items():
            print(f"Shard {shard}: {len(data)} subscriber records")


# ------------------------------
# Demo scenario
# ------------------------------
def demo_telecom_sharding():
    print("\nTELECOM SHARDING DEMO\n")

    # Suppose we have 3 backend servers storing subscriber data
    db = ShardedSubscriberDB(num_shards=3)

    # Generate sample telecom subscriber IMSIs
    subscribers = []
    for _ in range(15):
        imsi = "IMSI" + str(random.randint(100000, 999999))
        record = {
            "phone": "+2547" + str(random.randint(10000000, 99999999)),
            "plan": random.choice(["PREPAID", "POSTPAID", "UNLIMITED"]),
            "device": random.choice(["Android", "iPhone", "FeaturePhone"]),
        }
        subscribers.append((imsi, record))

    # Insert subscriber records into shards
    for imsi, rec in subscribers:
        db.insert_subscriber(imsi, rec)

    # Print how data was distributed
    db.print_distribution()

    # Perform lookups
    print("\n=== SAMPLE LOOKUPS ===")
    for imsi, _ in random.sample(subscribers, 3):
        result = db.get_subscriber(imsi)
        print("Record:", result)


if __name__ == "__main__":
    demo_telecom_sharding()
