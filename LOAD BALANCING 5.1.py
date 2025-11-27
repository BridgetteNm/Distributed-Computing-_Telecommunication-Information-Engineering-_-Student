import random  # for generating random traffic amounts
import time    # for simulating time delays and measuring duration

# Node class representing a telecom server

class Node:
    def __init__(self, node_id, capacity, weight):
        self.node_id = node_id            # unique identifier for the node
        self.capacity = capacity          # maximum load the node can handle comfortably
        self.weight = weight              # weight used by load balancer to assign traffic
        self.current_load = 0             # current load being handled
        self.total_traffic = 0            # total traffic successfully processed
        self.latency_history = []         # latency recorded for each request
        self.dropped = 0                  # traffic dropped due to overload

    def add_load(self, traffic):
        """
        Add incoming traffic to the node.
        Calculate latency and drop traffic if node is overloaded.
        """
        # Drop traffic if it exceeds 120% of node capacity
        if self.current_load + traffic > self.capacity * 1.2:
            self.dropped += traffic
            return None  # traffic dropped, no latency recorded

        # Accept the traffic
        self.current_load += traffic
        self.total_traffic += traffic

        # Calculate latency based on load: higher load → higher latency
        load_ratio = self.current_load / self.capacity
        latency = round(10 + load_ratio * 90, 2)  # latency in milliseconds
        self.latency_history.append(latency)

        # Simulate processing by reducing current load (traffic served)
        self.current_load *= 0.7
        return latency  # return latency for reporting



# Weighted Load Balancer

class WeightedBalancer:
    def __init__(self, nodes):
        self.nodes = nodes
        self.total_weight = sum(n.weight for n in nodes)  # sum of all node weights

    def pick_node(self):
        """
        Select a node based on weighted random selection.
        Nodes with higher weight have higher chance of being chosen.
        """
        r = random.uniform(0, self.total_weight)  # random number in weight range
        cumulative = 0
        for node in self.nodes:
            cumulative += node.weight
            if r <= cumulative:
                return node

    def distribute_traffic(self, t_id, traffic_amount):
        """
        Assign traffic to a selected node and print the result.
        """
        node = self.pick_node()
        latency = node.add_load(traffic_amount)

        if latency is None:
            print(f"Traffic {t_id} → Node {node.node_id} OVERLOADED → DROPPED")
        else:
            print(f"Traffic {t_id} → Node {node.node_id} | "
                  f"Load={traffic_amount} | Latency={latency} ms")



# Simulation function

def simulate():
    # Create 4 nodes with different capacities and weights
    nodes = [
        Node(0, 120, 5),  # largest node, highest weight
        Node(1, 90, 3),
        Node(2, 70, 2),
        Node(3, 40, 1)    # smallest node, lowest weight
    ]

    balancer = WeightedBalancer(nodes)  # create the load balancer

    print("\nTELECOMMUNICATION WEIGHTED LOAD BALANCING\n")

    start_time = time.time()  # record start time
    total_incoming = 0        # track total generated traffic

    # Generate 30 random traffic events
    for t in range(1, 31):
        traffic_amount = random.randint(10, 40)  # random traffic unit
        total_incoming += traffic_amount
        balancer.distribute_traffic(t, traffic_amount)
        time.sleep(0.1)  # simulate time interval between traffic events

    end_time = time.time()
    duration = end_time - start_time  # total simulation time

    
    # Calculate throughput and efficiency
   
    total_processed = sum(n.total_traffic for n in nodes)  # sum of processed traffic
    total_dropped = sum(n.dropped for n in nodes)          # sum of dropped traffic
    throughput = round(total_processed / duration, 2)      # traffic units per second
    efficiency = round((total_processed / total_incoming) * 100, 2) if total_incoming > 0 else 0

   
    # Print per-node results
    
    print("\n RESULTS ")
    for node in nodes:
        avg_latency = sum(node.latency_history) / len(node.latency_history) if node.latency_history else 0
        print(f"Node {node.node_id}: Processed={node.total_traffic}, Dropped={node.dropped}, "
              f"Avg Latency={avg_latency:.2f} ms")


    # Printing overall network metrics
    
    print("\n NETWORK METRICS ")
    print(f"Total Incoming Traffic: {total_incoming}")
    print(f"Total Processed: {total_processed}")
    print(f"Total Dropped: {total_dropped}")
    print(f"Throughput: {throughput} units/sec")
    print(f"Network Efficiency: {efficiency}%\n")


if __name__ == "__main__":
    simulate()  # running  the simulation
