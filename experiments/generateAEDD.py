import os
import random

MAX_EVENTS = 1000
MAX_REPLICAS = 16
P_NEW_REPLICA = 0.3     # probability to start a new branch
P_MERGE = 0.4           # probability to merge branches

OUT_DIR = "scale_reps/automatic/tests16_1000"

def generate_execution(
        out_dir: str,
        graphop_filename: str = "graphOp.facts",
        vis_filename: str = "vis.facts",
):


    events = []
    vis = []

    replicas = {}
    replica_state = {}
    active_replicas = []

    event_id = 1
    node_counter = 0

    def fresh_node():
        nonlocal node_counter

        n = node_counter
        node_counter += 1

        name = ""
        while True:
            name = chr(ord('a') + (n % 26)) + name
            n = n // 26 - 1
            if n < 0:
                break

        return name


    while event_id <= MAX_EVENTS:

        if (not active_replicas) or (len(active_replicas) < MAX_REPLICAS and random.random() < P_NEW_REPLICA):
            r = len(active_replicas)
            active_replicas.append(r)
            replicas[r] = []
            replica_state[r] = {'nodes': set(), 'edges': set()}
        else:
            r = random.choice(active_replicas)

        state = replica_state[r]

        ops = ["addNode"]
        if state['nodes']:
            ops.append("removeNode")
        if len(state['nodes']) >= 2:
            ops.append("addEdge")
        if state['edges']:
            ops.append("removeEdge")

        op = random.choice(ops)

        if op == "addNode":
            n = fresh_node()
            events.append(f"{event_id}\taddNode\t{n}\t-1")
            state['nodes'].add(n)

        elif op == "removeNode":
            n = random.choice(list(state['nodes']))
            events.append(f"{event_id}\tremoveNode\t{n}\t-1")

            state['nodes'].remove(n)

        elif op == "addEdge":
            a, b = random.sample(list(state['nodes']), 2)
            if (a, b) in state['edges']:
                continue
            events.append(f"{event_id}\taddEdge\t{a}\t{b}")
            state['edges'].add((a, b))

        elif op == "removeEdge":
            a, b = random.choice(list(state['edges']))
            events.append(f"{event_id}\tremoveEdge\t{a}\t{b}")
            state['edges'].remove((a, b))

        if replicas[r]:
            vis.append(f"{replicas[r][-1]}\t{event_id}")

        for other in active_replicas:
            if other != r and replicas[other] and random.random() < P_MERGE:
                seen = random.choice(replicas[other])
                vis.append(f"{seen}\t{event_id}")

        replicas[r].append(event_id)
        event_id += 1


    os.makedirs(out_dir, exist_ok=True)

    with open(os.path.join(out_dir, graphop_filename), "w") as f:
        f.write("\n".join(events))

    with open(os.path.join(out_dir, vis_filename), "w") as f:
        f.write("\n".join(vis))


    return events, vis


if __name__ == "__main__":
    for i in range(1, 31):
        out_dir = os.path.join(OUT_DIR, str(i))

        generate_execution(out_dir)

    print("Generated 1000 executions in folders 1..30")
