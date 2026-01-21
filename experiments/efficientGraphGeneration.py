import os
import random
from dataclasses import dataclass, field
from typing import List, Optional, Set, Tuple


# ----------------------------
# Node naming: a,b,...,z,aa,ab,...
# ----------------------------
def node_name(i: int) -> str:
    name = ""
    n = i
    while True:
        name = chr(ord("a") + (n % 26)) + name
        n = n // 26 - 1
        if n < 0:
            break
    return name


# ----------------------------
# Generate directed graph with exactly M edges (O(M))
# ----------------------------
def generate_random_directed_edges_with_prob(
        num_nodes: int,
        edge_prob: float,
        rng: random.Random,
        allow_self_loops: bool = False,
) -> List[Tuple[int, int]]:
    """
    Efficient ER-style directed graph generator.
    Uses expected edge count instead of O(n^2) iteration.
    """
    max_edges = num_nodes * (num_nodes if allow_self_loops else (num_nodes - 1))
    expected_edges = max_edges * edge_prob

    # Sample actual edge count from Binomial approximation
    # (Poisson approx is also fine for small p)
    num_edges = int(rng.gauss(expected_edges, expected_edges ** 0.5))
    num_edges = max(0, min(num_edges, max_edges))

    edges = set()

    while len(edges) < num_edges:
        u = rng.randrange(num_nodes)
        v = rng.randrange(num_nodes)
        if not allow_self_loops and u == v:
            continue
        edges.add((u, v))

    return list(edges)

# ----------------------------
# Replica model
# ----------------------------
@dataclass
class Replica:
    rid: int
    last_event: Optional[int] = None
    known_nodes: Set[int] = field(default_factory=set)


# ----------------------------
# Main: build abstract execution from graph, streaming to files
# ----------------------------
def generate_execution_from_random_graph(
        out_dir: str,
        num_nodes: int,
        edge_prob: int,
        max_replicas: int = 5,
        p_new_replica: float = 0.3,
        p_random_merge: float = 0.10,
        p_edge_bias: float = 0.60,
        seed: Optional[int] = None,
        allow_self_loops: bool = False,
) -> None:
    """
    Writes:
      - out_dir/graphOp.facts  (addNode, addEdge)
      - out_dir/vis.facts      (visibility relation)

    Efficient for 10k nodes:
      - fixed edge count generation (no n^2 loop)
      - incremental ready_edges scheduling
      - streaming output (no huge lists)
      - no per-event snapshots (only per-replica state)
    """
    os.makedirs(out_dir, exist_ok=True)

    # Randomness: if seed is None, this is still random (system-seeded)
    rng = random.Random(seed)

    # --- build graph edges (ints) ---
    edges = generate_random_directed_edges_with_prob(
        num_nodes=num_nodes,
        edge_prob=edge_prob,
        rng=rng,
        allow_self_loops=allow_self_loops,
    )


    # --- adjacency (edge indices) for incremental readiness ---
    out_adj: List[List[int]] = [[] for _ in range(num_nodes)]
    in_adj: List[List[int]] = [[] for _ in range(num_nodes)]
    U = [0] * len(edges)
    V = [0] * len(edges)

    for ei, (u, v) in enumerate(edges):
        U[ei] = u
        V[ei] = v
        out_adj[u].append(ei)
        in_adj[v].append(ei)

    # edge_state: 0 pending, 1 ready, 2 done
    edge_state = bytearray(len(edges))
    ready_edges: List[int] = []  # stack of edge indices

    node_added = bytearray(num_nodes)  # 0/1
    remaining_nodes = num_nodes

    # For fast "where can I merge from to learn node x?"
    # Store the replica that originally added x.
    node_origin_replica = [-1] * num_nodes

    def activate_edges_for_new_node(x: int):
        """
        When node x becomes added, edges become ready if the other endpoint is already added.
        """
        # x as source: x -> v
        for ei in out_adj[x]:
            if edge_state[ei] != 0:
                continue
            v = V[ei]
            if node_added[v]:
                edge_state[ei] = 1
                ready_edges.append(ei)

        # x as target: u -> x
        for ei in in_adj[x]:
            if edge_state[ei] != 0:
                continue
            u = U[ei]
            if node_added[u]:
                edge_state[ei] = 1
                ready_edges.append(ei)

    # --- replicas ---
    replicas: List[Replica] = []
    active_ids: List[int] = []

    def ensure_replica() -> int:
        nonlocal replicas, active_ids
        if not active_ids:
            replicas.append(Replica(0))
            active_ids.append(0)
            return 0

        if len(active_ids) < max_replicas and rng.random() < p_new_replica:
            rid = len(active_ids)
            replicas.append(Replica(rid))
            active_ids.append(rid)
            return rid

        return rng.choice(active_ids)

    # --- streaming output ---
    graphop_path = os.path.join(out_dir, "graphOp.facts")
    vis_path = os.path.join(out_dir, "vis.facts")
    meta_path = os.path.join(out_dir, "meta.txt")

    event_id = 1

    with open(graphop_path, "w", encoding="utf-8") as f_ops, open(vis_path, "w", encoding="utf-8") as f_vis:
        def write_vis(src: int, dst: int):
            f_vis.write(f"{src}\t{dst}\n")

        def local_predecessor_vis(rep: Replica, cur: int):
            if rep.last_event is not None:
                write_vis(rep.last_event, cur)

        def merge_from_replica(target: Replica, source: Replica, cur: int):
            """
            Merge from the *latest* event of `source` into `cur` (realistic).
            """
            if source.last_event is None:
                return
            write_vis(source.last_event, cur)
            target.known_nodes |= source.known_nodes

        def maybe_random_merge(target: Replica, cur: int):
            if len(active_ids) <= 1:
                return
            if rng.random() >= p_random_merge:
                return
            other_id = rng.choice([r for r in active_ids if r != target.rid and replicas[r].last_event is not None])
            merge_from_replica(target, replicas[other_id], cur)

        # ---------------------------------------------------
        # Main scheduling loop:
        # - add nodes until all added
        # - add edges when ready_edges non-empty (biased to mix)
        # ---------------------------------------------------
        remaining_edges_done = 0
        total_edges = len(edges)

        while remaining_nodes > 0 or ready_edges or remaining_edges_done < total_edges:
            rid = ensure_replica()
            rep = replicas[rid]

            can_do_edge = bool(ready_edges)
            do_edge = can_do_edge and (rng.random() < p_edge_bias)
            if remaining_nodes == 0:
                do_edge = True  # finish edges

            if not do_edge:
                # -------- addNode --------
                # pick a random not-yet-added node
                # (efficient sampling by rejection; with 10k this is fine)
                while True:
                    n = rng.randrange(num_nodes)
                    if not node_added[n]:
                        break

                # write op
                f_ops.write(f"{event_id}\taddNode\t{node_name(n)}\t-1\n")

                # vis: local predecessor
                local_predecessor_vis(rep, event_id)

                # optional extra merge for realism
                maybe_random_merge(rep, event_id)

                # commit node
                node_added[n] = 1
                remaining_nodes -= 1

                rep.known_nodes.add(n)
                node_origin_replica[n] = rid

                # enable new ready edges
                activate_edges_for_new_node(n)

                rep.last_event = event_id
                event_id += 1
                continue

            # -------- addEdge --------
            if not ready_edges:
                # If no ready edges but nodes remain, we'll add nodes next iteration.
                # If no nodes remain, this would indicate something inconsistent.
                if remaining_nodes > 0:
                    continue
                break

            ei = ready_edges.pop()
            if edge_state[ei] != 1:
                continue  # stale
            edge_state[ei] = 2
            remaining_edges_done += 1

            u = U[ei]
            v = V[ei]

            # write op
            f_ops.write(f"{event_id}\taddEdge\t{node_name(u)}\t{node_name(v)}\n")

            # vis: local predecessor
            local_predecessor_vis(rep, event_id)

            # Ensure replica knows u and v:
            # merge from the origin replica (latest event) if needed.
            for x in (u, v):
                if x not in rep.known_nodes:
                    src_rid = node_origin_replica[x]
                    if src_rid < 0:
                        raise RuntimeError(f"Internal error: endpoint {x} was never added.")
                    if src_rid != rep.rid:
                        merge_from_replica(rep, replicas[src_rid], event_id)

            # optional extra merge
            maybe_random_merge(rep, event_id)

            # commit
            rep.last_event = event_id
            event_id += 1

        # write metadata
        with open(meta_path, "w", encoding="utf-8") as f_meta:
            f_meta.write(f"seed={seed}\n")
            f_meta.write(f"num_nodes={num_nodes}\n")
            f_meta.write(f"num_edges={num_edges}\n")
            f_meta.write(f"max_replicas={max_replicas}\n")
            f_meta.write(f"p_new_replica={p_new_replica}\n")
            f_meta.write(f"p_random_merge={p_random_merge}\n")
            f_meta.write(f"p_edge_bias={p_edge_bias}\n")


# ----------------------------
# Generate 30 runs: random_graph/1..30
# ----------------------------
if __name__ == "__main__":
    NUM_RUNS = 30

    NUM_NODES = 10_000
    NUM_EDGES = 60_000

    MAX_REPLICAS = 5

    BASE_DIR = "random_graph/10000"
    os.makedirs(BASE_DIR, exist_ok=True)

    sysrng = random.SystemRandom()

    for i in range(1, NUM_RUNS + 1):
        out = os.path.join(BASE_DIR, str(i))

        # truly random seed each run; stored in meta.txt
        run_seed = sysrng.randrange(2**63)

        print(f"[{i}/{NUM_RUNS}] generating (nodes={NUM_NODES}, edges={NUM_EDGES}, seed={run_seed}) ...")
        generate_execution_from_random_graph(
            out_dir=out,
            num_nodes=NUM_NODES,
            num_edges=NUM_EDGES,
            max_replicas=MAX_REPLICAS,
            p_new_replica=0.3,
            p_random_merge=0.10,
            p_edge_bias=0.60,
            seed=run_seed,
            allow_self_loops=False,
        )
        print(f"[{i}/{NUM_RUNS}] wrote {out}/graphOp.facts and {out}/vis.facts")

    print(f"Done. Generated {NUM_RUNS} runs under {BASE_DIR}/1..{NUM_RUNS}")
