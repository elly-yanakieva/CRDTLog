import os
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


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
# Random directed graph (O(n^2))
# ----------------------------
def generate_random_directed_graph(
        num_nodes: int,
        edge_prob: float,
        seed: Optional[int] = None,
        allow_self_loops: bool = False,
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Directed Erdos–Renyi style: for each ordered pair (u,v), u!=v,
    add u->v with probability edge_prob.
    """
    if seed is not None:
        random.seed(seed)

    nodes = [node_name(i) for i in range(num_nodes)]
    edges: List[Tuple[str, str]] = []

    for i in range(num_nodes):
        for j in range(num_nodes):
            if not allow_self_loops and i == j:
                continue
            if random.random() < edge_prob:
                edges.append((nodes[i], nodes[j]))

    return nodes, edges


# ----------------------------
# Abstract execution generation
# ----------------------------
@dataclass
class Replica:
    rid: int
    events: List[int] = field(default_factory=list)  # local order
    known_nodes: Set[str] = field(default_factory=set)


def graph_to_abstract_execution(
        nodes: List[str],
        edges: List[Tuple[str, str]],
        max_replicas: int = 5,
        p_new_replica: float = 0.3,       # chance to create new replica
        p_random_merge: float = 0.15,
        p_edge_bias: float = 0.55,        # once edges are possible, bias towards edges
        seed: Optional[int] = None,
) -> Tuple[List[str], List[str]]:
    """
    Valid + more realistic abstract execution, with efficient edge scheduling:
      - No scanning remaining edges each event.
      - Maintains pending_edges + ready_edges incrementally via adjacency.
    """
    if seed is not None:
        random.seed(seed)

    # --- nodes left to add ---
    remaining_nodes: Set[str] = set(nodes)
    added_nodes: Set[str] = set()

    # --- edges scheduling (B option) ---
    pending_edges: Set[Tuple[str, str]] = set(edges)  # edges not yet enabled (endpoint missing)
    ready_edges: List[Tuple[str, str]] = []           # edges enabled (both endpoints added), can pop O(1)

    out_adj: Dict[str, List[Tuple[str, str]]] = {n: [] for n in nodes}
    in_adj: Dict[str, List[Tuple[str, str]]] = {n: [] for n in nodes}
    for (u, v) in edges:
        out_adj[u].append((u, v))
        in_adj[v].append((u, v))

    def activate_edges_for_new_node(x: str):
        """
        When node x is added, edges become ready:
          - x -> v if v already added
          - u -> x if u already added
        """
        # x as source
        for (u, v) in out_adj[x]:
            if (u, v) in pending_edges and v in added_nodes:
                pending_edges.remove((u, v))
                ready_edges.append((u, v))

        # x as target
        for (u, v) in in_adj[x]:
            if (u, v) in pending_edges and u in added_nodes:
                pending_edges.remove((u, v))
                ready_edges.append((u, v))

    # --- replicas ---
    replicas: Dict[int, Replica] = {}
    active: List[int] = []

    # snapshots: node knowledge in causal past of an event id
    snap_nodes: Dict[int, Set[str]] = {}

    events_out: List[str] = []
    vis_out: List[str] = []
    event_id = 1

    def ensure_replica() -> int:
        nonlocal replicas, active
        if not active:
            replicas[0] = Replica(0)
            active.append(0)
            return 0

        if len(active) < max_replicas and random.random() < p_new_replica:
            rid = len(active)
            replicas[rid] = Replica(rid)
            active.append(rid)
            return rid

        return random.choice(active)

    def add_local_predecessor_vis(rid: int, cur: int):
        rep = replicas[rid]
        if rep.events:
            vis_out.append(f"{rep.events[-1]}\t{cur}")

    def merge_from_event(target_rid: int, source_eid: int, cur: int):
        """
        Add vis source_eid -> cur, and update target replica knowledge with snapshot of source_eid.
        """
        vis_out.append(f"{source_eid}\t{cur}")
        replicas[target_rid].known_nodes |= snap_nodes.get(source_eid, set())

    def find_source_event_that_knows(node: str, exclude_rid: int) -> Optional[int]:
        """
        Return the latest event id from some other replica that knows `node`.
        """
        best = None
        for rid in active:
            if rid == exclude_rid:
                continue
            rep = replicas[rid]
            if not rep.events:
                continue
            last = rep.events[-1]
            if node in snap_nodes.get(last, set()):
                if best is None or last > best:
                    best = last
        return best

    def maybe_random_merge(rid: int, cur: int):
        if len(active) <= 1:
            return
        if random.random() >= p_random_merge:
            return

        other_choices = [o for o in active if o != rid and replicas[o].events]
        if not other_choices:
            return
        other = random.choice(other_choices)
        src = replicas[other].events[-1]
        merge_from_event(rid, src, cur)

    def any_edges_possible() -> bool:
        # fast: if we have any ready edges
        return bool(ready_edges)

    # ---------------------------------------------------
    # Main schedule: interleave node and edge events
    # ---------------------------------------------------
    while remaining_nodes or ready_edges or pending_edges:
        rid = ensure_replica()
        rep = replicas[rid]

        can_do_edge = any_edges_possible()
        do_edge = can_do_edge and (random.random() < p_edge_bias)
        if not remaining_nodes:
            do_edge = True  # once nodes exhausted, finish edges

        if not do_edge:
            # -------- addNode --------
            n = random.choice(list(remaining_nodes))
            events_out.append(f"{event_id}\taddNode\t{n}\t-1")

            add_local_predecessor_vis(rid, event_id)
            maybe_random_merge(rid, event_id)

            # local apply + snapshot
            rep.known_nodes.add(n)
            rep.events.append(event_id)
            snap_nodes[event_id] = set(rep.known_nodes)

            # update global node status + enable edges incrementally
            remaining_nodes.remove(n)
            added_nodes.add(n)
            activate_edges_for_new_node(n)

            event_id += 1
            continue

        # -------- addEdge --------
        if not ready_edges:
            # Should only happen if pending_edges exist but endpoints not added (impossible if no remaining_nodes)
            if remaining_nodes:
                continue
            raise RuntimeError("Stuck: no ready edges but no nodes remaining. Check graph construction.")

        u, v = ready_edges.pop()  # O(1)

        events_out.append(f"{event_id}\taddEdge\t{u}\t{v}")

        add_local_predecessor_vis(rid, event_id)

        # Ensure replica knows endpoints via minimal merges from other replicas' latest events
        for x in (u, v):
            if x not in rep.known_nodes:
                src = find_source_event_that_knows(x, exclude_rid=rid)
                if src is None:
                    raise RuntimeError(f"Cannot justify node {x} for edge ({u},{v}).")
                merge_from_event(rid, src, event_id)

        maybe_random_merge(rid, event_id)

        # Commit event + snapshot
        rep.events.append(event_id)
        snap_nodes[event_id] = set(rep.known_nodes)

        event_id += 1

        # Stop condition: nothing left
        if not remaining_nodes and not ready_edges and not pending_edges:
            break

    return events_out, vis_out


def write_execution(out_dir: str, events: List[str], vis: List[str],
                    graphop_filename: str = "graphOp.facts",
                    vis_filename: str = "vis.facts") -> None:
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, graphop_filename), "w", encoding="utf-8") as f:
        f.write("\n".join(events) + ("\n" if events else ""))
    with open(os.path.join(out_dir, vis_filename), "w", encoding="utf-8") as f:
        f.write("\n".join(vis) + ("\n" if vis else ""))


if __name__ == "__main__":
    NUM_RUNS = 30

    NUM_NODES = 10
    EDGE_PROB = 0.05          # beware: edges ~ n*(n-1)*p => ~9990 for 0.01 for 1000
    MAX_REPLICAS = 4

    BASE_DIR = "random_graph/10"
    os.makedirs(BASE_DIR, exist_ok=True)

    sysrng = random.SystemRandom()

    for i in range(1, NUM_RUNS + 1):
        out = os.path.join(BASE_DIR, str(i))
        run_seed = sysrng.randrange(2**63)

        nodes, edges = generate_random_directed_graph(NUM_NODES, EDGE_PROB, seed=run_seed)

        events, vis = graph_to_abstract_execution(
            nodes,
            edges,
            max_replicas=MAX_REPLICAS,
            p_new_replica=0.3,
            p_random_merge=0.15,
            p_edge_bias=0.6,
            seed=run_seed,
        )

        write_execution(out, events, vis)

        with open(os.path.join(out, "meta.txt"), "w", encoding="utf-8") as f:
            f.write(f"seed={run_seed}\n")
            f.write(f"num_nodes={NUM_NODES}\n")
            f.write(f"num_edges={len(edges)}\n")
            f.write(f"max_replicas={MAX_REPLICAS}\n")
            f.write(f"edge_prob={EDGE_PROB}\n")

        print(f"[{i}/{NUM_RUNS}] wrote {out} (edges={len(edges)})")

    print(f"Done. Generated {NUM_RUNS} runs under {BASE_DIR}/1..{NUM_RUNS}")
