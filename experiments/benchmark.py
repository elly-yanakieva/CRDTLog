import sys
import subprocess
from pathlib import Path
import time

# ----------------------------
# Configure paths
# ----------------------------
GRAPH_DIR = Path("graph")
ABSTRACT_DIR = Path("graphAbstract")

#TESTS_DIR = Path("/home/elena/Documents/Exps/scale_ops/manual/tests4_10")  # contains 1..1000
#RESULTS_DIR = Path("scale_ops/benchmark_results4_10")          # will be created



TESTS_DIR = Path("/home/elena/Documents/Exps/scale_ops/manual/tests4_10")

# Derive RESULTS_DIR automatically
tests_name = TESTS_DIR.name                 # tests4_10
results_name = tests_name.replace("tests", "benchmark_results")

#experiment_root = TESTS_DIR.parents[2]      # .../scale_ops
RESULTS_DIR = Path("scale_ops/manual/") / results_name


GRAPH_DL = GRAPH_DIR / "graph_manual.dl"
ABSTRACT_DL = ABSTRACT_DIR / "graph_manual.dl"

SOUFFLE_CMD = ["souffle"]
NUM_TESTS = 30


# ----------------------------
# Helpers
# ----------------------------
def run_souffle(program_path: Path, input_dir: Path) -> tuple[bool, str, float]:
    """
    Runs: souffle -F input_dir program_path
    Returns (ok, message, seconds).
    We omit -D <output_dir> because we're only timing (no outputs needed).
    """
    cmd = SOUFFLE_CMD + ["-F", str(input_dir), str(program_path)]

    t0 = time.perf_counter()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        return False, (
            "Could not find 'souffle' in PATH. "
            "Install Soufflé or adjust SOUFFLE_CMD in the script."
        ), 0.0
    t1 = time.perf_counter()

    if proc.returncode != 0:
        msg = f"Command failed: {' '.join(cmd)}\n\nSTDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}\n"
        return False, msg, (t1 - t0)

    return True, "", (t1 - t0)


# ----------------------------
# Main
# ----------------------------
def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    timing_path = RESULTS_DIR / "timing.tsv"
    timing_lines = ["test_id\tgraph_seconds\tgraphAbstract_seconds\tstatus"]

    graph_times = []
    abstract_times = []
    graph_ok = 0
    abstract_ok = 0

    # sanity checks
    if not GRAPH_DL.exists():
        print(f"ERROR: missing {GRAPH_DL}")
        sys.exit(1)
    if not ABSTRACT_DL.exists():
        print(f"ERROR: missing {ABSTRACT_DL}")
        sys.exit(1)
    if not TESTS_DIR.exists():
        print(f"ERROR: missing {TESTS_DIR}")
        sys.exit(1)

    for i in range(1, NUM_TESTS + 1):
        test_in = TESTS_DIR / str(i)
        if not test_in.exists():
            timing_lines.append(f"{i}\tNA\tNA\tMISSING_INPUT")
            continue

        ok1, msg1, t_graph = run_souffle(GRAPH_DL, test_in)
        if ok1:
            graph_times.append(t_graph)
            graph_ok += 1
        else:
            # keep a short status; full error can be huge
            timing_lines.append(f"{i}\t{t_graph:.6f}\tNA\tFAIL_graph")
            continue

        ok2, msg2, t_abs = run_souffle(ABSTRACT_DL, test_in)
        if ok2:
            abstract_times.append(t_abs)
            abstract_ok += 1
            timing_lines.append(f"{i}\t{t_graph:.6f}\t{t_abs:.6f}\tOK")
        else:
            timing_lines.append(f"{i}\t{t_graph:.6f}\t{t_abs:.6f}\tFAIL_graphAbstract")

    def avg(xs):
        return sum(xs) / len(xs) if xs else 0.0

    avg_graph = avg(graph_times)
    avg_abs = avg(abstract_times)

    timing_lines.append("")
    timing_lines.append("# averages over successful runs")
    timing_lines.append(f"AVG\t{avg_graph:.6f}\t{avg_abs:.6f}\t-")
    timing_lines.append(f"COUNT\t{graph_ok}\t{abstract_ok}\t-")

    timing_path.write_text("\n".join(timing_lines) + "\n", encoding="utf-8")

    print(f"Timing written to: {timing_path}")
    print(f"Average graph time:         {avg_graph:.6f} s over {graph_ok} successful runs")
    print(f"Average graphAbstract time: {avg_abs:.6f} s over {abstract_ok} successful runs")


if __name__ == "__main__":
    main()
