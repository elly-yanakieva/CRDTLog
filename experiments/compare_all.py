import os
import sys
import shutil
import subprocess
from collections import Counter
from pathlib import Path
import difflib
import time


# ----------------------------
# Configure paths
# ----------------------------
GRAPH_DIR = Path("graph")
ABSTRACT_DIR = Path("graphAbstract")
#TESTS_DIR = Path("/home/elena/Documents/Exps/manual/")          # contains 1..1000
#RESULTS_DIR = Path("manual/test_results5_10000")  # will be created

GRAPH_DL = GRAPH_DIR / "graph_manual.dl"
ABSTRACT_DL = ABSTRACT_DIR / "graph_manual.dl"

# If you're using souffle, this is typical:
# souffle -F <input_dir> -D <output_dir> <program.dl>
SOUFFLE_CMD = ["souffle"]

NUM_TESTS = 1000

# Output filenames to compare
COMPARE_PAIRS = [
    ("nodesState.csv", ABSTRACT_DIR.name, "nodes.csv"),
    ("mapState.csv", ABSTRACT_DIR.name, "edges.csv"),
]
# Meaning:
# graph outputs: nodesState.csv, mapState.csv
# graphAbstract outputs: nodes.csv, edges.csv


# ----------------------------
# Helpers
# ----------------------------
def run_souffle(program_path: Path, input_dir: Path, output_dir: Path) -> tuple[bool, str, float]:
    """
    Runs: souffle -F input_dir -D output_dir program_path
    Returns (ok, message, seconds).
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = SOUFFLE_CMD + ["-F", str(input_dir), "-D", str(output_dir), str(program_path)]
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



def read_csv_as_multiset(path: Path, ignore_singleton: bool = False) -> Counter:
    """
    Reads a CSV file and returns a multiset of rows.
    If ignore_singleton=True, lines with only one token are ignored.
    """
    rows = []
    if not path.exists():
        return Counter()

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if ignore_singleton:
                # split on comma or whitespace
                tokens = [t for t in line.replace(",", " ").split() if t]
                if len(tokens) <= 1:
                    continue

            rows.append(line)

    return Counter(rows)



def multiset_diff(a: Counter, b: Counter):
    """
    Returns (only_in_a, only_in_b) as lists of lines (with multiplicity),
    sorted for stable diffs.
    """
    only_a = []
    only_b = []

    # items only in a
    for line, cnt in (a - b).items():
        only_a.extend([line] * cnt)

    # items only in b
    for line, cnt in (b - a).items():
        only_b.extend([line] * cnt)

    only_a.sort()
    only_b.sort()
    return only_a, only_b


def write_unified_diff(file_a_label: str, lines_a: list[str], file_b_label: str, lines_b: list[str], out_path: Path):
    """
    Writes a unified diff between sorted normalized line lists.
    """
    diff = difflib.unified_diff(
        lines_a,
        lines_b,
        fromfile=file_a_label,
        tofile=file_b_label,
        lineterm="",
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(diff) + ("\n" if lines_a or lines_b else ""), encoding="utf-8")


def compare_files(
        graph_out: Path,
        abs_out: Path,
        diff_out_path: Path,
        ignore_singleton_graph: bool = False,
) -> tuple[bool, str]:

    a = read_csv_as_multiset(graph_out, ignore_singleton=ignore_singleton_graph)
    b = read_csv_as_multiset(abs_out)

    if a == b:
        return True, ""

    only_a, only_b = multiset_diff(a, b)

    report_lines = []
    report_lines.append(f"Mismatch comparing:\n  graph:         {graph_out}\n  graphAbstract: {abs_out}\n")

    report_lines.append(f"Rows only in graph ({len(only_a)}):")
    report_lines.extend(only_a[:200])
    if len(only_a) > 200:
        report_lines.append(f"... ({len(only_a) - 200} more)")
    report_lines.append("")

    report_lines.append(f"Rows only in graphAbstract ({len(only_b)}):")
    report_lines.extend(only_b[:200])
    if len(only_b) > 200:
        report_lines.append(f"... ({len(only_b) - 200} more)")
    report_lines.append("")

    diff_out_path.parent.mkdir(parents=True, exist_ok=True)
    diff_out_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    write_unified_diff(
        f"graph:{graph_out.name}",
        sorted(a.elements()),
        f"graphAbstract:{abs_out.name}",
        sorted(b.elements()),
        diff_out_path.with_suffix(".unified.diff"),
    )

    return False, f"Content mismatch: {graph_out.name} vs {abs_out.name}"



# ----------------------------
# Main
# ----------------------------
def main():
    # Point this to your "upper folder" that contains many tests_* folders
    TESTS_ROOT = Path("/home/elena/Documents/Exps/manual_1")

    RESULTS_ROOT = Path("manual") / "all_test_results"
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)

    # sanity checks
    if not GRAPH_DL.exists():
        print(f"ERROR: missing {GRAPH_DL}")
        sys.exit(1)
    if not ABSTRACT_DL.exists():
        print(f"ERROR: missing {ABSTRACT_DL}")
        sys.exit(1)
    if not TESTS_ROOT.exists():
        print(f"ERROR: missing {TESTS_ROOT}")
        sys.exit(1)

    # collect all suites (folders like tests5_10000, tests5_20, etc.)
    test_suites = sorted([
        p for p in TESTS_ROOT.iterdir()
        if p.is_dir() and p.name.startswith("tests")
    ])

    if not test_suites:
        print(f"No test folders found under {TESTS_ROOT} (expected names starting with 'tests').")
        sys.exit(1)

    print(f"Found {len(test_suites)} test suites:")
    for s in test_suites:
        print(" -", s)

    for suite_dir in test_suites:
        suite_name = suite_dir.name
        RESULTS_DIR = RESULTS_ROOT / f"results_{suite_name}"
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)

        summary_path = RESULTS_DIR / "summary.tsv"
        summary_lines = ["test_id\tstatus\treason"]

        timing_path = RESULTS_DIR / "timing.tsv"
        timing_lines = ["test_id\tgraph_seconds\tgraphAbstract_seconds\tstatus"]

        graph_times = []
        abstract_times = []

        # Detect available test cases (prefer numeric subfolders)
        test_case_dirs = sorted([
            p for p in suite_dir.iterdir()
            if p.is_dir() and p.name.isdigit()
        ], key=lambda p: int(p.name))

        if not test_case_dirs:
            print(f"[{suite_name}] No numeric test folders found (expected 1,2,3,...)")
            continue

        print(f"[{suite_name}] Running {len(test_case_dirs)} tests...")

        for test_case_dir in test_case_dirs:
            i = int(test_case_dir.name)
            test_in = test_case_dir

            # Per-test output folder
            test_res_dir = RESULTS_DIR / str(i)
            graph_out_dir = test_res_dir / "graph_out"
            abs_out_dir = test_res_dir / "graphAbstract_out"
            diff_dir = test_res_dir / "diffs"
            diff_dir.mkdir(parents=True, exist_ok=True)

            # Run both programs (time them)
            ok1, msg1, t_graph = run_souffle(GRAPH_DL, test_in, graph_out_dir)
            if ok1:
                graph_times.append(t_graph)

            ok2, msg2, t_abs = run_souffle(ABSTRACT_DL, test_in, abs_out_dir)
            if ok2:
                abstract_times.append(t_abs)

            if not ok1:
                (diff_dir / "run_graph_error.txt").write_text(msg1, encoding="utf-8")
                summary_lines.append(f"{i}\tFAIL\tgraph run failed")
                timing_lines.append(f"{i}\t{t_graph:.6f}\t{t_abs:.6f}\tFAIL_graph")
                continue

            if not ok2:
                (diff_dir / "run_graphAbstract_error.txt").write_text(msg2, encoding="utf-8")
                summary_lines.append(f"{i}\tFAIL\tgraphAbstract run failed")
                timing_lines.append(f"{i}\t{t_graph:.6f}\t{t_abs:.6f}\tFAIL_graphAbstract")
                continue

            # Compare outputs
            all_ok = True
            reasons = []

            g_nodes = graph_out_dir / "nodesState.csv"
            g_map = graph_out_dir / "mapState.csv"

            a_nodes = abs_out_dir / "nodes.csv"
            a_edges = abs_out_dir / "edges.csv"

            ok, reason = compare_files(g_nodes, a_nodes, diff_dir / "nodes_diff.txt")
            if not ok:
                all_ok = False
                reasons.append(reason)

            ok, reason = compare_files(
                g_map,
                a_edges,
                diff_dir / "edges_diff.txt",
                ignore_singleton_graph=True,
                )
            if not ok:
                all_ok = False
                reasons.append(reason)

            timing_lines.append(
                f"{i}\t{t_graph:.6f}\t{t_abs:.6f}\t" + ("OK" if all_ok else "FAIL_compare")
            )

            if all_ok:
                summary_lines.append(f"{i}\tOK\t-")
                shutil.rmtree(test_res_dir, ignore_errors=True)  # keep only fails
            else:
                summary_lines.append(f"{i}\tFAIL\t" + "; ".join(reasons))

        # Write suite reports
        summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

        def avg(xs):
            return sum(xs) / len(xs) if xs else 0.0

        avg_graph = avg(graph_times)
        avg_abstract = avg(abstract_times)

        timing_lines.append("")
        timing_lines.append("# averages over successful runs")
        timing_lines.append(f"AVG\t{avg_graph:.6f}\t{avg_abstract:.6f}\t-")
        timing_lines.append(f"COUNT\t{len(graph_times)}\t{len(abstract_times)}\t-")

        timing_path.write_text("\n".join(timing_lines) + "\n", encoding="utf-8")

        print(f"[{suite_name}] Done. Wrote:")
        print(f"  - {summary_path}")
        print(f"  - {timing_path}")
        print(f"  (kept only failing tests' folders)")

    print("All suites completed.")



if __name__ == "__main__":
    main()
