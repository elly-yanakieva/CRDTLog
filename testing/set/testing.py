import os
import subprocess
import filecmp
import difflib

# Configuration
DATALOG_FILE = '/home/elena/Documents/SouffleCRDTs/crdts/mapSet/mapSet.dl'   # change if your top-level DL file is named differently
OUTPUT_RELATION_FILE = 'mapState.csv'  # Name of output relation file Soufflé writes
EXPECTED_FILE = 'expected.csv'
OUTPUT_DIR_NAME = 'output'
INPUT_DIR_NAME = 'input'
EXPECTED_DIR_NAME = 'expectedOutput'

TESTS_DIR = 'tests'
SUMMARY_FILE = os.path.join(TESTS_DIR, 'results.txt')

def read_file_lines(filepath):
    with open(filepath, 'r') as f:
        return set(line.strip() for line in f if line.strip())

def compare_files(output_path, expected_path):
    output_lines = read_file_lines(output_path)
    expected_lines = read_file_lines(expected_path)

    if output_lines == expected_lines:
        return True, ""
    else:
        diff = '\n'.join(difflib.unified_diff(
            sorted(expected_lines),
            sorted(output_lines),
            fromfile='expected.csv',
            tofile='output.csv',
            lineterm=''
        ))
        return False, diff

def run_test(test_path, test_name):
    input_dir = os.path.join(test_path, INPUT_DIR_NAME)
    output_dir = os.path.join(test_path, OUTPUT_DIR_NAME)
    expected_dir = os.path.join(test_path, EXPECTED_DIR_NAME)
    output_file = os.path.join(output_dir, OUTPUT_RELATION_FILE)
    expected_file = os.path.join(expected_dir, EXPECTED_FILE)
    test_log = os.path.join(test_path, f"{test_name}.txt")

    # Make sure output dir exists
    os.makedirs(output_dir, exist_ok=True)

    # Run Soufflé
    try:
        subprocess.run(
            ["souffle", DATALOG_FILE, f"-F{input_dir}", f"-D{output_dir}"],
            check=True,
            capture_output=True,
            text=True
        )
    except subprocess.CalledProcessError as e:
        with open(test_log, 'w') as f:
            f.write(f"[ERROR] Soufflé failed to run:\n{e.stderr}")
        return False

    # Compare output
    if not os.path.exists(output_file):
        with open(test_log, 'w') as f:
            f.write(f"[ERROR] Output file '{OUTPUT_RELATION_FILE}' not found.\n")
        return False

    success, diff = compare_files(output_file, expected_file)
    with open(test_log, 'w') as f:
        if success:
            f.write("[PASS] Output matches expected.\n")
        else:
            f.write("[FAIL] Output does not match expected.\n")
            f.write("Diff:\n")
            f.write(diff + '\n')
    return success

def main():
    test_results = []

    for test_name in sorted(os.listdir(TESTS_DIR)):
        test_path = os.path.join(TESTS_DIR, test_name)
        if not os.path.isdir(test_path):
            continue

        print(f"Running {test_name}...")
        passed = run_test(test_path, test_name)
        test_results.append((test_name, passed))

    # Write summary
    with open(SUMMARY_FILE, 'w') as f:
        for test_name, passed in test_results:
            status = "PASS" if passed else "FAIL"
            f.write(f"{test_name}: {status}\n")

    print("Done. See 'results.txt' for summary.")

if __name__ == '__main__':
    main()
