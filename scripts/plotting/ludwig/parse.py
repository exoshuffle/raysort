import json
import os
import re

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

RUNS = [
    ("Global Shuffle", f"{SCRIPT_DIR}/full_shuffle", "train_full_shuffle.log"),
    ("Partial Shuffle", f"{SCRIPT_DIR}/no_shuffle", "train_no_shuffle.log"),
]
TRAINING_JSON = "training_statistics.json"

NUM_EPOCHS = 20
TOTAL_STEPS = 4700
STEPS_PER_EPOCH = TOTAL_STEPS // NUM_EPOCHS


def get_run_data(run_name: str, run_log: str):
    """Get the timing and accuracy data for a single run."""
    with open(f"{run_name}/{run_log}", encoding="utf8") as fin:
        lines = fin.readlines()

    def get_epoch_time(epoch):
        steps = STEPS_PER_EPOCH * epoch
        pattern = f" {steps}/{TOTAL_STEPS}"
        matched_line = [line for line in lines if pattern in line][0]
        time_matches = re.search(r"\[(\d+):(\d+)<", matched_line)
        assert time_matches is not None
        minutes, seconds = time_matches.groups()
        return int(minutes) * 60 + int(seconds)

    epoch_times = [get_epoch_time(i + 1) for i in range(NUM_EPOCHS)]

    with open(f"{run_name}/{TRAINING_JSON}", encoding="ascii") as fin:
        data = json.load(fin)

    accuracy = data["validation"]["label"]["accuracy"]

    return epoch_times, accuracy


def main():
    """Main function."""
    with open("ludwig_distributed.csv", "w", encoding="ascii") as fout:
        print("run,epoch,time,accuracy")
        for run_name, run_dir, run_log in RUNS:
            epoch_times, accuracy = get_run_data(run_dir, run_log)
            for i, (t, a) in enumerate(zip(epoch_times, accuracy)):
                print(f"{run_name},{i+1},{t},{a}")


if __name__ == "__main__":
    main()
