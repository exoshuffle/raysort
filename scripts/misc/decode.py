import numpy as np
from absl import app, flags

HEADER_SIZE = 10
RECORD_SIZE = 100

FLAGS = flags.FLAGS

flags.DEFINE_string(
    "input_file",
    None,
    "Path to binary data file.",
    short_name="i",
)
flags.DEFINE_string(
    "output_file",
    None,
    "Path to output file (optional).",
    short_name="o",
)
flags.DEFINE_bool(
    "header_only",
    True,
    f"If set, only decode the first {HEADER_SIZE} bytes of every {RECORD_SIZE} bytes.",
)
flags.DEFINE_integer(
    "count",
    -1,
    "Only decode this many bytes; if -1, decode all.",
    short_name="c",
)
flags.mark_flag_as_required("input_file")


def main(argv):
    del argv  # Unused.
    print("Decoding", FLAGS.input_file)
    if FLAGS.count >= 0:
        print(f"Decoding the first {FLAGS.count} bytes")
    if FLAGS.header_only:
        print("Only decoding headers")

    arr = np.fromfile(FLAGS.input_file, dtype=np.uint8, count=FLAGS.count)

    output_file = FLAGS.output_file or FLAGS.input_file + ".txt"
    with open(output_file, "w") as fout:
        for (i,), x in np.ndenumerate(arr):
            if not (FLAGS.header_only and i % RECORD_SIZE >= HEADER_SIZE):
                print(f"{x:02x} ", end="", file=fout)
            if i % RECORD_SIZE == RECORD_SIZE - 1:
                print("", file=fout)


if __name__ == "__main__":
    app.run(main)
