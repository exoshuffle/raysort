from absl import app
from absl import flags

import numpy as np

KEY_SIZE = 10
RECORD_SIZE = 100

FLAGS = flags.FLAGS

flags.DEFINE_string("input_file", None, "Path to binary data file.", short_name="i")
flags.DEFINE_string(
    "output_file", None, "Path to output file (optional).", short_name="o"
)
flags.DEFINE_bool(
    "key_only",
    False,
    f"If set, only decode the first {KEY_SIZE} bytes of every {RECORD_SIZE} bytes.",
)
flags.DEFINE_integer("limit", -1, "Only decode this many bytes; if -1, decode all.")
flags.mark_flag_as_required("input_file")


def main(argv):
    print("Decoding", FLAGS.input_file)
    if FLAGS.limit >= 0:
        print(f"Decoding the first {FLAGS.limit} bytes")
    if FLAGS.key_only:
        print("Only decoding keys")

    arr = np.fromfile(FLAGS.input_file, dtype=np.uint8, count=FLAGS.limit)

    output_file = FLAGS.output_file or FLAGS.input_file + ".txt"
    with open(output_file, "w") as fout:
        for idx, x in np.ndenumerate(arr):
            (i,) = idx
            if not (FLAGS.key_only and i % RECORD_SIZE >= KEY_SIZE):
                print(f"{x:02x} ", end="", file=fout)
            if i % RECORD_SIZE == RECORD_SIZE - 1:
                print("", file=fout)


if __name__ == "__main__":
    app.run(main)
