# Project skeleton

Run `just verify` from the repository root to build the project and run the skeleton with Spark. The development container provides the required tools.

Pass a different input file with `just verify path/to/input.jsonl`; paths are relative to the repository root.

The `verify` recipe checks result formatting. To check saved output separately:

```sh
python3 scripts/check_output.py < output.txt
```

The checker counts valid result lines and errors; it does not check the correctness or completeness of the results.

