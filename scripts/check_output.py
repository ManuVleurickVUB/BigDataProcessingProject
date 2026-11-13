#!/usr/bin/env python3
"""Count valid and invalid RESULT lines read from standard input."""

import re
import sys

# Each entry contains the number of integer fields and whether a decimal follows.
FORMATS = {
    "POP": (2, False),
    "TOP": (4, False),
    "CNT": (3, False),
    "SIM": (3, True),
    "REC": (3, False),
    "SES": (3, True),
}
DECIMAL = re.compile(r"[+-]?[0-9]+\.[0-9]{4}\Z")
INTEGER = re.compile(r"[+-]?[0-9]+\Z")


def valid_result(line):
    fields = re.split(r"\t+", line.rstrip("\r\n"))
    if len(fields) < 2 or fields[0] != "RESULT" or fields[1] not in FORMATS:
        return False
    integers, has_decimal = FORMATS[fields[1]]
    values = fields[2:]
    if len(values) != integers + has_decimal:
        return False
    if not all(INTEGER.fullmatch(value) for value in values[:integers]):
        return False
    return not has_decimal or bool(DECIMAL.fullmatch(values[-1]))


def main():
    parsed = errors = 0
    for line in sys.stdin:
        if line.startswith("RESULT"):
            if valid_result(line):
                parsed += 1
            else:
                errors += 1
    print("\n=== Result format check ===")
    print(f"Valid lines: {parsed}")
    print(f"Errors:      {errors}")
    print()
    print(f"Project: {'REJECTED' if errors else 'ACCEPTED'}")
    print("---------------------------")


if __name__ == "__main__":
    main()
