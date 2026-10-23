#!/usr/bin/env python3
"""Fail when a Stryker report has more survived + no-coverage mutants than the baseline.

Usage: check-mutation-baseline.py <baseline.json> <job name> <StrykerOutput dir>
"""

import json
import sys
from pathlib import Path


def main() -> None:
    baseline_path, name, root = sys.argv[1:]
    jobs = json.loads(Path(baseline_path).read_text())["jobs"]
    if name not in jobs:
        sys.exit("no baseline for " + name)
    cap = jobs[name]["survived"] + jobs[name]["noCoverage"]
    reports = sorted(Path(root).glob("**/mutation-report.json"), key=lambda path: path.stat().st_mtime)
    if not reports:
        sys.exit("no mutation report under " + root)
    report = json.loads(reports[-1].read_text())
    survived = 0
    no_coverage = 0
    for info in report["files"].values():
        for mutant in info["mutants"]:
            status = mutant["status"]
            if status == "Survived":
                survived += 1
            elif status == "NoCoverage":
                no_coverage += 1
    undetected = survived + no_coverage
    print(
        name
        + ": survived "
        + str(survived)
        + " no-coverage "
        + str(no_coverage)
        + " undetected "
        + str(undetected)
        + " cap "
        + str(cap)
    )
    if undetected > cap:
        sys.exit(1)


if __name__ == "__main__":
    main()
