#!/bin/bash
#
# Local tweet+stock ingestion job.
#
# The free X syndication endpoint only serves residential IPs (GitHub Actions'
# datacenter IPs get 429'd), so tweet ingestion MUST run from a local machine.
# This wrapper is invoked by the launchd agent (com.moneymaker.ingest) a few
# times a day to keep merged_data fresh so the sentiment/ML path has data to act on.
#
# Runs run_pipeline.py, appends timestamped output to logs/ingest.log, and keeps
# the log from growing without bound. Safe to run by hand too:  scripts/local_ingest.sh
set -o pipefail

# Project root = parent of this script's dir (portable, no hardcoded user path).
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DIR" || exit 1

PY="/Library/Frameworks/Python.framework/Versions/3.11/bin/python3"
[ -x "$PY" ] || PY="$(command -v python3)"

LOG="$DIR/logs/ingest.log"
mkdir -p "$DIR/logs"

# Trim the log if it gets large (keep the last ~2000 lines).
if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt 5000 ]; then
    tail -n 2000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi

echo "" >> "$LOG"
echo "========== $(date '+%Y-%m-%d %H:%M:%S %Z') — ingest start ==========" >> "$LOG"
"$PY" run_pipeline.py >> "$LOG" 2>&1
rc=$?
echo "========== $(date '+%Y-%m-%d %H:%M:%S %Z') — ingest done (exit $rc) ==========" >> "$LOG"
exit $rc
