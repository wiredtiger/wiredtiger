#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "usage: $0 <wt_root> <config_path> <workers> <output_dir> [--stop-on-fail]"
  echo ""
  echo "  wt_root      path to WiredTiger repo root (e.g. /data/wiredtiger)"
  echo "  config_path  path to test/format CONFIG file"
  echo "  workers      number of parallel tmux workers"
  echo "  output_dir   directory to collect per-worker logs"
  echo "  --stop-on-fail  (optional) print a reminder to kill sessions on first failure"
  exit 1
fi

WT_ROOT="$1"
CONFIG_PATH="$2"
WORKERS="$3"
OUT_DIR="$4"
STOP_ON_FAIL="${5:-}"

mkdir -p "$OUT_DIR"

timestamp() {
  date +"%Y%m%d_%H%M%S"
}

for i in $(seq 1 "$WORKERS"); do
  RUN_DIR="$OUT_DIR/worker_${i}_$(timestamp)"
  mkdir -p "$RUN_DIR"

  SESSION="wt_format_${i}_$(date +%s)"

  tmux new-session -d -s "$SESSION" \
    "cd \"$WT_ROOT/test/format\" && \
     echo \"worker=$i\" > \"$RUN_DIR/meta.txt\" && \
     echo \"config=$CONFIG_PATH\" >> \"$RUN_DIR/meta.txt\" && \
     cp \"$CONFIG_PATH\" \"$RUN_DIR/CONFIG\" && \
     WT_TRACING=1 ./t -c \"$CONFIG_PATH\" > \"$RUN_DIR/stdout.log\" 2> \"$RUN_DIR/stderr.log\"; \
     echo \$? > \"$RUN_DIR/exit_code.txt\""

  echo "started worker $i in tmux session $SESSION -> $RUN_DIR"
done

echo ""
echo "all $WORKERS workers started"
echo "output dir: $OUT_DIR"
echo ""
echo "monitor with:"
echo "  tmux ls"
echo "  cat $OUT_DIR/worker_*/exit_code.txt"

if [[ "$STOP_ON_FAIL" == "--stop-on-fail" ]]; then
  echo ""
  echo "stop-on-fail mode: watch exit_code.txt files; on first non-zero exit, kill"
  echo "remaining sessions with: tmux kill-session -t <session>"
fi
