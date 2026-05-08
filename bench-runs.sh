#!/bin/bash
# Run the perf benchmarks 3 times at QUERY_COUNT=50, 200, 1000.
# Each run's output is tee'd to $HOME/bench-results/<phase>-q<N>-run<R>.txt
# so we can aggregate medians-of-medians after the fact.
#
# Usage:
#   bash bench-runs.sh native        # 9 runs of native-backend-performance (run on IBM i)
#   bash bench-runs.sh rm            # 9 runs of backend-performance / rm-connector-js (on IBM i)
#   bash bench-runs.sh both          # native + rm back-to-back (18 runs total)
#   bash bench-runs.sh loopback-mux  # 9 runs of remote-mapepire-multiplex on IBM i (IBMI_HOST=localhost)
#   bash bench-runs.sh remote-mux    # 9 runs of remote-mapepire-multiplex from a remote workstation
#                                    # (IBMI_HOST=<ibm-i-hostname>)
#
# Prereqs: IBMI_HOST / IBMI_USER / IBMI_PASSWORD exported, SAMPLE schema accessible.
# The two -mux phases run the same test file; the only difference is the IBMI_HOST you
# export before running. File naming makes loopback vs remote unambiguous.

set -e

if [ -z "$IBMI_HOST" ] || [ -z "$IBMI_USER" ] || [ -z "$IBMI_PASSWORD" ]; then
  echo "Set IBMI_HOST, IBMI_USER, IBMI_PASSWORD before running."
  exit 1
fi

mkdir -p "$HOME/bench-results"

phase=${1:-both}

run_native() {
  for q in 50 200 1000; do
    for run in 1 2 3; do
      out="$HOME/bench-results/native-q${q}-run${run}.txt"
      echo ""
      echo ">>> NATIVE q=$q run=$run -> $out"
      QUERY_COUNT=$q npm run test:performance -- \
        --testPathPatterns=native-backend-performance \
        2>&1 | tee "$out"
    done
  done
}

run_rm() {
  # Filter by describe-block name to run only the rm-connector-js suite
  # (the native file's describe is "Native Backend Performance").
  for q in 50 200 1000; do
    for run in 1 2 3; do
      out="$HOME/bench-results/rm-q${q}-run${run}.txt"
      echo ""
      echo ">>> RM q=$q run=$run -> $out"
      QUERY_COUNT=$q npm run test:performance -- \
        --testNamePattern='^Backend Performance' \
        2>&1 | tee "$out"
    done
  done
}

run_mux() {
  # $1 = label ("loopback" or "remote") - used in filename only.
  local label="$1"
  for q in 50 200 1000; do
    for run in 1 2 3; do
      out="$HOME/bench-results/${label}-mux-q${q}-run${run}.txt"
      echo ""
      echo ">>> ${label^^}-MUX q=$q run=$run -> $out"
      QUERY_COUNT=$q npm run test:performance -- \
        --testPathPatterns=remote-mapepire-multiplex \
        2>&1 | tee "$out"
    done
  done
}

case "$phase" in
  native)       run_native ;;
  rm)           run_rm ;;
  both)         run_native; run_rm ;;
  loopback-mux) run_mux loopback ;;
  remote-mux)   run_mux remote ;;
  *)            echo "Unknown phase: $phase (use: native | rm | both | loopback-mux | remote-mux)"; exit 1 ;;
esac

echo ""
echo "Done. Results in $HOME/bench-results/"
ls $HOME/bench-results/
