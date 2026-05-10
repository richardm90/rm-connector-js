#!/bin/bash
# Run the rm-connector-js performance benchmarks 3 times at QUERY_COUNT=50, 200, 1000.
# Each run's output is tee'd to $HOME/bench-results/<phase>-<location>-q<N>-run<R>.txt
# so we can aggregate medians-of-medians after the fact.
#
# Phases (matching docs/PERFORMANCE-COMPARISON.md Section 4 scenarios):
#   native             — Scenario 5 (native drivers baseline, no rm-connector-js wrapper)
#   rm                 — Scenario 1 (rm default: onAttach=true, multiplex=false, pool=5)
#   rm-no-onattach     — Scenario 2 (onAttach=false, multiplex=false)
#   rm-mux             — Scenario 3 (multiplex=true, pool=5)
#   rm-mux-keepalive   — Scenario 4 (multiplex=true, keepalive=0.05, pool=5)
#
# Set IBMI_HOST=localhost when running on the IBM i (loopback) or to the remote
# IBM i hostname when running from a workstation. The output filename labels
# the run as "loopback" or "remote" accordingly so local and remote runs don't
# collide in $HOME/bench-results/.
#
# Usage:
#   export IBMI_HOST=localhost          # or the remote IBM i hostname
#   export IBMI_USER=<user>
#   export IBMI_PASSWORD=<pass>
#   bash tests/performance/bench-runs.sh native
#   bash tests/performance/bench-runs.sh rm
#   bash tests/performance/bench-runs.sh rm-no-onattach
#   bash tests/performance/bench-runs.sh rm-mux
#   bash tests/performance/bench-runs.sh rm-mux-keepalive
#
# Note: this script intentionally does NOT use `set -e`. Some scenarios (notably
# rm-no-onattach with high concurrency) can produce test failures from pool
# exhaustion; we want the harness to keep running and capture the full set of
# result files so we can analyse them afterwards.

if [ -z "$IBMI_HOST" ] || [ -z "$IBMI_USER" ] || [ -z "$IBMI_PASSWORD" ]; then
  echo "Set IBMI_HOST, IBMI_USER, IBMI_PASSWORD before running."
  exit 1
fi

mkdir -p "$HOME/bench-results"

phase=${1:-}

# Derive location label from IBMI_HOST: "loopback" if localhost, else "remote".
if [ "$IBMI_HOST" = "localhost" ] || [ "$IBMI_HOST" = "127.0.0.1" ]; then
  location="loopback"
else
  location="remote"
fi

run_native() {
  for q in 50 200 1000; do
    for run in 1 2 3; do
      out="$HOME/bench-results/native-${location}-q${q}-run${run}.txt"
      echo ""
      echo ">>> NATIVE (${location}) q=$q run=$run -> $out"
      QUERY_COUNT=$q npm run test:performance -- \
        --testPathPatterns=native-backend-performance \
        2>&1 | tee "$out"
    done
  done
}

# Run the rm-backend-performance test with a phase-specific set of env vars.
# Args: phase_label, [extra ENV=VAL pairs...]
run_rm() {
  local label="$1"; shift
  local env_setup=("$@")
  for q in 50 200 1000; do
    for run in 1 2 3; do
      out="$HOME/bench-results/${label}-${location}-q${q}-run${run}.txt"
      echo ""
      echo ">>> ${label^^} (${location}) q=$q run=$run -> $out"
      env "${env_setup[@]}" QUERY_COUNT=$q npm run test:performance -- \
        --testPathPatterns=rm-backend-performance \
        2>&1 | tee "$out"
    done
  done
}

case "$phase" in
  native)
    run_native
    ;;
  rm)
    run_rm rm
    ;;
  rm-no-onattach)
    run_rm rm-no-onattach RM_ON_ATTACH=false
    ;;
  rm-mux)
    run_rm rm-mux RM_MULTIPLEX=true
    ;;
  rm-mux-keepalive)
    run_rm rm-mux-keepalive RM_MULTIPLEX=true RM_KEEPALIVE=0.05
    ;;
  "")
    echo "Specify a phase: native | rm | rm-no-onattach | rm-mux | rm-mux-keepalive"
    exit 1
    ;;
  *)
    echo "Unknown phase: $phase (use: native | rm | rm-no-onattach | rm-mux | rm-mux-keepalive)"
    exit 1
    ;;
esac

echo ""
echo "Done. Results in $HOME/bench-results/"
ls "$HOME/bench-results/"
