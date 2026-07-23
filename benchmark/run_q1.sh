#!/usr/bin/env bash
# Runs Query 1 - High-Risk Zone Proximity Monitoring
# Usage : ./run_q1.sh
# Requirements : flink-processor working directory (pom.xml + Dockerfile)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" 1 "$@"
