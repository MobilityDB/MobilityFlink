#!/usr/bin/env bash
# Regenerate tools/baseline/streaming-relevance-baseline.json — the generator
# input consumed by codegen_facades.py.
#
# The baseline is a deterministic classification of the MEOS public API into
# streaming tiers. It is derived from the MEOS-API catalog (meos-idl.json),
# which is itself produced by running MEOS-API/run.py over the MEOS headers at
# the commit recorded in tools/meos-source-commit.txt. See GENERATION.md.
#
# Usage:
#   tools/regen_baseline.sh <meos-idl.json> [source-ref]
#
#   <meos-idl.json>  the MEOS-API catalog (from MEOS-API/run.py over the MEOS
#                    headers at tools/meos-source-commit.txt)
#   [source-ref]     optional portable provenance label recorded in the artifact
#
# The classifier is deterministic: the same catalog always yields the same
# baseline, so a refresh is: rebuild the catalog at the tracked commit, re-run
# this script, commit the diff.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
catalog="${1:?usage: regen_baseline.sh <meos-idl.json> [source-ref]}"
source_ref="${2:-meos-idl.json (MEOS-API run.py over tools/meos-source-commit.txt)}"
out="$here/baseline/streaming-relevance-baseline.json"

python3 "$here/classify_streaming_relevance.py" "$catalog" \
  --source-ref "$source_ref" -o "$out"

echo "regenerated $out"
