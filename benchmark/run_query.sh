#!/usr/bin/env bash

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; }
banner()  { echo -e "\n${BOLD}${CYAN}═══════════════════════════════════════════${RESET}"; \
            echo -e "${BOLD}${CYAN}  $*${RESET}"; \
            echo -e "${BOLD}${CYAN}═══════════════════════════════════════════${RESET}\n"; }

declare -A QUERY_CLASS=(
  [1]="aisdata.Query1_Main"
  [2]="aisdata.Query2_Main"
  [3]="aisdata.Query3_Main"
  [4]="aisdata.Query4_Main"
  [5]="aisdata.Query5_Main"
  [6]="aisdata.Query6_Main"
  [7]="aisdata.Query7_Main"
  [8]="aisdata.Query8_V2_Main"
  [9]="aisdata.Query9_Main"
)

declare -A QUERY_DESC=(
  [1]="High-Risk Zone Proximity Monitoring"
  [2]="Brake System Monitoring"
  [3]="Trajectory Creation"
  [4]="Trajectory Creation in a Restricted Space"
  [5]="Trajectory Creation and High-Speed Alert"
  [6]="Positional Divergence for a Device"
  [7]="Global Closest Device Pairs (Top-k)"
  [8]="Trajectory Denoising — MEOS native EKF (marianaGarcez fork)"
  [9]="Windowed Per-Device kNN Join"
)

declare -A QUERY_DOCKERFILE=(
  [1]="" [2]="" [3]="" [4]="" [5]="" [6]="" [7]="" [8]="Dockerfile_q8_meos_kalman" [9]=""
)

QUERY_NUM="${1:-}"
if [[ -z "$QUERY_NUM" || ! "$QUERY_NUM" =~ ^[1-9]$ ]]; then
  error "Usage : $(basename "$0") <1-9>"
  echo ""
  echo "Available queries :"
  for n in $(seq 1 9); do
    echo -e "  ${BOLD}$n${RESET}  — ${QUERY_DESC[$n]}"
  done
  exit 1
fi

CLASS="${QUERY_CLASS[$QUERY_NUM]}"
DESC="${QUERY_DESC[$QUERY_NUM]}"
CUSTOM_DF="${QUERY_DOCKERFILE[$QUERY_NUM]}"

banner "Query $QUERY_NUM — $DESC"

info "Verifying requirements..."

# Docker
if ! command -v docker &>/dev/null; then
  error "docker is not installed or missing in the PATH."
  exit 1
fi

# docker compose (v2) ou docker-compose (v1)
if docker compose version &>/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
elif command -v docker-compose &>/dev/null; then
  COMPOSE_CMD="docker-compose"
else
  error "Neither 'docker compose' or 'docker-compose' are available."
  exit 1
fi
success "Docker OK  (compose : $COMPOSE_CMD)"

# Maven
if ! command -v mvn &>/dev/null; then
  error "mvn is not install or missing in the PATH."
  exit 1
fi
success "Maven OK"

# Working directory : pom.xml required
if [[ ! -f "pom.xml" ]]; then
  error "pom.xml missing."
  exit 1
fi

# Dockerfile source
BASE_DOCKERFILE="${CUSTOM_DF:-Dockerfile}"
if [[ ! -f "$BASE_DOCKERFILE" ]]; then
  error "Dockerfile source '$BASE_DOCKERFILE' missing."
  exit 1
fi
success "Dockerfile source : $BASE_DOCKERFILE"

if [[ ! -f "wait-for-it.sh" ]]; then
  error "wait-for-it.sh missing."
  exit 1
fi
success "wait-for-it.sh OK"

Q8_STASHED=()

stash_q8_files() {
  while IFS= read -r -d '' f; do
    mv "$f" "${f}.bak"
    Q8_STASHED+=("$f")
    warn "Temporarily excluded from the compilation : $f"
  done < <(grep -rl --include="*.java" -Z "temporal_ext_kalman_filter" src/ 2>/dev/null || true)
}

restore_q8_files() {
  for f in "${Q8_STASHED[@]:-}"; do
    [[ -f "${f}.bak" ]] && mv "${f}.bak" "$f"
  done
}

trap 'restore_q8_files; rm -f "$TMP_DOCKERFILE" 2>/dev/null || true' EXIT

echo ""
info "Step 1/3: Build Maven (mvn clean package -DskipTests) ..."

if [[ "$QUERY_NUM" != "8" ]]; then
  info "Temporarily masking Query8 files (temporal_ext_kalman_filter not available before the patch)..."
  stash_q8_files
  if [[ ${#Q8_STASHED[@]} -eq 0 ]]; then
    warn "No Query8 files found to mask."
  fi
fi

if ! mvn clean package -DskipTests -DqueryMainClass="${CLASS}" -q; then
  error "Maven build failed."
  exit 1
fi

restore_q8_files
Q8_STASHED=()

success "Build Maven terminé."

JAR_PATH=""
for candidate in \
    "target/flink-kafka2postgres-1.0-SNAPSHOT.jar" \
    "jar/JMEOS-fat.jar" \
    target/*.jar jar/*.jar; do
  if [[ -f "$candidate" ]]; then
    JAR_PATH="$candidate"
    break
  fi
done

if [[ -z "$JAR_PATH" ]]; then
  error "No JAR found after building maven (target/ & jar/ have been verified)."
  exit 1
fi
success "JAR detected : $JAR_PATH"

echo ""
info "Step 2/3: Patching Dockerfile & building the image..."
info "  Class targeted : ${CLASS}"

if ! grep -q "^ENTRYPOINT" "${BASE_DOCKERFILE}"; then
  error "No ENTRYPOINT found in ${BASE_DOCKERFILE}."
  exit 1
fi

TMP_DF=$(mktemp /tmp/Dockerfile.query.XXXXXX)
sed "s@\"[a-zA-Z]*data\.[A-Za-z0-9_]*\"\]\$@\"${CLASS}\"]@" "${BASE_DOCKERFILE}" > "${TMP_DF}"

if ! grep -q "${CLASS}" "${TMP_DF}"; then
  error "The patch failed: '${CLASS}' absent from the buffer Dockerfile."
  error "ENTRYPOINT found : $(grep '^ENTRYPOINT' ${BASE_DOCKERFILE})"
  rm -f "${TMP_DF}"; exit 1
fi

PATCHED_EP=$(grep "^ENTRYPOINT" "${TMP_DF}")
success "Dockerfile : ${PATCHED_EP}"

info "Building the Docker image 'flink-processor'..."
if ! docker build -f "${TMP_DF}" -t flink-processor .; then
  error "Docker build failed."
  rm -f "${TMP_DF}"; exit 1
fi
rm -f "${TMP_DF}"

BUILT_EP=$(docker inspect --format='{{json .Config.Entrypoint}}' flink-processor 2>/dev/null || echo "")
if [[ "${BUILT_EP}" != *"${CLASS}"* ]]; then
  error "The built image does not contain '${CLASS}' in its ENTRYPOINT."
  error "ENTRYPOINT detected : ${BUILT_EP}"
  exit 1
fi
success "Image flink-processor built."
info  "  ENTRYPOINT : ${BUILT_EP}"

echo ""
info "Step 3/3: running docker compose..."

_cleanup() {
  echo ""
  warn "Stopping..."
  $COMPOSE_CMD down --remove-orphans 2>/dev/null || true
  success "Docker compose stopped."
  exit 0
}
trap '_cleanup' INT TERM

info "Stopping the existing containers..."
$COMPOSE_CMD down --remove-orphans 2>/dev/null || true

echo ""
info "Running with ${COMPOSE_CMD} up --force-recreate ..."
info "Active class : ${CLASS}"
echo -e "${BOLD}─────────────────────────────────────────────────────${RESET}"

${COMPOSE_CMD} up --force-recreate