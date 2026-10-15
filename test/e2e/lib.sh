# shellcheck shell=bash
# Shared helpers for the live e2e scripts; expects NAMESPACE and READY_TIMEOUT.

log() { echo "[$(date +%H:%M:%S)] $*"; }
fail() { log "FAIL: $*"; kubectl -n "$NAMESPACE" get instance,milvus,pods -o wide || true; exit 1; }

wait_ready() {
  local instance=$1 phase message
  for _ in $(seq $((READY_TIMEOUT / 10))); do
    phase=$(kubectl -n "$NAMESPACE" get instance "$instance" -o jsonpath='{.status.phase}')
    [[ "$phase" == "Ready" ]] && return 0
    [[ "$phase" == "Failed" ]] && fail "$instance is Failed"
    sleep 10
  done
  message=$(kubectl -n "$NAMESPACE" get instance "$instance" -o jsonpath='{.status.message}')
  fail "$instance not Ready after ${READY_TIMEOUT}s (phase=$phase, message=$message)"
}

check_components() {
  local instance=$1 not_ready
  not_ready=$(kubectl -n "$NAMESPACE" get instance "$instance" -o json |
    jq -r '[.status.components[]? | select(.replicas != .readyReplicas or .replicas == 0) | .name] | join(",")')
  [[ -z "$not_ready" ]] || fail "$instance components not ready: $not_ready"
  [[ $(kubectl -n "$NAMESPACE" get instance "$instance" -o json | jq '.status.components | length') -gt 0 ]] ||
    fail "$instance reports no status.components"
}

check_images() {
  local instance=$1 version=$2 images stale
  # Query nodes roll blue/green: the idle group is scaled to 0 and keeps the old image.
  images=$(kubectl -n "$NAMESPACE" get deploy -l "app.kubernetes.io/instance=$instance,app.kubernetes.io/name=milvus" \
    -o jsonpath='{range .items[*]}{.spec.replicas} {.metadata.name}={.spec.template.spec.containers[0].image}{"\n"}{end}' |
    awk '$1 > 0 {print $2}')
  [[ -n "$images" ]] || fail "$instance has no Milvus deployments"
  stale=$(grep -v "=.*:v$version\$" <<<"$images" || true)
  [[ -z "$stale" ]] || fail "$instance not on $version: $stale"
}

# Moves an instance to version and waits until every Milvus pod runs it.
upgrade_to() {
  local instance=$1 version=$2
  kubectl -n "$NAMESPACE" patch instance "$instance" --type merge -p "{\"spec\":{\"version\":\"$version\"}}"
  sleep 20
  wait_ready "$instance"
  check_images "$instance" "$version"
}

delete_and_check() {
  local instance=$1 leftovers
  kubectl -n "$NAMESPACE" delete instance "$instance" --wait=true --timeout=300s
  # The operator uninstalls the dependency releases after the Instance is gone.
  for _ in $(seq 60); do
    if leftovers=$(kubectl -n "$NAMESPACE" get milvus,pods,pvc,statefulsets,deployments -o name); then
      leftovers=$(grep -- "/$instance" <<<"$leftovers" || true)
      [[ -z "$leftovers" ]] && return 0
    fi
    sleep 10
  done
  fail "$instance left behind: $leftovers"
}
