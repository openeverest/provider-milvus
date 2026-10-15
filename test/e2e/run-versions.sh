#!/usr/bin/env bash
# Live version matrix against a real cluster with OpenEverest and this provider
# installed (operator running). For every version bundle the installed Provider
# offers: create a standalone instance at that version, check every Milvus pod
# runs it, write data, upgrade to the newest bundle of the same build (plain or
# -gpu) and verify the data, then delete the instance and check nothing is left
# behind. Versions run one at a time to keep the footprint small.
# Usage: [VERSIONS="2.6.10 2.6.15"] test/e2e/run-versions.sh [namespace]
set -euo pipefail

NAMESPACE=${1:-milvus-e2e-versions}
DIR=$(cd "$(dirname "$0")" && pwd)
READY_TIMEOUT=${READY_TIMEOUT:-900}
PROVIDER=${PROVIDER:-milvus}

# shellcheck source=test/e2e/lib.sh
. "$DIR/lib.sh"

provider_spec=$(kubectl get provider "$PROVIDER" -o json)
DEFAULT_VERSION=$(jq -r '.spec.defaultVersion' <<<"$provider_spec")
BUNDLES=$(jq -r '.spec.versions[].name' <<<"$provider_spec")
VERSIONS=${VERSIONS:-$BUNDLES}

# The newest bundle of the same build: 2.6.15-gpu upgrades to the default's -gpu bundle.
upgrade_target() {
  local version=$1 build
  build=${version#"${version%%-*}"}
  if grep -qx -- "$DEFAULT_VERSION$build" <<<"$BUNDLES"; then
    echo "$DEFAULT_VERSION$build"
  else
    echo "$DEFAULT_VERSION"
  fi
}

create_instance() {
  local instance=$1 version=$2
  kubectl -n "$NAMESPACE" apply -f - <<EOF
apiVersion: core.openeverest.io/v1alpha1
kind: Instance
metadata:
  name: ${instance}
spec:
  providerRef:
    name: ${PROVIDER}
  version: "${version}"
  topology:
    type: standalone
  components:
    standalone:
      type: milvus
      replicas: 1
      resources:
        requests:
          cpu: 500m
          memory: 1Gi
        limits:
          cpu: "1"
          memory: 2Gi
EOF
}

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

log "kubernetes $(kubectl version -o json | jq -r .serverVersion.gitVersion)," \
  "operator $(kubectl get deploy -A -o jsonpath='{..containers[*].image}' | tr ' ' '\n' | grep -m1 milvus-operator)," \
  "default version $DEFAULT_VERSION"

for version in $VERSIONS; do
  instance="e2e-v${version//./-}"
  target=$(upgrade_target "$version")
  log "$version: create, smoke, upgrade to $target"
  create_instance "$instance" "$version"
  wait_ready "$instance"
  check_components "$instance"
  check_images "$instance" "$version"
  if [[ "$target" == "$version" ]]; then
    "$DIR/run-smoke.sh" "$NAMESPACE" "$instance"
  else
    MODE=write "$DIR/run-smoke.sh" "$NAMESPACE" "$instance"
    upgrade_to "$instance" "$target"
    check_components "$instance"
    MODE=verify "$DIR/run-smoke.sh" "$NAMESPACE" "$instance"
  fi
  delete_and_check "$instance"
  log "$version: PASS"
done

log "PASS"
