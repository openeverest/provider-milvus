#!/usr/bin/env bash
# Live TLS end-to-end run against a real cluster with OpenEverest and this
# provider installed (operator running). Standalone with a generated
# certificate: gRPC and REST over TLS, plain text rejected, root password
# rotation, backup and restore into a TLS instance, a switch to the user's own
# certificate and its renewal (each rolls the pods), then TLS turned off.
# Cluster with a cert-manager issuer (skipped without cert-manager): gRPC and
# REST over TLS through the proxy. Every instance must leave nothing behind.
# Usage: test/e2e/run-tls.sh [namespace]
set -euo pipefail

NAMESPACE=${1:-milvus-e2e-tls}
DIR=$(cd "$(dirname "$0")" && pwd)
READY_TIMEOUT=${READY_TIMEOUT:-900}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# shellcheck source=test/e2e/lib.sh
. "$DIR/lib.sh"

conn() {
  kubectl -n "$NAMESPACE" get secret "$1-conn" -o jsonpath="{.data.${2//./\\.}}" | base64 -d
}

rest_url() {
  echo "https://$1-milvus.$NAMESPACE.svc:8443"
}

tls_hash() {
  kubectl -n "$NAMESPACE" get deploy "$1" -o jsonpath='{.spec.template.metadata.annotations.tls\.milvus\.openeverest\.io/hash}'
}

# Waits for the certificate change to reach the Deployment and roll out.
wait_tls_rollout() {
  local instance=$1 deploy=$2 old=$3
  for _ in $(seq 60); do
    [[ "$(tls_hash "$deploy")" != "$old" ]] && break
    sleep 5
  done
  [[ "$(tls_hash "$deploy")" != "$old" ]] || fail "$deploy did not pick up the new certificate"
  kubectl -n "$NAMESPACE" rollout status "deploy/$deploy" --timeout=10m
  sleep 10
  wait_ready "$instance"
}

wait_backup() {
  local backup=$1 state
  for _ in $(seq 60); do
    state=$(kubectl -n "$NAMESPACE" get backup "$backup" -o jsonpath='{.status.state}')
    [[ "$state" == "Succeeded" ]] && return 0
    [[ "$state" == "Failed" ]] && fail "backup $backup failed: $(kubectl -n "$NAMESPACE" get backup "$backup" -o jsonpath='{.status.message}')"
    sleep 10
  done
  fail "backup $backup not done (state=$state)"
}

check_tls_connection() {
  local instance=$1
  [[ "$(conn "$instance" uri)" == https://* ]] || fail "$instance publishes $(conn "$instance" uri), not https"
  [[ "$(conn "$instance" ca.crt)" == *"BEGIN CERTIFICATE"* ]] || fail "$instance publishes no ca.crt"
}

# A CA and a server certificate for the standalone's Service, as a user
# would bring them.
user_certificate() {
  local service=tls-standalone-milvus
  if [[ ! -f "$WORK/ca.key" ]]; then
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
      -subj "/CN=e2e corp CA" -keyout "$WORK/ca.key" -out "$WORK/ca.crt" 2>/dev/null
  fi
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$service" \
    -keyout "$WORK/tls.key" -out "$WORK/tls.csr" 2>/dev/null
  printf 'subjectAltName=DNS:%s,DNS:%s.%s,DNS:%s.%s.svc,DNS:%s.%s.svc.cluster.local\nextendedKeyUsage=serverAuth\n' \
    "$service" "$service" "$NAMESPACE" "$service" "$NAMESPACE" "$service" "$NAMESPACE" >"$WORK/ext"
  openssl x509 -req -in "$WORK/tls.csr" -CA "$WORK/ca.crt" -CAkey "$WORK/ca.key" -CAcreateserial \
    -days 30 -extfile "$WORK/ext" -out "$WORK/tls.crt" 2>/dev/null
  kubectl -n "$NAMESPACE" create secret generic tls-standalone-corp \
    --from-file=tls.crt="$WORK/tls.crt" --from-file=tls.key="$WORK/tls.key" --from-file=ca.crt="$WORK/ca.crt" \
    --dry-run=client -o yaml | kubectl apply -f -
}

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" apply -f "$DIR/backup/storage.yaml"

log "standalone: generated certificate, TLS clients, plain text rejected"
kubectl -n "$NAMESPACE" apply -f "$DIR/tls/standalone.yaml"
wait_ready tls-standalone
check_tls_connection tls-standalone
MODE=write MILVUS_REST_URL=$(rest_url tls-standalone) "$DIR/run-smoke.sh" "$NAMESPACE" tls-standalone
MODE=rejected MILVUS_URI="http://tls-standalone-milvus.$NAMESPACE.svc:19530" "$DIR/run-smoke.sh" "$NAMESPACE" tls-standalone

log "standalone: root password rotation over TLS"
kubectl -n "$NAMESPACE" patch secret tls-standalone-credentials --type merge -p '{"stringData":{"password":"e2e-rotated-password-1"}}'
for _ in $(seq 30); do
  applied=$(kubectl -n "$NAMESPACE" get secret tls-standalone-credentials -o jsonpath='{.data.appliedPassword}' | base64 -d)
  [[ "$applied" == "e2e-rotated-password-1" ]] && break
  sleep 5
done
[[ "$applied" == "e2e-rotated-password-1" ]] || fail "root password not rotated over TLS"
for _ in $(seq 30); do
  [[ "$(conn tls-standalone token)" == "root:e2e-rotated-password-1" ]] && break
  sleep 5
done

log "standalone: backup and restore into a TLS instance"
kubectl -n "$NAMESPACE" apply -f "$DIR/tls/backup.yaml"
wait_backup tls-standalone-backup
kubectl -n "$NAMESPACE" apply -f "$DIR/tls/restored.yaml"
wait_ready tls-restored
check_tls_connection tls-restored
MODE=verify "$DIR/run-smoke.sh" "$NAMESPACE" tls-restored
kubectl -n "$NAMESPACE" delete backup tls-standalone-backup --wait=true --timeout=300s
delete_and_check tls-restored

log "standalone: the user's certificate, then its renewal"
deploy=tls-standalone-milvus-standalone
user_certificate
hash=$(tls_hash "$deploy")
kubectl -n "$NAMESPACE" patch instance tls-standalone --type merge \
  -p '{"spec":{"parameters":{"tls":{"certificateSecretName":"tls-standalone-corp"}}}}'
wait_tls_rollout tls-standalone "$deploy" "$hash"
[[ "$(conn tls-standalone ca.crt)" == "$(cat "$WORK/ca.crt")" ]] || fail "connection secret does not carry the user's CA"
MODE=verify MILVUS_REST_URL=$(rest_url tls-standalone) "$DIR/run-smoke.sh" "$NAMESPACE" tls-standalone
hash=$(tls_hash "$deploy")
user_certificate
wait_tls_rollout tls-standalone "$deploy" "$hash"
MODE=full MILVUS_REST_URL=$(rest_url tls-standalone) "$DIR/run-smoke.sh" "$NAMESPACE" tls-standalone

log "standalone: TLS turned off"
kubectl -n "$NAMESPACE" patch instance tls-standalone --type merge -p '{"spec":{"parameters":{"tls":{"mode":"disabled"}}}}'
for _ in $(seq 60); do
  [[ "$(conn tls-standalone uri)" == http://* ]] && break
  sleep 5
done
[[ "$(conn tls-standalone uri)" == http://* ]] || fail "tls-standalone still publishes $(conn tls-standalone uri)"
kubectl -n "$NAMESPACE" rollout status "deploy/$deploy" --timeout=10m
wait_ready tls-standalone
MODE=full "$DIR/run-smoke.sh" "$NAMESPACE" tls-standalone
delete_and_check tls-standalone
for secret in tls-standalone-tls tls-standalone-tls-ca; do
  for _ in $(seq 30); do
    kubectl -n "$NAMESPACE" get secret "$secret" >/dev/null 2>&1 || break
    sleep 5
  done
  ! kubectl -n "$NAMESPACE" get secret "$secret" >/dev/null 2>&1 || fail "$secret left behind"
done
kubectl -n "$NAMESPACE" delete secret tls-standalone-corp

if kubectl get crd certificates.cert-manager.io >/dev/null 2>&1; then
  log "cluster: certificate from a cert-manager issuer"
  kubectl -n "$NAMESPACE" apply -f "$DIR/tls/cluster.yaml"
  wait_ready tls-cluster
  check_components tls-cluster
  check_tls_connection tls-cluster
  MODE=full MILVUS_REST_URL=$(rest_url tls-cluster) "$DIR/run-smoke.sh" "$NAMESPACE" tls-cluster
  MODE=rejected MILVUS_URI="http://tls-cluster-milvus.$NAMESPACE.svc:19530" "$DIR/run-smoke.sh" "$NAMESPACE" tls-cluster
  delete_and_check tls-cluster
  kubectl -n "$NAMESPACE" delete issuer tls-e2e-selfsigned
else
  log "skipping the cert-manager issuer: cert-manager is not installed"
fi

kubectl -n "$NAMESPACE" delete -f "$DIR/backup/storage.yaml"
log "PASS"
