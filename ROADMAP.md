# Milvus Provider Roadmap

This document outlines the planned features and improvements for the Milvus provider for OpenEverest.

## Phase 1: MVP (Current - ✅ Complete)
**Goal:** Core provider lifecycle and basic resource configuration.

- [x] Provider scaffolding and layout
- [x] Standalone topology support
- [x] Cluster topology support (proxy, data nodes, query nodes, coordinators)
- [x] CPU/RAM/Storage resource mapping
- [x] Component replica management
- [x] Basic Milvus CR generation
- [x] Provider registration with OpenEverest runtime

## Phase 2: Configuration & Advanced Validation
**Goal:** Enable Milvus configuration management and stricter validation.

### Features
- [x] **Configuration Management**
  - Support `spec.components[].parameters.configuration` for Milvus engine config
  - Map OpenEverest parameters into Milvus ConfigMap
  - Enable user-driven tuning of Milvus settings (performance, memory, logging)

- [x] **Enhanced Validation**
  - Component sizing constraints (e.g., min CPU/RAM per component)
  - Storage size consistency (prevent decrease-only constraint in edit mode)
  - Topology-specific rules (e.g., minimum coordinators for cluster mode)
  - Resource limit vs. request balance checks

- [x] **Bundled Dependency Resource Management**
  - Expose resource requests/limits for operator-deployed dependencies (etcd, Pulsar bookie/broker/zookeeper, MinIO)
  - Provide sane per-topology defaults so dependencies don't over-provision on small clusters
  - Support replica/count tuning for dependency components (e.g., etcd cluster size, Pulsar broker count)
  - Allow disabling bundled dependencies when external equivalents are configured

- [ ] **External Dependency Support**
  - Support external etcd endpoints
  - Support external S3/MinIO (AWS S3, Google Cloud Storage, Azure Blob)
  - Support external Kafka/Pulsar for message streaming
  - Secret management for external service credentials

## Phase 3: Status & Connection Management
**Goal:** Provide real-time cluster status and connection details.

### Features
- [ ] **Status Translation**
  - Map Milvus health status (Pending → Creating, Healthy → Healthy, etc.) to OpenEverest status
  - Surface component-level health
  - Track deployment readiness

- [ ] **Connection Details**
  - Extract and expose Milvus service endpoints
  - Generate connection strings for SDK clients
  - Support internal and external endpoint exposure (LoadBalancer, NodePort, ClusterIP)

- [ ] **Credential Management**
  - Handle initial credential generation if supported by Milvus
  - Store/retrieve credentials from OpenEverest secrets
  - Support credential rotation workflows

## Phase 4: Backup & Restore
**Goal:** Enable data persistence and recovery workflows.

### Features
- [ ] **Backup Integration**
  - Implement BackupClass support for Milvus
  - Support point-in-time recovery (if Milvus supports it)
  - Manage backup storage (S3, MinIO, GCS, Azure)

- [ ] **Restore Workflows**
  - Support restoring an Instance from a Backup
  - Handle data seeding during creation
  - Coordinate with OpenEverest backup/restore orchestration

## Phase 5: Monitoring & Observability
**Goal:** Integrate with OpenEverest monitoring and alerting.

### Features
- [ ] **Prometheus Metrics**
  - Enable/disable PodMonitor for component metrics
  - Map Milvus metrics to OpenEverest monitoring schema
  - Support custom scrape intervals and labels

- [ ] **Health Checks**
  - Liveness and readiness probe configuration
  - Component health monitoring
  - Cluster readiness detection

- [ ] **Logging**
  - Log level configuration
  - Log destination management (stdout, files)
  - Integration with OpenEverest logging infrastructure

## Phase 6: Scaling & Upgrades
**Goal:** Support dynamic scaling and version upgrades.

### Features
- [ ] **Horizontal Scaling**
  - Safe replica scaling for data nodes, query nodes
  - Replica count validation and constraints
  - Rolling restart strategies

- [ ] **Version Management**
  - Version bundle definitions and defaults
  - Component-level version pinning
  - Rolling upgrade workflows
  - Compatibility matrix validation

- [ ] **Maintenance Windows**
  - Support for maintenance mode and disruptive operations
  - User approval gates for upgrades/restarts
  - Maintenance mode tokenization

## Phase 7: Advanced Clustering
**Goal:** Support Milvus operator-specific advanced features.

### Features
- [ ] **Coordinator Modes**
  - MixCoord support (single coordinator combining all roles)
  - Support for independent coordinator scaling
  - HA coordinator configuration

- [ ] **Deployment Groups**
  - Multiple deployment groups per component
  - Blue/green deployments for rolling updates
  - Custom deployment group configurations

- [ ] **Streaming Node Support**
  - Configuration for Milvus streaming node (experimental in Milvus)
  - Streaming mode defaults and topology integration

- [ ] **CDC (Change Data Capture)**
  - Support for CDC component if needed
  - Change log streaming configuration

## Phase 8: Testing & Validation
**Goal:** Ensure provider reliability and compatibility.

### Features
- [ ] **Unit Tests**
  - Provider logic validation (BuildMilvusSpec, resource mapping, status translation)
  - Edge case handling

- [ ] **Integration Tests**
  - End-to-end Instance creation and lifecycle
  - Component scaling scenarios
  - Dependency deployment verification

- [ ] **E2E Tests**
  - Real Kubernetes cluster validation
  - Multi-topology deployment verification
  - Upgrade and scaling workflows
  - Backup/restore workflows (when implemented)

- [ ] **Conformance Tests**
  - OpenEverest provider contract compliance
  - Version compatibility matrix

## Phase 9: Documentation & Examples
**Goal:** Provide comprehensive documentation and usage patterns.

### Features
- [ ] **Architecture Documentation**
  - Provider design and reconciliation flow
  - Milvus CRD mapping
  - Component interaction model

- [ ] **Usage Guide**
  - Standalone deployment tutorial
  - Cluster deployment with best practices
  - Resource sizing recommendations
  - External dependency configuration

- [ ] **Example Manifests**
  - Standalone with minimal resources
  - Cluster with HA setup
  - Multi-AZ deployment patterns
  - Monitoring and logging examples

## Phase 10: Performance & Optimization
**Goal:** Optimize provider performance and resource efficiency.

### Features
- [ ] **Performance Tuning**
  - Reconciliation frequency optimization
  - Watch efficiency
  - Caching strategies

- [ ] **Resource Efficiency**
  - Default resource recommendations per topology
  - Right-sizing guidelines
  - Cost optimization tips

## Deferred/Exploratory
Features that may be added in future phases or based on community feedback:

- [ ] **Multi-Tenancy** - Support for namespace/RBAC isolation
- [ ] **Disaster Recovery** - Cross-cluster failover and replication
- [ ] **Network Policies** - Advanced networking and security controls
- [ ] **Custom Helm Values** - Direct passthrough of custom Helm chart values
- [ ] **Operator Versioning** - Support for multiple Milvus operator versions
- [ ] **GitOps Integration** - ArgoCD/Flux native support patterns

---

## Implementation Priority

**High Priority (Phases 2–3):**
- Configuration management
- Status reporting
- Connection details
- Enhanced validation

**Medium Priority (Phases 4–5):**
- Backup/restore
- Monitoring integration

**Lower Priority (Phases 6–10):**
- Advanced clustering features
- Full test coverage
- Documentation
- Performance optimization

---

## Success Metrics

- [ ] All MVP features stable and tested
- [ ] Configuration management enables 80% of common Milvus tuning scenarios
- [ ] Status reporting accurate and actionable
- [ ] Bundled dependencies (etcd, Pulsar, MinIO) have resource constraints and sane defaults
- [ ] External dependencies fully supported
- [ ] Backup/restore workflows match MongoDB provider capability
- [ ] Monitoring integration matches OpenEverest standards
- [ ] E2E tests cover main topology and scaling scenarios
- [ ] Documentation sufficient for self-service deployment
