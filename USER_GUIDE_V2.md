# User Guide (v2): End-to-End Setup & Walkthrough on a New 1.37 GKE Cluster

This guide covers the complete setup from scratch on a **new Kubernetes 1.37 GKE alpha cluster**:
1. **Part 1**: Creating and pre-flight checking an 18-node GKE 1.37 alpha cluster (Ubuntu nodes).
2. **Part 2**: Building the custom NVIDIA DRA driver (`poc/nvlink-domain-claims`) and Mokka simulator images from source.
3. **Part 3**: Installing Mokka (2 cliques of 9 nodes, simulated GB200 GPUs + IMEX channels), the NVIDIA device plugin, and the custom DRA driver Helm chart.
4. **Part 4**: Walking step-by-step through the full design flow in the design doc.

---

## Part 1: Create & Verify a New 1.37 GKE Alpha Cluster

### 1.1 Environment Variables & Authentication

Set the environment variables used throughout this guide:
```bash
export PROJECT_ID="mortent-gke-dev"
export ZONE="us-central1-a"
export CLUSTER_NAME="alpha-gpu-test-cluster-137"
export REGISTRY="us-docker.pkg.dev/${PROJECT_ID}/dra-test"

export DRIVER_REPO="/usr/local/google/home/mortent/Development/mortent/go/src/sigs.k8s.io/dra-driver-nvidia-gpu"
export MOKKA_REPO="/usr/local/google/home/mortent/Development/mortent/go/src/github.com/NVIDIA/k8s-test-infra"
export WORK_DIR="/tmp/poc-nvlink-setup"
mkdir -p "${WORK_DIR}"
```

Authenticate `gcloud` and Docker for Artifact Registry:
```bash
gcloud auth login --update-adc
gcloud auth configure-docker us-docker.pkg.dev --quiet
```

### 1.2 Create the 18-Node GKE 1.37 Alpha Cluster (Ubuntu Nodes)

> **Important:**
> - **Why Alpha mode (`--enable-kubernetes-alpha`)**: Enables Kubernetes alpha/beta feature gates and API groups (`scheduling.k8s.io`, DRA `resource.k8s.io`).
> - **Why Ubuntu (`--image-type=UBUNTU_CONTAINERD`)**: Mokka's node agent writes `/etc/containerd/conf.d/50-mokka.toml` to enable NRI and register the mock NVIDIA container runtime. Container-Optimized OS (COS) has a read-only `/etc` and managed containerd configuration that prevents Mokka's runtime hook from persisting.

1. Find the latest available `1.37` GKE master/node version in your zone:
   ```bash
   gcloud container get-server-config --zone="${ZONE}" --project="${PROJECT_ID}" \
     --format="yaml(validMasterVersions)" | grep "1.37"
   ```

2. Create the 18-node cluster (replace `1.37` below with the exact `1.37.x-gke.N` string if needed):
   ```bash
   gcloud container clusters create "${CLUSTER_NAME}" \
     --project="${PROJECT_ID}" \
     --zone="${ZONE}" \
     --cluster-version="1.37" \
     --enable-kubernetes-alpha \
     --no-enable-autorepair \
     --no-enable-autoupgrade \
     --image-type="UBUNTU_CONTAINERD" \
     --machine-type="e2-standard-8" \
     --num-nodes=18 \
     --disk-size="100GB"

   gcloud container clusters get-credentials "${CLUSTER_NAME}" \
     --zone="${ZONE}" \
     --project="${PROJECT_ID}"
   ```

### 1.3 Run the Control-Plane Pre-Flight Check (`poc_brief.md` §4.3 Pre-Flight A)

1. **Check enabled feature gates on `kube-apiserver`**:
   ```bash
   kubectl get --raw /metrics | grep kubernetes_feature_enabled \
     | grep -E 'GenericWorkload|TopologyAwareWorkloadScheduling|DRAWorkloadResourceClaims|PodGroupResourceClaims|DRAConsumableCapacity|DRAResourceClaimDeviceStatus'
   ```
   Confirm that the workload, topology-aware scheduling, and DRA feature gates report `1`.

2. **Check the served `scheduling.k8s.io` API version (`v1beta1` in 1.37 vs `v1alpha2` in 1.36)**:
   ```bash
   kubectl api-resources --api-group=scheduling.k8s.io
   ```
   - On Kubernetes **1.37+**, `Workload` and `PodGroup` are served at **`scheduling.k8s.io/v1beta1`** (matching `d3_walkthrough.md`).
   - *(Note: If your cluster serves `scheduling.k8s.io/v1alpha2`, see the callout in Part 4 Step 1.2 for the single field-name difference in `PodGroup.spec`.)*

3. **Verify field persistence and gang-gate behavior**:
   ```bash
   cat <<'EOF' | kubectl apply -f -
   apiVersion: scheduling.k8s.io/v1beta1
   kind: PodGroup
   metadata:
     name: preflight
     namespace: default
   spec:
     schedulingPolicy:
       gang:
         minCount: 2
     schedulingConstraints:
       topology:
       - key: kubernetes.io/hostname
     resourceClaims:
     - name: x
       resourceClaimTemplateName: does-not-exist
   ---
   apiVersion: v1
   kind: Pod
   metadata:
     name: preflight-gang-pod-0
     namespace: default
   spec:
     schedulingGroup:
       podGroupName: preflight
     containers:
     - name: pause
       image: registry.k8s.io/pause:3.10
   EOF

   sleep 8
   kubectl get podgroup preflight -n default -o yaml | grep -E 'schedulingConstraints|resourceClaims'
   kubectl get pod preflight-gang-pod-0 -n default -o wide
   ```
   Expected:
   - `schedulingConstraints` and `resourceClaims` are persisted on the `PodGroup` (not dropped).
   - `preflight-gang-pod-0` remains `Pending` with `NODE=<none>` because `minCount: 2` is not yet satisfied.

   Clean up the preflight objects:
   ```bash
   kubectl delete pod preflight-gang-pod-0 -n default
   kubectl delete podgroup preflight -n default
   ```

---

## Part 2: Build the Custom NVIDIA DRA Driver & Mokka Images

> **Caution:** Keep `REGISTRY` (`us-docker.pkg.dev/${PROJECT_ID}/dra-test`) private. The Mokka-wrapped daemon image contains the proprietary `nvidia-imex` binary and must never be pushed to a public registry.

### 2.1 Verify the Custom Driver Source Code (`poc/nvlink-domain-claims`)

Switch to the `poc/nvlink-domain-claims` branch in the driver repository and verify the build and linter:
```bash
cd "${DRIVER_REPO}"
git checkout poc/nvlink-domain-claims
git log --oneline -n 6
make check
make build
```
You should see the 5 stage commits on top of `main` and `0 issues.` from `make check`.

### 2.2 Build & Push the Mokka `nvml-mock` Image

Build the Mokka simulator image from the `k8s-test-infra` checkout:
```bash
docker build -f "${MOKKA_REPO}/deployments/nvml-mock/Dockerfile" \
  -t "${REGISTRY}/nvml-mock:poc" \
  "${MOKKA_REPO}"

docker push "${REGISTRY}/nvml-mock:poc"
```

### 2.3 Build & Push the Custom NVIDIA DRA Driver Image (Wrapped for Mokka `NO-GPU` IMEX)

Running the custom driver on Mokka requires a two-step image build:
1. Build the base `k8s-dra-driver-gpu` image from the `poc/nvlink-domain-claims` source tree (which compiles the custom `compute-domain-controller`, `compute-domain-kubelet-plugin`, `compute-domain-daemon`, and `gpu-kubelet-plugin` binaries and packages `/templates/compute-domain-daemon.tmpl.yaml`).
2. Wrap that base image with Mokka's `deployments/nvml-mock/Dockerfile.compute-domain-daemon`, which installs the `nvidia-imex` `NO-GPU` shim (`/usr/bin/nvidia-imex` → `nvidia-imex.real --nogpu`) so the real `nvidia-imex` daemon forms TCP quorums on CPU nodes without physical NVSwitch hardware.

**Option A — Full Build from `deployments/container/Makefile` (Recommended for a Fresh Setup):**
```bash
# 1. Build the custom driver base image from poc/nvlink-domain-claims
make -C "${DRIVER_REPO}" -f deployments/container/Makefile build \
  IMAGE_NAME="${REGISTRY}/k8s-dra-driver-gpu" \
  IMAGE_TAG="poc-base"

# 2. Wrap with Mokka's NO-GPU nvidia-imex shim
docker build \
  -f "${MOKKA_REPO}/deployments/nvml-mock/Dockerfile.compute-domain-daemon" \
  --target daemon \
  --build-arg COMPUTE_DOMAIN_DAEMON_IMAGE="${REGISTRY}/k8s-dra-driver-gpu:poc-base" \
  -t "${REGISTRY}/k8s-dra-driver-gpu:stage6-nogpu" \
  "${MOKKA_REPO}"

docker push "${REGISTRY}/k8s-dra-driver-gpu:stage6-nogpu"
```

**Option B — Fast Incremental Rebuild (If `stage0-nogpu` or `stage6-nogpu` Already Exists in Your Registry):**
If you already built the Mokka-wrapped image once and only modified Go code or templates in `${DRIVER_REPO}`, you can cross-compile the two binaries locally and layer them in ~10 seconds:
```bash
cd "${DRIVER_REPO}"
mkdir -p /tmp/cd-build
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/cd-build/compute-domain-controller ./cmd/compute-domain-controller
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/cd-build/compute-domain-kubelet-plugin ./cmd/compute-domain-kubelet-plugin
cp -r templates /tmp/cd-build/

cat <<EOF > /tmp/cd-build/Dockerfile
FROM ${REGISTRY}/k8s-dra-driver-gpu:stage0-nogpu
COPY compute-domain-controller /usr/bin/compute-domain-controller
COPY compute-domain-kubelet-plugin /usr/bin/compute-domain-kubelet-plugin
COPY templates/compute-domain-daemon.tmpl.yaml /templates/compute-domain-daemon.tmpl.yaml
EOF

docker build -t "${REGISTRY}/k8s-dra-driver-gpu:stage6-nogpu" /tmp/cd-build
docker push "${REGISTRY}/k8s-dra-driver-gpu:stage6-nogpu"
```

---

## Part 3: Install Mokka, NVIDIA Device Plugin & Custom DRA Driver

### 3.1 Label the 18 Nodes & Find Two Unused Character Device Majors

1. Collect the 18 worker node names into `${WORK_DIR}/worker-nodes.txt` and label all nodes for Mokka (works in both `bash` and `zsh`):
   ```bash
   kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
     | sort > "${WORK_DIR}/worker-nodes.txt"
   echo "Found $(wc -l < "${WORK_DIR}/worker-nodes.txt") nodes"

   kubectl label nodes --all \
     mokka.nvidia.com/type=sgpu \
     nvidia.com/gpu.present=true \
     --overwrite
   ```

2. Discover two unused character device major numbers (`>= 235`) on a cluster node for Mokka's mock IMEX channel and capability devices:
   ```bash
   FIRST_NODE=$(head -n 1 "${WORK_DIR}/worker-nodes.txt")
   read -r IMEX_MAJOR CAPS_MAJOR <<EOF
   $(kubectl run preflight-majors --rm -i --restart=Never \
     --image=debian:bookworm-slim \
     --overrides="{\"spec\":{\"nodeName\":\"${FIRST_NODE}\"}}" \
     -- awk '
       /^Character devices:/ { in_char=1; next }
       /^Block devices:/     { in_char=0 }
       in_char && $1 ~ /^[0-9]+$/ { used[$1]=1 }
       END {
         for (m = 235; m < 512 && count < 2; m++) {
           if (!(m in used)) { printf "%d ", m; count++ }
         }
         print ""
       }
     ' /proc/devices)
   EOF
   echo "Using IMEX_MAJOR=${IMEX_MAJOR}, CAPS_MAJOR=${CAPS_MAJOR}"
   ```

### 3.2 Generate the 2-Clique Topology & Install Mokka (`nvml-mock`)

1. Generate `topology-2cliques-18nodes.yaml` dividing the 18 nodes into two 9-node cliques (`00000000-0000-0000-0000-0000000000ab.0` and `00000000-0000-0000-0000-0000000000ab.1`):
   ```bash
   {
     cat <<'EOF'
   topology:
     enabled: true
     domains:
       - name: poc-domain
         uuid: "00000000-0000-0000-0000-0000000000ab"
         cliques:
           - id: 0
             nodes:
   EOF
     sed -n '1,9s/^/            - /p' "${WORK_DIR}/worker-nodes.txt"
     cat <<'EOF'
           - id: 1
             nodes:
   EOF
     sed -n '10,18s/^/            - /p' "${WORK_DIR}/worker-nodes.txt"
   } > "${WORK_DIR}/topology-2cliques-18nodes.yaml"
   ```

2. Generate `mokka-values.yaml` (`gb200` profile with 4 GPUs/node and mock IMEX channels enabled):
   ```bash
   cat <<EOF > "${WORK_DIR}/mokka-values.yaml"
   image:
     repository: ${REGISTRY}/nvml-mock
     tag: poc
     pullPolicy: IfNotPresent

   gpu:
     profile: gb200
     count: 4

   nodeSelector:
     mokka.nvidia.com/type: sgpu

   imex:
     mockChannels:
       enabled: true
       channelCount: 2048
       channelMajor: ${IMEX_MAJOR}
       capsMajor: ${CAPS_MAJOR}

   nri:
     enabled: true
     socketPath: /var/run/nri/nri.sock

   updateStrategy:
     rollingUpdate:
       maxUnavailable: "100%"
   EOF
   ```

3. Install Mokka via Helm and verify that a test pod sees 4 simulated GB200 GPUs:
   ```bash
   helm upgrade --install nvml-mock \
     "${MOKKA_REPO}/deployments/nvml-mock/helm/nvml-mock" \
     --namespace mokka --create-namespace \
     -f "${WORK_DIR}/mokka-values.yaml" \
     -f "${WORK_DIR}/topology-2cliques-18nodes.yaml" \
     --wait --timeout 5m

   kubectl run mokka-verify --rm -i --restart=Never \
     --image=debian:bookworm-slim \
     --annotations="nvml-mock.nvidia.com/devices=true" \
     --overrides='{"spec":{"nodeSelector":{"mokka.nvidia.com/type":"sgpu"}}}' \
     -- nvidia-smi -L
   ```
   Expected output:
   ```text
   GPU 0: NVIDIA GB200 (UUID: GPU-...)
   GPU 1: NVIDIA GB200 (UUID: GPU-...)
   GPU 2: NVIDIA GB200 (UUID: GPU-...)
   GPU 3: NVIDIA GB200 (UUID: GPU-...)
   ```

### 3.3 Install the NVIDIA Device Plugin (`nvidia.com/gpu: 4` per Node)

> **Important:** On GKE, the `ResourceQuota` admission controller restricts `PriorityClass In [system-node-critical, system-cluster-critical]` to `kube-system` unless a `ResourceQuota` granting those scopes exists in the target namespace.
> Additionally, Mokka provides a purpose-built `tests/e2e/device-plugin-mock.yaml` manifest in `kube-system` configured with `--nvidia-driver-root=/var/lib/nvml-mock/driver`, `--driver-root-ctr-path=/var/lib/nvml-mock/driver`, and `--pass-device-specs=true`.

*(If you previously ran `helm upgrade --install nvidia-device-plugin` in `nvidia-device-plugin`, clean it up first:)*
```bash
helm uninstall nvidia-device-plugin -n nvidia-device-plugin 2>/dev/null || true
kubectl delete namespace nvidia-device-plugin --ignore-not-found
```

Apply Mokka's device plugin `DaemonSet` in `kube-system` and wait for it to roll out across all 18 nodes:
```bash
kubectl apply -f "${MOKKA_REPO}/tests/e2e/device-plugin-mock.yaml"
kubectl -n kube-system rollout status daemonset/nvidia-device-plugin-mock --timeout=3m
```

### 3.4 Install the Custom NVIDIA DRA Driver Helm Chart

1. **Create the `nvidia-dra-driver-gpu` namespace and the `gcp-critical-pods` `ResourceQuota`**:
   Because `dra-driver-nvidia-gpu` sets `priorityClassName: system-node-critical` on the controller, kubelet-plugin, and per-domain IMEX daemon pods, GKE requires a `ResourceQuota` in `nvidia-dra-driver-gpu` permitting `system-node-critical` and `system-cluster-critical` pods:
   ```bash
   kubectl create namespace nvidia-dra-driver-gpu --dry-run=client -o yaml | kubectl apply -f -

   cat <<'EOF' | kubectl apply -f -
   apiVersion: v1
   kind: ResourceQuota
   metadata:
     name: gcp-critical-pods
     namespace: nvidia-dra-driver-gpu
   spec:
     hard:
       pods: 1000
     scopeSelector:
       matchExpressions:
       - operator: In
         scopeName: PriorityClass
         values:
         - system-node-critical
         - system-cluster-critical
   EOF
   ```

2. **Create `dra-driver-mokka-values.yaml` and install the chart** from `${DRIVER_REPO}/deployments/helm/dra-driver-nvidia-gpu`:
   ```bash
   cat <<EOF > "${WORK_DIR}/dra-driver-mokka-values.yaml"
   image:
     repository: ${REGISTRY}/k8s-dra-driver-gpu
     tag: stage6-nogpu
     pullPolicy: Always

   nvidiaDriverRoot: /var/lib/nvml-mock/driver
   altProcDevices: /var/lib/nvml-mock/imex/proc-devices
   gpuResourcesEnabledOverride: true

   resources:
     gpus:
       enabled: false
     computeDomains:
       enabled: true

   kubeletPlugin:
     podAnnotations:
       nvml-mock.nvidia.com/devices: "true"
     containers:
       computeDomains:
         gpuCliqueLabelEnabled: true
   EOF

   helm upgrade --install dra-driver-nvidia-gpu \
     "${DRIVER_REPO}/deployments/helm/dra-driver-nvidia-gpu" \
     --namespace nvidia-dra-driver-gpu \
     -f "${WORK_DIR}/dra-driver-mokka-values.yaml" \
     --wait --timeout 5m
   ```

Verify that all 18 nodes now have `nvidia.com/gpu.clique` set to either `00000000-0000-0000-0000-0000000000ab.0` (9 nodes) or `00000000-0000-0000-0000-0000000000ab.1` (9 nodes), and `nvidia.com/gpu: 4` allocatable:
```bash
kubectl get nodes -L nvidia.com/gpu.clique -o custom-columns=\
NAME:.metadata.name,\
CLIQUE:.metadata.labels.'nvidia\.com/gpu\.clique',\
GPUS:.status.allocatable.'nvidia\.com/gpu'
```

---

## Part 4: Walkthrough of the Full Design Flow (`d3_walkthrough.md` §§0–9)

### Step 0: Inspect Install-Time Driver Objects (Walkthrough §0)

1. **Per-Clique `ResourceSlice`s published by `compute-domain-controller` (§0.1)**:
   ```bash
   kubectl get resourceslices -o custom-columns=\
   NAME:.metadata.name,\
   DRIVER:.spec.driver,\
   POOL:.spec.pool.name,\
   GEN:.spec.pool.generation,\
   NODE:.spec.nodeName \
   | grep nvlink-domain
   ```
   Inspect one clique `ResourceSlice` in detail:
   ```bash
   kubectl get resourceslices -o json | jq '.items[] | select(.spec.pool.name == "nvlink-domain-00000000-0000-0000-0000-0000000000ab.1") | .spec'
   ```
   Confirm:
   - `pool.name: "nvlink-domain-00000000-0000-0000-0000-0000000000ab.1"`
   - `nodeSelector`: `nvidia.com/gpu.clique In [00000000-0000-0000-0000-0000000000ab.1]`
   - `devices[0]`: `name: "domain"`, `allowMultipleAllocations: true`, no `capacity`, attributes `type="domain"`, `topologyKey="nvidia.com/gpu.clique"`, `topologyValue="00000000-0000-0000-0000-0000000000ab.1"`, `nodeCount=9`.

2. **Per-Node `daemon-0` `ResourceSlice`s published by the kubelet plugin (§0.2)**:
   ```bash
   kubectl get resourceslices -o json | jq '.items[] | select(.spec.driver == "compute-domain.nvidia.com" and .spec.nodeName != null) | {nodeName: .spec.nodeName, devices: [.spec.devices[].name]}' | head -n 12
   ```

3. **Domain `DeviceClass` `compute-domain.nvidia.com` (§0.3)**:
   ```bash
   kubectl get deviceclass compute-domain.nvidia.com -o yaml
   ```

4. **Static Daemon `ResourceClaimTemplate` `compute-domain-daemon` (§0.4)**:
   ```bash
   kubectl get resourceclaimtemplate -n nvidia-dra-driver-gpu compute-domain-daemon -o yaml
   ```

---

### Step 1 & Step 2: Submit the User Objects (`ResourceClaimTemplate`, `Workload`, `PodGroup`, and `Pod`s) (Walkthrough §§1–2)

1. **Create namespace `team-a` and the static user `ResourceClaimTemplate` (`cd-domain`)** (§1.1):
   ```bash
   kubectl create namespace team-a --dry-run=client -o yaml | kubectl apply -f -

   cat <<'EOF' | kubectl apply -f -
   apiVersion: resource.k8s.io/v1
   kind: ResourceClaimTemplate
   metadata:
     name: cd-domain
     namespace: team-a
   spec:
     spec:
       devices:
         requests:
         - name: domain
           exactly:
             deviceClassName: compute-domain.nvidia.com
   EOF
   ```

2. **Create the `Workload` (`train`) and `PodGroup` (`train-workers-0`) for a 6-node gang** (§1.2 & §2):

   > **Tip:** The manifest below uses the **Kubernetes 1.37 `scheduling.k8s.io/v1beta1`** schema from `d3_walkthrough.md`.
   > *(If testing on a 1.36 cluster where only `scheduling.k8s.io/v1alpha2` is served, change `apiVersion` to `scheduling.k8s.io/v1alpha2` and replace `workloadRef: {workloadName: train, templateName: workers}` with `podGroupTemplateRef: {workload: {workloadName: train, podGroupTemplateName: workers}}`.)*

   ```bash
   cat <<'EOF' | kubectl apply -f -
   apiVersion: scheduling.k8s.io/v1beta1
   kind: Workload
   metadata:
     name: train
     namespace: team-a
   spec:
     podGroupTemplates:
     - name: workers
       schedulingPolicy:
         gang:
           minCount: 6
       schedulingConstraints:
         topology:
         - key: nvidia.com/gpu.clique
       resourceClaims:
       - name: compute-domain
         resourceClaimTemplateName: cd-domain
   ---
   apiVersion: scheduling.k8s.io/v1beta1
   kind: PodGroup
   metadata:
     name: train-workers-0
     namespace: team-a
   spec:
     workloadRef:
       workloadName: train
       templateName: workers
     schedulingPolicy:
       gang:
         minCount: 6
     schedulingConstraints:
       topology:
       - key: nvidia.com/gpu.clique
     resourceClaims:
     - name: compute-domain
       resourceClaimTemplateName: cd-domain
   EOF
   ```

3. **Create the 6 worker `Pod`s (`train-workers-0-0` .. `train-workers-0-5`), each requesting all 4 GPUs of its node and referencing the shared `compute-domain` claim** (§1.3 & §2):
   ```bash
   for i in 0 1 2 3 4 5; do
     cat <<EOF | kubectl apply -f -
   apiVersion: v1
   kind: Pod
   metadata:
     name: train-workers-0-$i
     namespace: team-a
     labels:
       app: train-workers
   spec:
     schedulingGroup:
       podGroupName: train-workers-0
     restartPolicy: Never
     containers:
     - name: worker
       image: ubuntu:22.04
       command: ["bash", "-c", "sleep 3600"]
       resources:
         limits:
           nvidia.com/gpu: 4
         claims:
         - name: compute-domain
     resourceClaims:
     - name: compute-domain
       resourceClaimTemplateName: cd-domain
   EOF
   done
   ```

---

### Step 3, Step 4 & Step 5: Verify Shared Claim Minting, Gang Scheduling & Fence Allocation (Walkthrough §§3–5)

1. **Inspect the `PodGroup`-owned `ResourceClaim` minted by `kube-controller-manager` and allocated by `kube-scheduler`**:
   ```bash
   CLAIM_NAME=$(kubectl get podgroup -n team-a train-workers-0 -o jsonpath='{.status.resourceClaimStatuses[0].resourceClaimName}')
   CLAIM_UID=$(kubectl get resourceclaim -n team-a "$CLAIM_NAME" -o jsonpath='{.metadata.uid}')
   echo "Shared Claim Name: $CLAIM_NAME"
   echo "Shared Claim UID (Domain ID): $CLAIM_UID"

   kubectl get resourceclaim -n team-a "$CLAIM_NAME" -o yaml
   ```
   Verify:
   - `metadata.ownerReferences`: `kind: PodGroup`, `name: train-workers-0` (§3)
   - `status.allocation.devices.results[0]`: `driver: compute-domain.nvidia.com`, `pool: nvlink-domain-00000000-0000-0000-0000-0000000000ab.<0 or 1>`, `device: domain`, `shareID: <UUID>` (§5.1)
   - `status.allocation.nodeSelector`: fences all current and future member pods to `nvidia.com/gpu.clique In [00000000-0000-0000-0000-0000000000ab.<0 or 1>]` (§5.1)
   - `status.reservedFor`: references the `PodGroup` `train-workers-0` (§5.1)

2. **Verify that all 6 pods were placed on 6 distinct nodes inside the selected clique** (§5.2):
   ```bash
   kubectl get pods -n team-a -l app=train-workers -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
     | while read -r p; do
         NODE=$(kubectl get pod -n team-a "$p" -o jsonpath='{.spec.nodeName}')
         CLIQUE=$(kubectl get node "$NODE" -o jsonpath='{.metadata.labels.nvidia\.com/gpu\.clique}')
         CLAIM_REF=$(kubectl get pod -n team-a "$p" -o jsonpath='{.status.resourceClaimStatuses[0].resourceClaimName}')
         printf "%-18s  %-55s  clique=%s  claim=%s\n" "$p" "$NODE" "$CLIQUE" "$CLAIM_REF"
       done
   ```

3. **Check the T4 concurrent first-bind events** (`poc_brief.md` §7 T4):
   ```bash
   kubectl get events -n team-a --field-selector reason=FailedScheduling
   ```
   On first bind of an unallocated shared claim, 1 pod wins `UpdateStatus` and `N - 1` pods emit `claim ... got allocated elsewhere in the meantime` before binding 2 seconds later on pass 2.

---

### Step 6: Verify Controller `DaemonSet` Materialization (Walkthrough §6)

1. **Confirm no `ComputeDomain` CR exists anywhere in the cluster**:
   ```bash
   kubectl get computedomains -A
   ```

2. **Inspect the per-domain `DaemonSet` (`computedomain-daemon-$CLAIM_UID`) created by `compute-domain-controller`** (§6.1):
   ```bash
   kubectl get daemonset -n nvidia-dra-driver-gpu "computedomain-daemon-$CLAIM_UID" -o json | jq '{
     name: .metadata.name,
     nodeSelector: .spec.template.spec.nodeSelector,
     resourceClaims: .spec.template.spec.resourceClaims,
     env: .spec.template.spec.containers[0].env
   }'
   ```
   Confirm:
   - `nodeSelector`: `{"resource.nvidia.com/computeDomain": "<CLAIM_UID>"}`
   - `resourceClaims`: references the static `compute-domain-daemon` template
   - `env`: `COMPUTE_DOMAIN_UUID`, `COMPUTE_DOMAIN_NAME`, `COMPUTE_DOMAIN_NAMESPACE`, `MAX_NODES_PER_IMEX_DOMAIN`

---

### Step 7: Verify Node Join, `ComputeDomainClique` Readiness, IMEX Quorum & `ResourceClaim.status.devices[]` (Walkthrough §7)

Wait for all 6 workload pods to become `Ready`:
```bash
kubectl wait --for=condition=Ready pod -l app=train-workers -n team-a --timeout=120s
```

1. **Verify that the kubelet plugin labelled all 6 participating nodes with `resource.nvidia.com/computeDomain=$CLAIM_UID`** (§7.1):
   ```bash
   kubectl get nodes -l "resource.nvidia.com/computeDomain=$CLAIM_UID" -L nvidia.com/gpu.clique,resource.nvidia.com/computeDomain
   ```

2. **Verify that only the single static `compute-domain-daemon` `ResourceClaimTemplate` exists in the driver namespace and 6 daemon pods are `Running`** (§7.2):
   ```bash
   kubectl get resourceclaimtemplates -n nvidia-dra-driver-gpu
   kubectl get pods -n nvidia-dra-driver-gpu -l "resource.nvidia.com/computeDomain=$CLAIM_UID" -o wide
   ```

3. **Inspect the `ComputeDomainClique` (`<CLAIM_UID>.<cliqueID>`)** (§7.3):
   ```bash
   kubectl get computedomaincliques -n nvidia-dra-driver-gpu -l "resource.nvidia.com/computeDomain=$CLAIM_UID" -o yaml
   ```
   Confirm that 6 daemons (`index: 0..5`) are listed with `status: Ready`.

4. **Verify that `/dev/nvidia-caps-imex-channels/channel0` is injected into every workload pod** (§7.4):
   ```bash
   for i in 0 1 2 3 4 5; do
     echo "=== train-workers-0-$i ==="
     kubectl exec -n team-a "train-workers-0-$i" -- ls -l /dev/nvidia-caps-imex-channels/channel0
   done
   ```

5. **Query `nvidia-imex-ctl -c /imexd/imexd.cfg -N -j` inside one of the daemon pods**:
   ```bash
   DAEMON_POD=$(kubectl get pods -n nvidia-dra-driver-gpu -l "resource.nvidia.com/computeDomain=$CLAIM_UID" -o jsonpath='{.items[0].metadata.name}')
   kubectl exec -n nvidia-dra-driver-gpu "$DAEMON_POD" -- nvidia-imex-ctl -c /imexd/imexd.cfg -N -j | jq '{
     readyNodeCount: ([.nodes[] | select(.status == "READY")] | length),
     readyNodes: [.nodes | to_entries[] | select(.value.status == "READY") | {id: .key, status: .value.status, host: .value.host}]
   }'
   ```
   Expected output: `"readyNodeCount": 6`, and all 6 active nodes reporting `"status": "READY"`. *(Note: because `IMEXDaemonsWithDNSNames` is enabled, `/imexd/nodes.cfg` pre-allocates 18 DNS slots (`MAX_NODES_PER_IMEX_DOMAIN=18`), with the 6 active daemons mapped to their pod IPs and `READY`/`CONNECTED`, and the remaining 12 unused slots mapped to sentinel `127.0.0.2`.)*

6. **Inspect `ResourceClaim.status.devices[]` written by `compute-domain-controller`** (§7.5):
   ```bash
   kubectl get resourceclaim -n team-a "$CLAIM_NAME" -o jsonpath='{.status.devices}' | jq .
   ```
   Confirm:
   - `conditions`: `DomainProvisioned=True`, `NodesReady=True` (`message: "6/6"`), `CliqueSplit=False`, `Ready=True`
   - `data`: `cliques` and `nodes` (6 nodes, all `status: Ready`).

---

### Step 8: Test Variations Before Teardown (Walkthrough §9)

#### 8.1 Replacement Pod Rejoins the Existing Clique (§9 Bullet 1)
```bash
OLD_NODE=$(kubectl get pod -n team-a train-workers-0-5 -o jsonpath='{.spec.nodeName}')
kubectl delete pod -n team-a train-workers-0-5

echo "Node label on $OLD_NODE after pod deletion (expect empty):"
kubectl get node "$OLD_NODE" -o jsonpath='{.metadata.labels.resource\.nvidia\.com/computeDomain}'; echo ""

cat <<'EOF' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: train-workers-0-5
  namespace: team-a
  labels:
    app: train-workers
spec:
  schedulingGroup:
    podGroupName: train-workers-0
  restartPolicy: Never
  containers:
  - name: worker
    image: ubuntu:22.04
    command: ["bash", "-c", "sleep 3600"]
    resources:
      limits:
        nvidia.com/gpu: 4
      claims:
      - name: compute-domain
  resourceClaims:
  - name: compute-domain
    resourceClaimTemplateName: cd-domain
EOF

kubectl wait --for=condition=Ready pod/train-workers-0-5 -n team-a --timeout=120s
NEW_NODE=$(kubectl get pod -n team-a train-workers-0-5 -o jsonpath='{.spec.nodeName}')
echo "Replacement pod running on $NEW_NODE in clique $(kubectl get node "$NEW_NODE" -o jsonpath='{.metadata.labels.nvidia\.com/gpu\.clique}')"
kubectl get resourceclaim -n team-a "$CLAIM_NAME" -o jsonpath='{.status.devices[0].conditions}' | jq .
```

#### 8.2 Per-Node Exclusivity Rejection (`poc_brief.md` §6 Stage 4)
```bash
BUSY_NODE=$(kubectl get pod -n team-a train-workers-0-0 -o jsonpath='{.spec.nodeName}')
cat <<EOF | kubectl apply -f -
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: second-domain-claim
  namespace: team-a
spec:
  devices:
    requests:
    - name: domain
      exactly:
        deviceClassName: compute-domain.nvidia.com
---
apiVersion: v1
kind: Pod
metadata:
  name: conflict-pod
  namespace: team-a
spec:
  nodeName: $BUSY_NODE
  restartPolicy: Never
  containers:
  - name: ctr
    image: ubuntu:22.04
    command: ["bash", "-c", "sleep 3600"]
    resources:
      claims:
      - name: cd
  resourceClaims:
  - name: cd
    resourceClaimName: second-domain-claim
EOF

sleep 8
kubectl describe pod -n team-a conflict-pod | grep -A 5 "Events:"
kubectl delete pod -n team-a conflict-pod --force --grace-period=0
kubectl delete resourceclaim -n team-a second-domain-claim
```

---

### Step 9: Teardown — Unwinding the Domain from `PodGroup` Deletion (Walkthrough §8)

Delete the workload pods, `PodGroup`, and `Workload`:
```bash
kubectl delete pods -n team-a -l app=train-workers
kubectl delete podgroup -n team-a train-workers-0
kubectl delete workload -n team-a train
```

Confirm that the node labels, `ResourceClaim`, per-domain `DaemonSet`, and `ComputeDomainClique` are all removed:
```bash
sleep 10
kubectl get resourceclaims -n team-a
kubectl get nodes -l resource.nvidia.com/computeDomain
kubectl get daemonsets -n nvidia-dra-driver-gpu -l resource.nvidia.com/computeDomain
kubectl get computedomaincliques -n nvidia-dra-driver-gpu
```

---

### Step 10: Multi-Clique `CompositePodGroup` Workload Across Both Cliques (Walkthrough §9 Bullet 2 / Test T2)

Run a 12-node workload (`2 × 6` pods) with two child `PodGroup`s (`composite-train-workers-a` and `composite-train-workers-b`), producing two shared `ResourceClaim`s, two cliques, and two independent 6-node IMEX domains:

```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: scheduling.k8s.io/v1beta1
kind: Workload
metadata:
  name: composite-train
  namespace: team-a
spec:
  podGroupTemplates:
  - name: workers-a
    schedulingPolicy:
      gang:
        minCount: 6
    schedulingConstraints:
      topology:
      - key: nvidia.com/gpu.clique
    resourceClaims:
    - name: compute-domain
      resourceClaimTemplateName: cd-domain
  - name: workers-b
    schedulingPolicy:
      gang:
        minCount: 6
    schedulingConstraints:
      topology:
      - key: nvidia.com/gpu.clique
    resourceClaims:
    - name: compute-domain
      resourceClaimTemplateName: cd-domain
---
apiVersion: scheduling.k8s.io/v1beta1
kind: PodGroup
metadata:
  name: composite-train-workers-a
  namespace: team-a
spec:
  workloadRef:
    workloadName: composite-train
    templateName: workers-a
  schedulingPolicy:
    gang:
      minCount: 6
  schedulingConstraints:
    topology:
    - key: nvidia.com/gpu.clique
  resourceClaims:
  - name: compute-domain
    resourceClaimTemplateName: cd-domain
---
apiVersion: scheduling.k8s.io/v1beta1
kind: PodGroup
metadata:
  name: composite-train-workers-b
  namespace: team-a
spec:
  workloadRef:
    workloadName: composite-train
    templateName: workers-b
  schedulingPolicy:
    gang:
      minCount: 6
  schedulingConstraints:
    topology:
    - key: nvidia.com/gpu.clique
  resourceClaims:
  - name: compute-domain
    resourceClaimTemplateName: cd-domain
EOF

for grp in a b; do
  for i in 0 1 2 3 4 5; do
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: composite-worker-$grp-$i
  namespace: team-a
  labels:
    app: composite-train
    subjob: $grp
spec:
  schedulingGroup:
    podGroupName: composite-train-workers-$grp
  restartPolicy: Never
  containers:
  - name: worker
    image: ubuntu:22.04
    command: ["bash", "-c", "sleep 3600"]
    resources:
      limits:
        nvidia.com/gpu: 4
      claims:
      - name: compute-domain
  resourceClaims:
  - name: compute-domain
    resourceClaimTemplateName: cd-domain
EOF
  done
done

kubectl wait --for=condition=Ready pod -l app=composite-train -n team-a --timeout=120s
kubectl get resourceclaims -n team-a -o json | jq '.items[] | {name: .metadata.name, uid: .metadata.uid, pool: .status.allocation.devices.results[0].pool, conditions: .status.devices[0].conditions}'
kubectl get computedomaincliques -n nvidia-dra-driver-gpu
```

Clean up when finished:
```bash
kubectl delete pods -n team-a -l app=composite-train
kubectl delete podgroup -n team-a composite-train-workers-a composite-train-workers-b
kubectl delete workload -n team-a composite-train
```
