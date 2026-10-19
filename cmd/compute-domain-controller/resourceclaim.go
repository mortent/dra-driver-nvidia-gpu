/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

const (
	domainClaimFinalizer = computeDomainFinalizer

	domainConditionReady             = "Ready"
	domainConditionDomainProvisioned = "DomainProvisioned"
	domainConditionNodesReady        = "NodesReady"
	domainConditionCliqueSplit       = "CliqueSplit"
)

type domainDeviceStatusData struct {
	Cliques []string                   `json:"cliques"`
	Nodes   []*nvapi.ComputeDomainNode `json:"nodes"`
}

// DomainClaimManager watches ResourceClaims across all namespaces, filters to
// allocated domain claims (pool prefix "nvlink-domain-"), creates the
// per-domain IMEX DaemonSet when allocated, reports domain readiness on
// ResourceClaim.status.devices[], and tears down the DaemonSet,
// ComputeDomainCliques, and leftover node labels when the claim is deallocated
// or deleted.
type DomainClaimManager struct {
	config        *ManagerConfig
	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc

	factory       informers.SharedInformerFactory
	informer      cache.SharedIndexInformer
	mutationCache cache.MutationCache

	daemonSetManager *DaemonSetManager
	nodeManager      *NodeManager
}

// NewDomainClaimManager creates a new DomainClaimManager watching cluster-wide ResourceClaims.
func NewDomainClaimManager(config *ManagerConfig, daemonSetManager *DaemonSetManager, nodeManager *NodeManager) *DomainClaimManager {
	factory := informers.NewSharedInformerFactory(config.clientsets.Core, informerResyncPeriod)
	informer := factory.Resource().V1().ResourceClaims().Informer()

	return &DomainClaimManager{
		config:           config,
		factory:          factory,
		informer:         informer,
		daemonSetManager: daemonSetManager,
		nodeManager:      nodeManager,
	}
}

// Start registers the UID indexer and event handlers, starts the ResourceClaim
// informer factory, waits for cache synchronization, and launches the periodic
// ResourceClaim.status.devices[] sync loop.
func (m *DomainClaimManager) Start(ctx context.Context) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel

	defer func() {
		if rerr != nil {
			if err := m.Stop(); err != nil {
				klog.Errorf("error stopping DomainClaimManager: %v", err)
			}
		}
	}()

	err := m.informer.AddIndexers(cache.Indexers{
		"uid": uidIndexer[*resourceapi.ResourceClaim],
	})
	if err != nil {
		return fmt.Errorf("error adding UID indexer for ResourceClaim informer: %w", err)
	}

	m.mutationCache = cache.NewIntegerResourceVersionMutationCache(
		klog.Background(),
		m.informer.GetStore(),
		m.informer.GetIndexer(),
		mutationCacheTTL,
		true,
	)

	_, err = m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			claim, ok := obj.(*resourceapi.ResourceClaim)
			if !ok || !isDomainClaim(claim) {
				return
			}
			m.config.workQueue.Enqueue(claim, m.onAddOrUpdate)
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldClaim, _ := oldObj.(*resourceapi.ResourceClaim)
			newClaim, ok := newObj.(*resourceapi.ResourceClaim)
			if !ok {
				return
			}
			if !isDomainClaim(newClaim) && !isDomainClaim(oldClaim) {
				return
			}
			m.config.workQueue.Enqueue(newClaim, m.onAddOrUpdate)
		},
		DeleteFunc: func(obj any) {
			var claim *resourceapi.ResourceClaim
			switch t := obj.(type) {
			case *resourceapi.ResourceClaim:
				claim = t
			case cache.DeletedFinalStateUnknown:
				c, ok := t.Obj.(*resourceapi.ResourceClaim)
				if !ok {
					return
				}
				claim = c
			default:
				return
			}
			if !isDomainClaim(claim) {
				return
			}
			m.config.workQueue.Enqueue(claim, m.onDelete)
		},
	})
	if err != nil {
		return fmt.Errorf("error adding event handlers for ResourceClaim informer: %w", err)
	}

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
	}()

	if !cache.WaitForCacheSync(ctx.Done(), m.informer.HasSynced) {
		return fmt.Errorf("DomainClaimManager: ResourceClaim informer cache sync failed")
	}

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.startPeriodicStatusSync(ctx)
	}()

	return nil
}

// Stop stops the DomainClaimManager informer and status sync loops.
func (m *DomainClaimManager) Stop() error {
	if m.cancelContext != nil {
		m.cancelContext()
	}
	m.waitGroup.Wait()
	return nil
}

// Get retrieves a ResourceClaim by UID from the mutation cache.
func (m *DomainClaimManager) Get(uid string) (*resourceapi.ResourceClaim, error) {
	if m.mutationCache == nil {
		return nil, nil
	}
	objs, err := m.mutationCache.ByIndex("uid", uid)
	if err != nil {
		return nil, fmt.Errorf("error retrieving ResourceClaim by UID: %w", err)
	}
	if len(objs) == 0 {
		return nil, nil
	}
	if len(objs) != 1 {
		return nil, fmt.Errorf("multiple ResourceClaims with the same UID %s", uid)
	}
	claim, ok := objs[0].(*resourceapi.ResourceClaim)
	if !ok {
		return nil, fmt.Errorf("failed to cast object to ResourceClaim")
	}
	return claim, nil
}

// GetActiveDomainClaim returns the ResourceClaim with the given UID if and only
// if it is currently allocated to a domain pool ("nvlink-domain-*") and not
// marked for deletion.
func (m *DomainClaimManager) GetActiveDomainClaim(uid string) (*resourceapi.ResourceClaim, error) {
	claim, err := m.Get(uid)
	if err != nil || claim == nil {
		return nil, err
	}
	if !claim.DeletionTimestamp.IsZero() || !isAllocatedDomainClaim(claim) {
		return nil, nil
	}
	return claim, nil
}

func isAllocatedDomainClaim(claim *resourceapi.ResourceClaim) bool {
	if claim == nil || claim.Status.Allocation == nil {
		return false
	}
	for i := range claim.Status.Allocation.Devices.Results {
		res := &claim.Status.Allocation.Devices.Results[i]
		if res.Driver == DriverName && strings.HasPrefix(res.Pool, domainPoolPrefix) {
			return true
		}
	}
	return false
}

func hasDomainClaimFinalizer(claim *resourceapi.ResourceClaim) bool {
	if claim == nil {
		return false
	}
	return slices.Contains(claim.Finalizers, domainClaimFinalizer) ||
		slices.Contains(claim.Finalizers, computeDomainFinalizer)
}

func isDomainClaim(claim *resourceapi.ResourceClaim) bool {
	return isAllocatedDomainClaim(claim) || hasDomainClaimFinalizer(claim)
}

func (m *DomainClaimManager) onAddOrUpdate(ctx context.Context, obj any) error {
	target, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return fmt.Errorf("failed to cast to ResourceClaim")
	}

	claimUID := string(target.UID)
	claim, err := m.Get(claimUID)
	if err != nil {
		return fmt.Errorf("error getting ResourceClaim %s: %w", claimUID, err)
	}
	if claim == nil {
		return m.cleanupClaimResources(ctx, claimUID)
	}

	if !claim.DeletionTimestamp.IsZero() || !isAllocatedDomainClaim(claim) {
		klog.V(2).Infof("DomainClaimManager: tearing down domain resources for deallocated/deleting ResourceClaim %s/%s (%s)", claim.Namespace, claim.Name, claim.UID)
		if err := m.cleanupClaimResources(ctx, claimUID); err != nil {
			return err
		}
		if err := m.removeFinalizer(ctx, claim); err != nil {
			return fmt.Errorf("error removing finalizer from ResourceClaim %s/%s: %w", claim.Namespace, claim.Name, err)
		}
		return nil
	}

	klog.V(2).Infof("DomainClaimManager: reconciling allocated domain ResourceClaim %s/%s (%s)", claim.Namespace, claim.Name, claim.UID)

	updatedClaim, err := m.addFinalizer(ctx, claim)
	if err != nil {
		return fmt.Errorf("error adding finalizer to ResourceClaim %s/%s: %w", claim.Namespace, claim.Name, err)
	}
	claim = updatedClaim

	if m.nodeManager != nil {
		m.nodeManager.RemoveStaleComputeDomainLabelsAsync(ctx)
	}

	if m.daemonSetManager != nil {
		if _, err := m.daemonSetManager.CreateForClaim(ctx, claim); err != nil {
			return fmt.Errorf("error creating DaemonSet for ResourceClaim %s/%s (%s): %w", claim.Namespace, claim.Name, claim.UID, err)
		}
	}

	if err := m.syncClaimDeviceStatus(ctx, claim); err != nil {
		klog.V(4).Infof("DomainClaimManager: transient error syncing device status for ResourceClaim %s/%s: %v", claim.Namespace, claim.Name, err)
	}

	return nil
}

func (m *DomainClaimManager) onDelete(ctx context.Context, obj any) error {
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return fmt.Errorf("failed to cast to ResourceClaim")
	}
	return m.cleanupClaimResources(ctx, string(claim.UID))
}

func (m *DomainClaimManager) startPeriodicStatusSync(ctx context.Context) {
	ticker := time.NewTicker(cdStatusSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.syncAllActiveClaimsStatus(ctx)
		}
	}
}

func (m *DomainClaimManager) syncAllActiveClaimsStatus(ctx context.Context) {
	for _, obj := range m.informer.GetStore().List() {
		claim, ok := obj.(*resourceapi.ResourceClaim)
		if !ok || claim == nil {
			continue
		}
		if !claim.DeletionTimestamp.IsZero() || !isAllocatedDomainClaim(claim) {
			continue
		}
		latest, err := m.Get(string(claim.UID))
		if err != nil || latest == nil || !latest.DeletionTimestamp.IsZero() || !isAllocatedDomainClaim(latest) {
			continue
		}
		if err := m.syncClaimDeviceStatus(ctx, latest); err != nil {
			klog.V(4).Infof("DomainClaimManager: error syncing device status for ResourceClaim %s/%s: %v", latest.Namespace, latest.Name, err)
		}
	}
}

// syncClaimDeviceStatus computes and updates ResourceClaim.status.devices[] for
// all domain device allocations on the claim.
func (m *DomainClaimManager) syncClaimDeviceStatus(ctx context.Context, claim *resourceapi.ResourceClaim) error {
	if claim == nil || claim.Status.Allocation == nil || !claim.DeletionTimestamp.IsZero() {
		return nil
	}

	claimUID := string(claim.UID)

	// 1. Check whether the per-domain DaemonSet exists.
	daemonSetExists := false
	if m.daemonSetManager != nil {
		ds, err := m.daemonSetManager.Get(ctx, claimUID)
		if err != nil {
			return fmt.Errorf("error checking DaemonSet for claim %s: %w", claimUID, err)
		}
		daemonSetExists = ds != nil
	}

	// 2. Count nodes labeled resource.nvidia.com/computeDomain=<claim.UID>.
	labeledNodes, err := m.config.clientsets.Core.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", computeDomainLabelKey, claimUID),
	})
	if err != nil {
		return fmt.Errorf("error listing labeled nodes for claim %s: %w", claimUID, err)
	}
	expectedNodes := len(labeledNodes.Items)

	// 3. List ComputeDomainClique objects for this claim UID.
	cliques, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliques(m.config.driverNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", computeDomainLabelKey, claimUID),
	})
	if err != nil {
		return fmt.Errorf("error listing ComputeDomainCliques for claim %s: %w", claimUID, err)
	}

	var cliqueNames []string
	var domainNodes []*nvapi.ComputeDomainNode
	seenNodes := make(map[string]struct{})

	for i := range cliques.Items {
		clique := &cliques.Items[i]
		cliqueID := clique.Labels[computeDomainCliqueLabelKey]
		if cliqueID == "" {
			cliqueID = clique.Name
		}
		if !slices.Contains(cliqueNames, cliqueID) {
			cliqueNames = append(cliqueNames, cliqueID)
		}
		for _, daemon := range clique.Daemons {
			if _, exists := seenNodes[daemon.NodeName]; exists {
				continue
			}
			seenNodes[daemon.NodeName] = struct{}{}
			domainNodes = append(domainNodes, &nvapi.ComputeDomainNode{
				Name:      daemon.NodeName,
				IPAddress: daemon.IPAddress,
				CliqueID:  daemon.CliqueID,
				Index:     daemon.Index,
				Status:    daemon.Status,
			})
		}
	}

	// Fallback for non-fabric daemon pods where no ComputeDomainClique is created.
	if len(domainNodes) == 0 && expectedNodes > 0 {
		pods, err := m.config.clientsets.Core.CoreV1().Pods(m.config.driverNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s", computeDomainLabelKey, claimUID),
		})
		if err == nil {
			for i := range pods.Items {
				pod := &pods.Items[i]
				if pod.Spec.NodeName == "" || pod.Status.PodIP == "" {
					continue
				}
				if _, exists := seenNodes[pod.Spec.NodeName]; exists {
					continue
				}
				seenNodes[pod.Spec.NodeName] = struct{}{}
				status := nvapi.ComputeDomainStatusNotReady
				for _, cond := range pod.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						status = nvapi.ComputeDomainStatusReady
						break
					}
				}
				domainNodes = append(domainNodes, &nvapi.ComputeDomainNode{
					Name:      pod.Spec.NodeName,
					IPAddress: pod.Status.PodIP,
					CliqueID:  "",
					Index:     -1,
					Status:    status,
				})
			}
		}
	}

	slices.Sort(cliqueNames)
	slices.SortFunc(domainNodes, func(a, b *nvapi.ComputeDomainNode) int {
		return strings.Compare(a.Name, b.Name)
	})
	if cliqueNames == nil {
		cliqueNames = []string{}
	}
	if domainNodes == nil {
		domainNodes = []*nvapi.ComputeDomainNode{}
	}

	readyNodes := 0
	for _, node := range domainNodes {
		if node.Status == nvapi.ComputeDomainStatusReady {
			readyNodes++
		}
	}
	if expectedNodes < len(domainNodes) {
		expectedNodes = len(domainNodes)
	}

	isSplit := len(cliques.Items) > 1
	allNodesReady := expectedNodes > 0 && readyNodes == expectedNodes
	isReady := daemonSetExists && allNodesReady && !isSplit

	rawData, err := json.Marshal(domainDeviceStatusData{
		Cliques: cliqueNames,
		Nodes:   domainNodes,
	})
	if err != nil {
		return fmt.Errorf("error marshaling domainDeviceStatusData: %w", err)
	}

	// Preserve AllocatedDeviceStatus entries owned by other drivers.
	var newDevices []resourceapi.AllocatedDeviceStatus
	for i := range claim.Status.Devices {
		dev := &claim.Status.Devices[i]
		if dev.Driver == DriverName && strings.HasPrefix(dev.Pool, domainPoolPrefix) {
			continue
		}
		newDevices = append(newDevices, *dev.DeepCopy())
	}

	now := metav1.Now()
	for i := range claim.Status.Allocation.Devices.Results {
		res := &claim.Status.Allocation.Devices.Results[i]
		if res.Driver != DriverName || !strings.HasPrefix(res.Pool, domainPoolPrefix) {
			continue
		}

		var shareID *string
		if res.ShareID != nil {
			s := string(*res.ShareID)
			shareID = &s
		}

		existing := findExistingAllocatedDeviceStatus(claim.Status.Devices, res.Driver, res.Pool, res.Device, shareID)

		provStatus := metav1.ConditionFalse
		provReason := "DaemonSetPending"
		provMsg := "Waiting for IMEX DaemonSet creation"
		if daemonSetExists {
			provStatus = metav1.ConditionTrue
			provReason = "DaemonSetCreated"
			provMsg = fmt.Sprintf("IMEX DaemonSet computedomain-daemon-%s provisioned in %s", claimUID, m.config.driverNamespace)
		}

		nodesStatus := metav1.ConditionFalse
		nodesReason := "NodesNotReady"
		if allNodesReady {
			nodesStatus = metav1.ConditionTrue
			nodesReason = "AllNodesReady"
		}
		nodesMsg := fmt.Sprintf("%d/%d", readyNodes, expectedNodes)

		splitStatus := metav1.ConditionFalse
		splitReason := "SingleClique"
		splitMsg := "All domain daemons belong to a single NVLink clique"
		if isSplit {
			splitStatus = metav1.ConditionTrue
			splitReason = "MultipleCliquesDetected"
			splitMsg = fmt.Sprintf("Domain spans %d cliques", len(cliques.Items))
		}

		readyStatus := metav1.ConditionFalse
		readyReason := "Provisioning"
		readyMsg := fmt.Sprintf("Provisioning IMEX domain (%d/%d nodes ready)", readyNodes, expectedNodes)
		if isSplit {
			readyReason = "CliqueSplit"
			readyMsg = fmt.Sprintf("IMEX domain split across %d cliques", len(cliques.Items))
		} else if isReady {
			readyStatus = metav1.ConditionTrue
			readyReason = "DomainReady"
			readyMsg = fmt.Sprintf("IMEX domain ready across %d/%d nodes", readyNodes, expectedNodes)
		}

		conditions := []metav1.Condition{
			buildDeviceCondition(existing, domainConditionDomainProvisioned, provStatus, provReason, provMsg, now),
			buildDeviceCondition(existing, domainConditionNodesReady, nodesStatus, nodesReason, nodesMsg, now),
			buildDeviceCondition(existing, domainConditionCliqueSplit, splitStatus, splitReason, splitMsg, now),
			buildDeviceCondition(existing, domainConditionReady, readyStatus, readyReason, readyMsg, now),
		}

		newDevices = append(newDevices, resourceapi.AllocatedDeviceStatus{
			Driver:     res.Driver,
			Pool:       res.Pool,
			Device:     res.Device,
			ShareID:    shareID,
			Conditions: conditions,
			Data: &runtime.RawExtension{
				Raw: rawData,
			},
		})
	}

	if allocatedDeviceStatusesEqual(claim.Status.Devices, newDevices) {
		return nil
	}

	newClaim := claim.DeepCopy()
	newClaim.Status.Devices = newDevices
	updated, err := m.config.clientsets.Core.ResourceV1().ResourceClaims(newClaim.Namespace).UpdateStatus(ctx, newClaim, metav1.UpdateOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("error updating ResourceClaim %s/%s device status: %w", newClaim.Namespace, newClaim.Name, err)
	}
	m.mutationCache.Mutation(updated)
	return nil
}

func findExistingAllocatedDeviceStatus(devices []resourceapi.AllocatedDeviceStatus, driver, pool, device string, shareID *string) *resourceapi.AllocatedDeviceStatus {
	for i := range devices {
		d := &devices[i]
		if d.Driver == driver && d.Pool == pool && d.Device == device && stringPtrEqual(d.ShareID, shareID) {
			return d
		}
	}
	return nil
}

func buildDeviceCondition(existing *resourceapi.AllocatedDeviceStatus, condType string, status metav1.ConditionStatus, reason, message string, now metav1.Time) metav1.Condition {
	transitionTime := now
	if existing != nil {
		for i := range existing.Conditions {
			c := &existing.Conditions[i]
			if c.Type == condType && c.Status == status && !c.LastTransitionTime.IsZero() {
				transitionTime = c.LastTransitionTime
				break
			}
		}
	}
	return metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: transitionTime,
	}
}

func stringPtrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func allocatedDeviceStatusesEqual(a, b []resourceapi.AllocatedDeviceStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Driver != b[i].Driver || a[i].Pool != b[i].Pool || a[i].Device != b[i].Device || !stringPtrEqual(a[i].ShareID, b[i].ShareID) {
			return false
		}
		if len(a[i].Conditions) != len(b[i].Conditions) {
			return false
		}
		for j := range a[i].Conditions {
			ca := &a[i].Conditions[j]
			cb := &b[i].Conditions[j]
			if ca.Type != cb.Type || ca.Status != cb.Status || ca.Reason != cb.Reason || ca.Message != cb.Message {
				return false
			}
		}
		var rawA, rawB []byte
		if a[i].Data != nil {
			rawA = a[i].Data.Raw
		}
		if b[i].Data != nil {
			rawB = b[i].Data.Raw
		}
		if !bytes.Equal(rawA, rawB) {
			return false
		}
	}
	return true
}

func (m *DomainClaimManager) cleanupClaimResources(ctx context.Context, cdUID string) error {
	if m.daemonSetManager != nil {
		if err := m.daemonSetManager.Delete(ctx, cdUID); err != nil {
			return fmt.Errorf("error deleting DaemonSet for claim %s: %w", cdUID, err)
		}
	}

	if m.nodeManager != nil {
		if err := m.nodeManager.RemoveComputeDomainLabels(ctx, cdUID); err != nil {
			return fmt.Errorf("error removing ComputeDomain node labels for claim %s: %w", cdUID, err)
		}
	}

	if err := m.deleteCliquesForDomain(ctx, cdUID); err != nil {
		return fmt.Errorf("error deleting ComputeDomainCliques for claim %s: %w", cdUID, err)
	}

	if m.daemonSetManager != nil {
		if err := m.daemonSetManager.RemoveFinalizer(ctx, cdUID); err != nil {
			return fmt.Errorf("error removing finalizer on DaemonSet for claim %s: %w", cdUID, err)
		}
		if err := m.daemonSetManager.AssertRemoved(ctx, cdUID); err != nil {
			return fmt.Errorf("error asserting removal of DaemonSet for claim %s: %w", cdUID, err)
		}
	}

	return nil
}

func (m *DomainClaimManager) deleteCliquesForDomain(ctx context.Context, cdUID string) error {
	cliques, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliques(m.config.driverNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", computeDomainLabelKey, cdUID),
	})
	if err != nil {
		return fmt.Errorf("error listing ComputeDomainCliques for claim %s: %w", cdUID, err)
	}

	for i := range cliques.Items {
		clique := &cliques.Items[i]
		if len(clique.Finalizers) > 0 {
			newClique := clique.DeepCopy()
			newClique.Finalizers = nil
			if _, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliques(clique.Namespace).Update(ctx, newClique, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("error removing finalizers from ComputeDomainClique %s/%s: %w", clique.Namespace, clique.Name, err)
			}
		}
		if err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliques(clique.Namespace).Delete(ctx, clique.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("error deleting ComputeDomainClique %s/%s: %w", clique.Namespace, clique.Name, err)
		}
	}

	return nil
}

func (m *DomainClaimManager) addFinalizer(ctx context.Context, claim *resourceapi.ResourceClaim) (*resourceapi.ResourceClaim, error) {
	if slices.Contains(claim.Finalizers, domainClaimFinalizer) {
		return claim, nil
	}

	newClaim := claim.DeepCopy()
	newClaim.Finalizers = append(newClaim.Finalizers, domainClaimFinalizer)
	updated, err := m.config.clientsets.Core.ResourceV1().ResourceClaims(newClaim.Namespace).Update(ctx, newClaim, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	m.mutationCache.Mutation(updated)
	return updated, nil
}

func (m *DomainClaimManager) removeFinalizer(ctx context.Context, claim *resourceapi.ResourceClaim) error {
	if !hasDomainClaimFinalizer(claim) {
		return nil
	}

	newClaim := claim.DeepCopy()
	newClaim.Finalizers = make([]string, 0, len(claim.Finalizers))
	for _, f := range claim.Finalizers {
		if f != domainClaimFinalizer && f != computeDomainFinalizer {
			newClaim.Finalizers = append(newClaim.Finalizers, f)
		}
	}
	if len(newClaim.Finalizers) == len(claim.Finalizers) {
		return nil
	}

	updated, err := m.config.clientsets.Core.ResourceV1().ResourceClaims(newClaim.Namespace).Update(ctx, newClaim, metav1.UpdateOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	m.mutationCache.Mutation(updated)
	return nil
}
