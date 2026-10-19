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
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	domainClaimFinalizer = computeDomainFinalizer
)

// DomainClaimManager watches ResourceClaims across all namespaces, filters to
// allocated domain claims (pool prefix "nvlink-domain-"), creates the
// per-domain IMEX DaemonSet when allocated, and tears down the DaemonSet,
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
// informer factory, and waits for cache synchronization.
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

	return nil
}

// Stop stops the DomainClaimManager informer loop.
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

	return nil
}

func (m *DomainClaimManager) onDelete(ctx context.Context, obj any) error {
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return fmt.Errorf("failed to cast to ResourceClaim")
	}
	return m.cleanupClaimResources(ctx, string(claim.UID))
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
