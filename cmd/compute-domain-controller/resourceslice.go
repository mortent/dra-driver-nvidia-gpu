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

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	domainPoolPrefix = "nvlink-domain-"
	domainDeviceName = "domain"
	domainDeviceType = "domain"
)

// DomainSliceManager watches cluster nodes via an unfiltered Node informer,
// groups nodes by the configured topology label key, and publishes one
// capacity-less shareable domain device per clique pool via the DRA
// ResourceSlice controller.
type DomainSliceManager struct {
	mu            sync.Mutex
	config        *ManagerConfig
	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc

	factory         informers.SharedInformerFactory
	informer        cache.SharedIndexInformer
	nodeSelector    labels.Selector
	sliceController *resourceslice.Controller

	poolGenerations map[string]int64
	poolMembers     map[string]string
}

// NewDomainSliceManager creates a new DomainSliceManager with an unfiltered Node informer.
func NewDomainSliceManager(config *ManagerConfig) *DomainSliceManager {
	factory := informers.NewSharedInformerFactory(config.clientsets.Core, informerResyncPeriod)
	informer := factory.Core().V1().Nodes().Informer()

	return &DomainSliceManager{
		config:          config,
		factory:         factory,
		informer:        informer,
		poolGenerations: make(map[string]int64),
		poolMembers:     make(map[string]string),
	}
}

// Start initializes existing pool generations, starts the unfiltered Node informer
// and DRA ResourceSlice controller, and performs the initial pool synchronization.
func (m *DomainSliceManager) Start(ctx context.Context) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel

	defer func() {
		if rerr != nil {
			if err := m.Stop(); err != nil {
				klog.Errorf("error stopping DomainSliceManager: %v", err)
			}
		}
	}()

	if m.config.domainNodeSelector != "" {
		sel, err := labels.Parse(m.config.domainNodeSelector)
		if err != nil {
			return fmt.Errorf("invalid domain-node-selector %q: %w", m.config.domainNodeSelector, err)
		}
		m.nodeSelector = sel
	} else {
		m.nodeSelector = labels.Everything()
	}

	if err := m.initPoolGenerations(ctx); err != nil {
		klog.Warningf("DomainSliceManager: unable to seed existing pool generations (starting from 1): %v", err)
	}

	_, err := m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.syncPools()
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldNode, okOld := oldObj.(*corev1.Node)
			newNode, okNew := newObj.(*corev1.Node)
			if okOld && okNew {
				oldVal := oldNode.Labels[m.config.domainTopologyKey]
				newVal := newNode.Labels[m.config.domainTopologyKey]
				oldMatch := m.matchesSelector(oldNode)
				newMatch := m.matchesSelector(newNode)
				if oldVal == newVal && oldMatch == newMatch {
					return
				}
			}
			m.syncPools()
		},
		DeleteFunc: func(obj any) {
			m.syncPools()
		},
	})
	if err != nil {
		return fmt.Errorf("error adding event handlers for DomainSliceManager Node informer: %w", err)
	}

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
	}()

	if !cache.WaitForCacheSync(ctx.Done(), m.informer.HasSynced) {
		return fmt.Errorf("DomainSliceManager: node informer cache sync failed")
	}

	sliceController, err := resourceslice.StartController(ctx, resourceslice.Options{
		DriverName: m.config.driverName,
		KubeClient: m.config.clientsets.Core,
		Owner:      nil,
		Resources: &resourceslice.DriverResources{
			Pools: make(map[string]resourceslice.Pool),
		},
	})
	if err != nil {
		return fmt.Errorf("DomainSliceManager: failed to start ResourceSlice controller: %w", err)
	}

	m.mu.Lock()
	m.sliceController = sliceController
	m.mu.Unlock()

	m.syncPools()
	return nil
}

// Stop stops the ResourceSlice controller and the unfiltered Node informer.
func (m *DomainSliceManager) Stop() error {
	m.mu.Lock()
	sc := m.sliceController
	m.sliceController = nil
	m.mu.Unlock()

	if sc != nil {
		sc.Stop()
	}
	if m.cancelContext != nil {
		m.cancelContext()
	}
	m.waitGroup.Wait()
	return nil
}

// initPoolGenerations seeds per-pool generations from existing cluster ResourceSlices
// so that pool generations remain monotonically increasing across controller restarts.
func (m *DomainSliceManager) initPoolGenerations(ctx context.Context) error {
	sliceList, err := m.config.clientsets.Core.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range sliceList.Items {
		slice := &sliceList.Items[i]
		if slice.Spec.Driver != m.config.driverName {
			continue
		}
		poolName := slice.Spec.Pool.Name
		if !strings.HasPrefix(poolName, domainPoolPrefix) {
			continue
		}
		if slice.Spec.Pool.Generation > m.poolGenerations[poolName] {
			m.poolGenerations[poolName] = slice.Spec.Pool.Generation
		}
	}
	return nil
}

func (m *DomainSliceManager) matchesSelector(node *corev1.Node) bool {
	if m.nodeSelector == nil {
		return true
	}
	return m.nodeSelector.Matches(labels.Set(node.Labels))
}

func (m *DomainSliceManager) syncPools() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sliceController == nil || m.config.domainTopologyKey == "" {
		return
	}

	nodesByValue := make(map[string][]string)
	for _, item := range m.informer.GetStore().List() {
		node, ok := item.(*corev1.Node)
		if !ok {
			continue
		}
		if !m.matchesSelector(node) {
			continue
		}
		val := node.Labels[m.config.domainTopologyKey]
		if val == "" {
			continue
		}
		nodesByValue[val] = append(nodesByValue[val], node.Name)
	}

	pools := make(map[string]resourceslice.Pool, len(nodesByValue))
	activePools := make(map[string]struct{}, len(nodesByValue))

	for val, nodeNames := range nodesByValue {
		slices.Sort(nodeNames)
		membershipKey := strings.Join(nodeNames, ",")
		poolName := domainPoolPrefix + val
		activePools[poolName] = struct{}{}

		gen := m.poolGenerations[poolName]
		prevMembers, seen := m.poolMembers[poolName]
		if !seen {
			if gen == 0 {
				gen = 1
			} else {
				gen++
			}
		} else if prevMembers != membershipKey {
			gen++
		}
		m.poolGenerations[poolName] = gen
		m.poolMembers[poolName] = membershipKey

		nodeCount := int64(len(nodeNames))
		pools[poolName] = resourceslice.Pool{
			Generation: gen,
			NodeSelector: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      m.config.domainTopologyKey,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{val},
							},
						},
					},
				},
			},
			Slices: []resourceslice.Slice{
				{
					Devices: []resourceapi.Device{
						{
							Name:                     domainDeviceName,
							AllowMultipleAllocations: ptr.To(true),
							Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
								"type": {
									StringValue: ptr.To(domainDeviceType),
								},
								"topologyKey": {
									StringValue: ptr.To(m.config.domainTopologyKey),
								},
								"topologyValue": {
									StringValue: ptr.To(val),
								},
								"nodeCount": {
									IntValue: ptr.To(nodeCount),
								},
							},
						},
					},
				},
			},
		}
	}

	for poolName := range m.poolMembers {
		if _, exists := activePools[poolName]; !exists {
			delete(m.poolMembers, poolName)
		}
	}

	m.sliceController.Update(&resourceslice.DriverResources{
		Pools: pools,
	})
}
