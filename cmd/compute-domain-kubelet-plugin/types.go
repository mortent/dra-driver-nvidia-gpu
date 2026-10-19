/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"

	resourcev1 "k8s.io/api/resource/v1"

	"sigs.k8s.io/dra-driver-nvidia-gpu/internal/resourceclaimutils"
)

const (
	ComputeDomainChannelType = "channel"
	ComputeDomainDaemonType  = "daemon"
	UnknownDeviceType        = "unknown"

	domainPoolPrefix = "nvlink-domain-"
)

func ResourceClaimToString(rc *resourcev1.ResourceClaim) string {
	return resourceclaimutils.ToString(rc)
}

// deviceRequestKey returns the checkpoint overlap key for a device allocation
// result. Shared allocations (such as consumable domain devices where each
// claim has a distinct ShareID) are keyed by "device/shareID"; non-shared
// allocations are keyed by device name.
func deviceRequestKey(r resourcev1.DeviceRequestAllocationResult) string {
	if r.ShareID != nil && *r.ShareID != "" {
		return fmt.Sprintf("%s/%s", r.Device, *r.ShareID)
	}
	return r.Device
}
