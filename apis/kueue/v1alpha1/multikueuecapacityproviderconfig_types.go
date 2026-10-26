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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// MultiKueueCapacityProviderControllerName is the spec.controllerName of the
	// CapacityProviders that the built-in MultiKueue CapacityProvider manages.
	MultiKueueCapacityProviderControllerName CapacityProviderControllerName = "kueue.x-k8s.io/multikueue"

	// MultiKueueCapacityProviderConfigKind is the kind that spec.parameters of a
	// MultiKueue CapacityProvider refers to.
	MultiKueueCapacityProviderConfigKind = "MultiKueueCapacityProviderConfig"
)

// TODO(#17401): re-evaluate for Beta the Cohort scope and more than one manager
// flavor (for example, separate TAS and non-TAS quotas). Both can add fields here.

// MultiKueueCapacityProviderConfigSpec defines the configuration of the MultiKueue CapacityProvider.
// +kubebuilder:validation:XValidation:rule="self.managerSubtreeRootRef.kind == 'ClusterQueue'", message="only the ClusterQueue kind is allowed for managerSubtreeRootRef"
type MultiKueueCapacityProviderConfigSpec struct {
	// managerSubtreeRootRef selects the manager ClusterQueue
	// whose related worker ClusterQueues the provider sums.
	// Only kind ClusterQueue is supported.
	// +required
	ManagerSubtreeRootRef CapacityDistributionSubtreeRootRef `json:"managerSubtreeRootRef,omitzero"`
}

// TODO(#17401): limit metadata.name to 63 characters, because
// CapacityProvider spec.parameters.name allows at most 63 characters.

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:resource:scope=Cluster

// MultiKueueCapacityProviderConfig is the configuration of the MultiKueue CapacityProvider.
type MultiKueueCapacityProviderConfig struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the configuration of the MultiKueue CapacityProvider.
	// +required
	Spec MultiKueueCapacityProviderConfigSpec `json:"spec,omitzero"`
}

// +kubebuilder:object:root=true

// MultiKueueCapacityProviderConfigList contains a list of MultiKueueCapacityProviderConfig
type MultiKueueCapacityProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MultiKueueCapacityProviderConfig `json:"items"`
}
