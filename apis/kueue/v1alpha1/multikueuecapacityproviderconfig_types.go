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

type MultiKueueCapacityProviderConfigSpec struct {
	// multiKueueConfigName is the name of the MultiKueueConfig that defines
	// the worker clusters from which capacity is aggregated.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"
	MultiKueueConfigName string `json:"multiKueueConfigName,omitempty"`

	// managerQuotaScope bounds which manager ClusterQueues contribute
	// LocalQueues when discovering related worker ClusterQueues.
	// Only manager ClusterQueues in this scope that also reference a
	// MultiKueue AdmissionCheck backed by multiKueueConfigName are included.
	//
	// +required
	ManagerQuotaScope CapacityDistributionSubtreeRootRef `json:"managerQuotaScope,omitzero"`
}

// MultiKueueCapacityProviderConfig holds the settings of the built-in
// MultiKueue CapacityProvider.
//
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:resource:scope=Cluster
type MultiKueueCapacityProviderConfig struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec is the specification of the MultiKueueCapacityProviderConfig.
	// +required
	Spec MultiKueueCapacityProviderConfigSpec `json:"spec,omitzero"`
}

// +kubebuilder:object:root=true

// MultiKueueCapacityProviderConfigList contains a list of MultiKueueCapacityProviderConfig.
type MultiKueueCapacityProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MultiKueueCapacityProviderConfig `json:"items"`
}
