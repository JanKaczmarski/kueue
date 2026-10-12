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

package multikueue

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/util/api"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
)

const multiKueueCapacityProviderConfigKind = "MultiKueueCapacityProviderConfig"

// errCapacityProviderMisconfigured marks configuration errors that are reported
// through the CapacitySynchronized=False/Misconfigured condition instead of being retried.
var errCapacityProviderMisconfigured = errors.New("misconfigured")

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=capacityproviders,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=capacityproviders/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=multikueuecapacityproviderconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=cohorts,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=localqueues,verbs=get;list;watch

// cpReconciler publishes the capacity of the related worker ClusterQueues into the
// status of CapacityProviders whose controllerName is kueue.x-k8s.io/multikueue (KEP-9988 Alpha2).
type cpReconciler struct {
	client            client.Client
	clusters          *clustersReconciler
	roleTracker       *roletracker.RoleTracker
	eventsBatchPeriod time.Duration
}

var _ reconcile.Reconciler = (*cpReconciler)(nil)

func newCPReconciler(c client.Client, clusters *clustersReconciler, roleTracker *roletracker.RoleTracker, eventsBatchPeriod time.Duration) *cpReconciler {
	return &cpReconciler{
		client:            c,
		clusters:          clusters,
		roleTracker:       roleTracker,
		eventsBatchPeriod: eventsBatchPeriod,
	}
}

func (r *cpReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	cp := &kueuealpha.CapacityProvider{}
	if err := r.client.Get(ctx, req.NamespacedName, cp); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if cp.Spec.ControllerName != kueue.MultiKueueControllerName {
		return reconcile.Result{}, nil
	}
	log := ctrl.LoggerFrom(ctx)
	log.V(2).Info("Reconciling MultiKueue CapacityProvider")

	cpConfig, mkConfig, err := r.loadConfigs(ctx, cp)
	if err != nil {
		if errors.Is(err, errCapacityProviderMisconfigured) {
			return reconcile.Result{}, r.updateStatus(ctx, cp, nil, metav1.ConditionFalse, kueuealpha.CapacityProviderReasonMisconfigured, err.Error())
		}
		return reconcile.Result{}, err
	}

	lqKeys, err := r.relatedManagerLocalQueues(ctx, cpConfig)
	if err != nil {
		if errors.Is(err, errCapacityProviderMisconfigured) {
			return reconcile.Result{}, r.updateStatus(ctx, cp, nil, metav1.ConditionFalse, kueuealpha.CapacityProviderReasonMisconfigured, err.Error())
		}
		return reconcile.Result{}, err
	}

	total, disconnected, err := r.aggregateWorkerCapacity(ctx, mkConfig, lqKeys)
	if err != nil {
		return reconcile.Result{}, err
	}
	if len(disconnected) == len(mkConfig.Spec.Clusters) {
		// Keep the last published capacity, so that DQO retains the last effective quotas.
		return reconcile.Result{}, r.updateStatus(ctx, cp, cp.Status.Capacity, metav1.ConditionFalse, kueuealpha.CapacityProviderReasonSourceUnavailable,
			fmt.Sprintf("None of the worker clusters in MultiKueueConfig %q is connected", mkConfig.Name))
	}

	capacity := &kueuealpha.CapacityProviderNormalizedCapacity{
		Flavors: []kueuealpha.CapacityProviderNormalizedCapacityFlavor{{
			Name:      cp.Spec.OrchestratedFlavors[0].Name,
			Resources: total,
		}},
	}
	message := fmt.Sprintf("Aggregated capacity from %d of %d worker clusters", len(mkConfig.Spec.Clusters)-len(disconnected), len(mkConfig.Spec.Clusters))
	if len(disconnected) > 0 {
		message = fmt.Sprintf("%s; disconnected: %s", message, strings.Join(disconnected, ", "))
	}
	return reconcile.Result{}, r.updateStatus(ctx, cp, capacity, metav1.ConditionTrue, kueuealpha.CapacityProviderReasonSynchronized, message)
}

func (r *cpReconciler) loadConfigs(ctx context.Context, cp *kueuealpha.CapacityProvider) (*kueuealpha.MultiKueueCapacityProviderConfig, *kueue.MultiKueueConfig, error) {
	if len(cp.Spec.OrchestratedFlavors) != 1 {
		return nil, nil, fmt.Errorf("%w: spec.orchestratedFlavors must contain exactly one flavor", errCapacityProviderMisconfigured)
	}
	params := cp.Spec.Parameters
	if params == nil || params.APIGroup != kueuealpha.SchemeGroupVersion.Group || params.Kind != multiKueueCapacityProviderConfigKind {
		return nil, nil, fmt.Errorf("%w: spec.parameters must reference a %s.%s", errCapacityProviderMisconfigured, multiKueueCapacityProviderConfigKind, kueuealpha.SchemeGroupVersion.Group)
	}

	cpConfig := &kueuealpha.MultiKueueCapacityProviderConfig{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: params.Name}, cpConfig); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, fmt.Errorf("%w: %s %q not found", errCapacityProviderMisconfigured, multiKueueCapacityProviderConfigKind, params.Name)
		}
		return nil, nil, err
	}

	mkConfig := &kueue.MultiKueueConfig{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: cpConfig.Spec.MultiKueueConfigName}, mkConfig); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, fmt.Errorf("%w: MultiKueueConfig %q not found", errCapacityProviderMisconfigured, cpConfig.Spec.MultiKueueConfigName)
		}
		return nil, nil, err
	}
	return cpConfig, mkConfig, nil
}

// relatedManagerLocalQueues returns the (namespace, name) keys of the manager LocalQueues
// that point to ClusterQueues in managerQuotaScope which use a MultiKueue AdmissionCheck
// backed by multiKueueConfigName. MultiKueue dispatches by LocalQueue (namespace, name),
// so these keys identify the related worker ClusterQueues.
func (r *cpReconciler) relatedManagerLocalQueues(ctx context.Context, cpConfig *kueuealpha.MultiKueueCapacityProviderConfig) (sets.Set[types.NamespacedName], error) {
	cqs, err := r.managerClusterQueuesInScope(ctx, cpConfig.Spec.ManagerQuotaScope)
	if err != nil {
		return nil, err
	}

	acList := &kueue.AdmissionCheckList{}
	if err := r.client.List(ctx, acList, client.MatchingFields{AdmissionCheckUsingConfigKey: cpConfig.Spec.MultiKueueConfigName}); err != nil {
		return nil, fmt.Errorf("listing AdmissionChecks: %w", err)
	}
	acNames := sets.New[kueue.AdmissionCheckReference]()
	for _, ac := range acList.Items {
		acNames.Insert(kueue.AdmissionCheckReference(ac.Name))
	}

	lqKeys := sets.New[types.NamespacedName]()
	for _, cq := range cqs {
		if cq.Spec.AdmissionChecksStrategy == nil || !slices.ContainsFunc(cq.Spec.AdmissionChecksStrategy.AdmissionChecks, func(rule kueue.AdmissionCheckStrategyRule) bool {
			return acNames.Has(rule.Name)
		}) {
			continue
		}
		lqList := &kueue.LocalQueueList{}
		if err := r.client.List(ctx, lqList, client.MatchingFields{indexer.QueueClusterQueueKey: cq.Name}); err != nil {
			return nil, fmt.Errorf("listing LocalQueues: %w", err)
		}
		for _, lq := range lqList.Items {
			lqKeys.Insert(types.NamespacedName{Namespace: lq.Namespace, Name: lq.Name})
		}
	}
	return lqKeys, nil
}

func (r *cpReconciler) managerClusterQueuesInScope(ctx context.Context, scope kueuealpha.CapacityDistributionSubtreeRootRef) ([]kueue.ClusterQueue, error) {
	switch scope.Kind {
	case kueuealpha.ClusterQueueSubtreeRootRefKind:
		cq := kueue.ClusterQueue{}
		if err := r.client.Get(ctx, types.NamespacedName{Name: scope.Name}, &cq); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("%w: managerQuotaScope ClusterQueue %q not found", errCapacityProviderMisconfigured, scope.Name)
			}
			return nil, err
		}
		return []kueue.ClusterQueue{cq}, nil
	case kueuealpha.CohortSubtreeRootRefKind:
		var cqs []kueue.ClusterQueue
		pending := []string{scope.Name}
		visited := sets.New(scope.Name)
		for len(pending) > 0 {
			cohortName := pending[0]
			pending = pending[1:]

			cqList := &kueue.ClusterQueueList{}
			if err := r.client.List(ctx, cqList, client.MatchingFields{indexer.ClusterQueueCohortKey: cohortName}); err != nil {
				return nil, fmt.Errorf("listing ClusterQueues: %w", err)
			}
			cqs = append(cqs, cqList.Items...)

			cohortList := &kueue.CohortList{}
			if err := r.client.List(ctx, cohortList, client.MatchingFields{indexer.CohortParentKey: cohortName}); err != nil {
				return nil, fmt.Errorf("listing Cohorts: %w", err)
			}
			for _, child := range cohortList.Items {
				if !visited.Has(child.Name) {
					visited.Insert(child.Name)
					pending = append(pending, child.Name)
				}
			}
		}
		return cqs, nil
	default:
		return nil, fmt.Errorf("%w: unsupported managerQuotaScope kind %q", errCapacityProviderMisconfigured, scope.Kind)
	}
}

// aggregateWorkerCapacity sums the effective nominal quotas of the related worker ClusterQueues
// on the connected worker clusters, across all worker ResourceFlavors. A worker ClusterQueue
// managed by its own DQO contributes status.effectiveQuotas instead of spec.resourceGroups.
// It returns the names of the worker clusters that are not connected.
func (r *cpReconciler) aggregateWorkerCapacity(ctx context.Context, mkConfig *kueue.MultiKueueConfig, lqKeys sets.Set[types.NamespacedName]) (corev1.ResourceList, []string, error) {
	total := corev1.ResourceList{}
	var disconnected []string
	for _, clusterName := range mkConfig.Spec.Clusters {
		rc, found := r.clusters.controllerFor(clusterName)
		if !found || !rc.connState.isConnected() {
			disconnected = append(disconnected, clusterName)
			continue
		}
		remoteClient := rc.getClient()
		if remoteClient == nil {
			disconnected = append(disconnected, clusterName)
			continue
		}

		remoteLQList := &kueue.LocalQueueList{}
		// This List is cached (by the selectivelyCachingClient).
		if err := remoteClient.List(ctx, remoteLQList); err != nil {
			return nil, nil, fmt.Errorf("listing LocalQueues on worker cluster %q: %w", clusterName, err)
		}
		relatedCQNames := sets.New[string]()
		for _, rlq := range remoteLQList.Items {
			if lqKeys.Has(types.NamespacedName{Namespace: rlq.Namespace, Name: rlq.Name}) {
				relatedCQNames.Insert(string(rlq.Spec.ClusterQueue))
			}
		}

		remoteCQList := &kueue.ClusterQueueList{}
		// This List is cached (by the selectivelyCachingClient).
		if err := remoteClient.List(ctx, remoteCQList); err != nil {
			return nil, nil, fmt.Errorf("listing ClusterQueues on worker cluster %q: %w", clusterName, err)
		}
		for _, rcq := range remoteCQList.Items {
			if !relatedCQNames.Has(rcq.Name) {
				continue
			}
			resourceGroups := rcq.Spec.ResourceGroups
			if rcq.Status.EffectiveQuotas != nil {
				resourceGroups = rcq.Status.EffectiveQuotas.ResourceGroups
			}
			for _, rg := range resourceGroups {
				for _, flavor := range rg.Flavors {
					for _, res := range flavor.Resources {
						sum := total[res.Name]
						sum.Add(res.NominalQuota)
						total[res.Name] = sum
					}
				}
			}
		}
	}
	return total, disconnected, nil
}

func (r *cpReconciler) updateStatus(ctx context.Context, cp *kueuealpha.CapacityProvider, capacity *kueuealpha.CapacityProviderNormalizedCapacity, status metav1.ConditionStatus, reason, message string) error {
	newStatus := cp.Status.DeepCopy()
	newStatus.Capacity = capacity
	apimeta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
		Type:               kueuealpha.CapacityProviderCapacitySynchronized,
		Status:             status,
		Reason:             reason,
		Message:            api.TruncateConditionMessage(message),
		ObservedGeneration: cp.Generation,
	})
	if equality.Semantic.DeepEqual(&cp.Status, newStatus) {
		return nil
	}
	cp.Status = *newStatus
	if err := r.client.Status().Update(ctx, cp); err != nil {
		return fmt.Errorf("updating CapacityProvider status: %w", err)
	}
	return nil
}

// enqueueAll queues every MultiKueue CapacityProvider. There are few of them, so
// mapping each event to the affected providers is not worth it for now.
func (r *cpReconciler) enqueueAll(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	cpList := &kueuealpha.CapacityProviderList{}
	if err := r.client.List(ctx, cpList); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "Failed to list CapacityProviders")
		return
	}
	for _, cp := range cpList.Items {
		if cp.Spec.ControllerName == kueue.MultiKueueControllerName {
			q.AddAfter(reconcile.Request{NamespacedName: types.NamespacedName{Name: cp.Name}}, r.eventsBatchPeriod)
		}
	}
}

func (r *cpReconciler) enqueueAllHandler() handler.EventHandler {
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, _ event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.enqueueAll(ctx, q)
		},
		UpdateFunc: func(ctx context.Context, _ event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.enqueueAll(ctx, q)
		},
		DeleteFunc: func(ctx context.Context, _ event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.enqueueAll(ctx, q)
		},
		GenericFunc: func(ctx context.Context, _ event.GenericEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.enqueueAll(ctx, q)
		},
	}
}

func (r *cpReconciler) setupWithManager(mgr ctrl.Manager) error {
	isMultiKueueProvider := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		cp, ok := obj.(*kueuealpha.CapacityProvider)
		return ok && cp.Spec.ControllerName == kueue.MultiKueueControllerName
	})
	activeConditionChanged := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldMKC, okOld := e.ObjectOld.(*kueue.MultiKueueCluster)
			newMKC, okNew := e.ObjectNew.(*kueue.MultiKueueCluster)
			if !okOld || !okNew {
				return true
			}
			return !isConditionEqual(
				apimeta.FindStatusCondition(oldMKC.Status.Conditions, kueue.MultiKueueClusterActive),
				apimeta.FindStatusCondition(newMKC.Status.Conditions, kueue.MultiKueueClusterActive),
			)
		},
	}
	remoteHandler := handler.TypedFuncs[kueue.ClusterQueueReference, reconcile.Request]{
		GenericFunc: func(ctx context.Context, _ event.TypedGenericEvent[kueue.ClusterQueueReference], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.enqueueAll(ctx, q)
		},
	}
	allHandler := r.enqueueAllHandler()
	generationChanged := builder.WithPredicates(predicate.GenerationChangedPredicate{})

	return ctrl.NewControllerManagedBy(mgr).
		Named("multikueue_capacityprovider").
		For(&kueuealpha.CapacityProvider{}, builder.WithPredicates(isMultiKueueProvider, predicate.GenerationChangedPredicate{})).
		Watches(&kueuealpha.MultiKueueCapacityProviderConfig{}, allHandler, generationChanged).
		Watches(&kueue.MultiKueueConfig{}, allHandler, generationChanged).
		Watches(&kueue.MultiKueueCluster{}, allHandler, builder.WithPredicates(activeConditionChanged)).
		Watches(&kueue.AdmissionCheck{}, allHandler, generationChanged).
		Watches(&kueue.ClusterQueue{}, allHandler, generationChanged).
		Watches(&kueue.Cohort{}, allHandler, generationChanged).
		Watches(&kueue.LocalQueue{}, allHandler, generationChanged).
		WatchesRawSource(source.Channel(r.clusters.cpUpdateCh, remoteHandler)).
		WithOptions(controller.Options{
			LogConstructor: roletracker.NewLogConstructor(r.roleTracker, "multikueue-capacityprovider-reconciler"),
		}).
		Complete(r)
}
