package machine

import (
	"context"
	"fmt"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/hypershift/control-plane-operator/hostedclusterconfigoperator/operator"
	"github.com/openshift/hypershift/support/upsert"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	capiv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	tenantServiceNameLabelKey      = "cluster.x-k8s.io/tenant-service-name"
	tenantServiceNamespaceLabelKey = "cluster.x-k8s.io/tenant-service-namespace"
	clusterNameLabelKey            = "cluster.x-k8s.io/cluster-name"
)

type reconciler struct {
	client              client.Client
	kubevirtInfraClient client.Client
	// guestClient reads tenant Services and EndpointSlices. Those objects are
	// not labeled for the HCCO cache, so this client must not use that cache.
	guestClient client.Client
	hcpKey      client.ObjectKey
	upsert.CreateOrUpdateProvider
}

func Setup(ctx context.Context, opts *operator.HostedClusterConfigOperatorConfig) error {
	// This controller is just needed at kubevirt to manage passthrough
	// endpointslices
	if opts.PlatformType != hyperv1.KubevirtPlatform {
		return nil
	}

	log := ctrl.LoggerFrom(ctx)
	log.Info("Setup")
	kubevirtScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(kubevirtScheme)
	_ = kubevirtv1.AddToScheme(kubevirtScheme)
	_ = discoveryv1.AddToScheme(kubevirtScheme)

	kubevirtHttpClient, err := rest.HTTPClientFor(opts.KubevirtInfraConfig)
	if err != nil {
		return fmt.Errorf("failed creating kubevirt cluster http client: %w", err)
	}

	kubevirtMapper, err := apiutil.NewDynamicRESTMapper(opts.KubevirtInfraConfig, kubevirtHttpClient)
	if err != nil {
		return fmt.Errorf("failed creating kubevirt cluster rest mapper: %w", err)
	}

	hcpKey := client.ObjectKey{
		Namespace: opts.Namespace,
		Name:      opts.HCPName,
	}
	hcp := &hyperv1.HostedControlPlane{}
	// Use uncached client so we don't have to start the cache before read
	// hcp, this is done only at hosted cluster startup so we are not going
	// to hit api server too much.
	if err := opts.CPCluster.GetAPIReader().Get(ctx, hcpKey, hcp); err != nil {
		return fmt.Errorf("failed to get HostedControlPlane: %w", err)
	}

	kubevirtInfraCache, err := cache.New(opts.KubevirtInfraConfig, cache.Options{
		Scheme: kubevirtScheme,
		Mapper: kubevirtMapper,
		DefaultNamespaces: map[string]cache.Config{
			kubevirtInfraNamespace(hcp): cache.Config{}},
	})
	if err != nil {
		return fmt.Errorf("failed building kubevirt infra cache: %w", err)
	}
	// if kubevirt infra config is not used, it is being set the same as the mgmt config
	kubevirtInfraClientRestConfig := opts.KubevirtInfraConfig
	kubevirtInfraClientRestConfig.WarningHandler = rest.NoWarnings{}
	kubevirtInfraClient, err := client.New(kubevirtInfraClientRestConfig, client.Options{
		Scheme: kubevirtScheme,
		Mapper: kubevirtMapper,
		Cache:  &client.CacheOptions{Reader: kubevirtInfraCache},
	})
	if err != nil {
		return fmt.Errorf("failed to create kubevirt infra uncached client: %w", err)
	}
	if opts.TargetConfig == nil {
		return fmt.Errorf("kubevirt machine controller requires a guest cluster config")
	}
	guestScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(guestScheme)
	_ = discoveryv1.AddToScheme(guestScheme)
	guestCache, err := cache.New(opts.TargetConfig, cache.Options{Scheme: guestScheme})
	if err != nil {
		return fmt.Errorf("failed building guest cluster cache: %w", err)
	}
	guestClient, err := client.New(opts.TargetConfig, client.Options{
		Scheme: guestScheme,
		Cache:  &client.CacheOptions{Reader: guestCache},
	})
	if err != nil {
		return fmt.Errorf("failed to create guest cluster client: %w", err)
	}

	r := &reconciler{
		client:                 opts.CPCluster.GetClient(),
		kubevirtInfraClient:    kubevirtInfraClient,
		guestClient:            guestClient,
		hcpKey:                 hcpKey,
		CreateOrUpdateProvider: opts.TargetCreateOrUpdateProvider,
	}
	c, err := controller.New("machine", opts.Manager, controller.Options{Reconciler: r})
	if err != nil {
		return fmt.Errorf("failed to construct controller: %w", err)
	}

	if err := c.Watch(source.Kind[client.Object](opts.CPCluster.GetCache(), &capiv1.Machine{}, &handler.EnqueueRequestForObject{})); err != nil {
		return fmt.Errorf("failed to watch Machines: %w", err)
	}
	go func() {
		_ = kubevirtInfraCache.Start(ctx)
	}()
	allNodes := func(watchContext context.Context, obj client.Object) []reconcile.Request {
		machineList := &capiv1.MachineList{}
		if err := r.client.List(watchContext, machineList); err != nil {
			log.Error(err, "failed listing machines at kubevirt service watch function")
			return nil
		}
		requests := []reconcile.Request{}
		for _, machine := range machineList.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: machine.Namespace,
					Name:      machine.Name,
				},
			})
		}
		return requests
	}
	isKCCMService := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		serviceLabels := obj.GetLabels()
		if len(serviceLabels) == 0 {
			return false
		}
		_, ok := serviceLabels[tenantServiceNameLabelKey]
		if !ok {
			return false
		}
		obtainedClusterName, ok := serviceLabels[clusterNameLabelKey]
		if !ok || obtainedClusterName != hcp.Labels[clusterNameLabelKey] {
			return false
		}
		return true
	})
	if err := c.Watch(source.Kind[client.Object](kubevirtInfraCache, &corev1.Service{}, handler.EnqueueRequestsFromMapFunc(allNodes), isKCCMService)); err != nil {
		return fmt.Errorf("failed to watch kubevirt services: %w", err)
	}
	// A live migration changes the node that must receive Local traffic.
	if err := c.Watch(source.Kind[client.Object](kubevirtInfraCache, &kubevirtv1.VirtualMachineInstance{}, handler.EnqueueRequestsFromMapFunc(allNodes))); err != nil {
		return fmt.Errorf("failed to watch kubevirt virtual machine instances: %w", err)
	}

	go func() {
		_ = guestCache.Start(ctx)
	}()
	isGuestLoadBalancer := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		service, ok := obj.(*corev1.Service)
		return ok && service.Spec.Type == corev1.ServiceTypeLoadBalancer
	})
	if err := c.Watch(source.Kind[client.Object](guestCache, &corev1.Service{}, handler.EnqueueRequestsFromMapFunc(allNodes), isGuestLoadBalancer)); err != nil {
		return fmt.Errorf("failed to watch guest load balancer services: %w", err)
	}
	enqueueLocalTenantEndpoints := func(watchContext context.Context, obj client.Object) []reconcile.Request {
		slice, ok := obj.(*discoveryv1.EndpointSlice)
		if !ok {
			return nil
		}
		serviceName := slice.Labels[discoveryv1.LabelServiceName]
		if serviceName == "" {
			return nil
		}
		tenantService := &corev1.Service{}
		if err := guestClient.Get(watchContext, client.ObjectKey{Namespace: slice.Namespace, Name: serviceName}, tenantService); err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error(err, "failed to get guest service for endpoint slice", "endpointSlice", slice.Name)
			}
			return nil
		}
		if tenantService.Spec.Type != corev1.ServiceTypeLoadBalancer || tenantService.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal {
			return nil
		}
		return allNodes(watchContext, obj)
	}
	if err := c.Watch(source.Kind[client.Object](guestCache, &discoveryv1.EndpointSlice{}, handler.EnqueueRequestsFromMapFunc(enqueueLocalTenantEndpoints))); err != nil {
		return fmt.Errorf("failed to watch guest endpoint slices: %w", err)
	}

	return nil
}
