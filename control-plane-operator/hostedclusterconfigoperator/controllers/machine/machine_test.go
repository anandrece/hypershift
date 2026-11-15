package machine

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/hypershift/client/clientset/clientset/scheme"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	capiv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/onsi/gomega/format"
	"gopkg.in/yaml.v2"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

func TestReconcileDefaultIngressEndpoints(t *testing.T) {
	format.MaxLength = 0

	machineTypeMeta := metav1.TypeMeta{
		Kind:       "Machine",
		APIVersion: capiv1.GroupVersion.String(),
	}
	vmTypeMeta := metav1.TypeMeta{
		Kind:       "VirtualMachine",
		APIVersion: kubevirtv1.GroupVersion.String(),
	}
	newObjectMeta := func(namespace, name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       types.UID(name + "-uid"),
		}
	}
	kubevirtHCP := &hyperv1.HostedControlPlane{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "cluster1",
			Labels: map[string]string{
				clusterNameLabelKey: "cluster1",
			},
		},
		Spec: hyperv1.HostedControlPlaneSpec{
			Platform: hyperv1.PlatformSpec{
				Type: hyperv1.KubevirtPlatform,
				Kubevirt: &hyperv1.KubevirtPlatformSpec{
					GenerateID: "foobar",
				},
			},
			Capabilities: &hyperv1.Capabilities{
				Enabled: []hyperv1.OptionalCapability{
					hyperv1.IngressCapability,
				},
			},
		},
	}

	worker1Meta := newObjectMeta("ns1", "worker1")
	vmWorker1Meta := newObjectMeta("ns1", "vm-worker1")
	worker2Meta := newObjectMeta("ns1", "worker2")
	vmWorker2Meta := newObjectMeta("ns1", "vm-worker2")

	addressesByMachine := map[string][]string{
		worker1Meta.Name: {"192.168.1.3", "2001:db8:a0b:12f0::3"},
		worker2Meta.Name: {"192.168.1.4", "2001:db8:a0b:12f0::4"},
	}

	protocolTCP := corev1.ProtocolTCP
	pairOfVirtualMachines := []kubevirtv1.VirtualMachine{
		{
			TypeMeta:   vmTypeMeta,
			ObjectMeta: vmWorker1Meta,
		},
		{
			TypeMeta:   vmTypeMeta,
			ObjectMeta: vmWorker2Meta,
		},
	}
	pairOfDualStackMachines := func(phaseWorker1, phaseWorker2 capiv1.MachinePhase) []capiv1.Machine {
		return []capiv1.Machine{
			{
				TypeMeta:   machineTypeMeta,
				ObjectMeta: worker1Meta,
				Spec: capiv1.MachineSpec{
					InfrastructureRef: capiv1.ContractVersionedObjectReference{
						Name: vmWorker1Meta.Name,
					},
				},
				Status: capiv1.MachineStatus{
					Phase: string(phaseWorker1),
					Addresses: []capiv1.MachineAddress{
						{
							Type:    capiv1.MachineInternalIP,
							Address: addressesByMachine[worker1Meta.Name][0],
						},
						{
							Type:    capiv1.MachineInternalIP,
							Address: addressesByMachine[worker1Meta.Name][1],
						},
					},
				},
			},
			{
				TypeMeta:   machineTypeMeta,
				ObjectMeta: worker2Meta,
				Spec: capiv1.MachineSpec{
					InfrastructureRef: capiv1.ContractVersionedObjectReference{
						Name: vmWorker2Meta.Name,
					},
				},
				Status: capiv1.MachineStatus{
					Phase: string(phaseWorker2),
					Addresses: []capiv1.MachineAddress{
						{
							Type:    capiv1.MachineInternalIP,
							Address: addressesByMachine[worker2Meta.Name][0],
						},
						{
							Type:    capiv1.MachineInternalIP,
							Address: addressesByMachine[worker2Meta.Name][1],
						},
					},
				},
			},
		}
	}

	pairOfDualStackRunningMachines := pairOfDualStackMachines(capiv1.MachinePhaseRunning, capiv1.MachinePhaseRunning)

	defaultIngressService := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "default-ingress-passthrough-service-foobar",
			UID:       types.UID("test-svc-1-uid"),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				TargetPort: intstr.FromInt(2222),
				Protocol:   corev1.ProtocolTCP,
				Name:       "port1",
			}},
			IPFamilies: []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol},
		},
	}
	normalService := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "normal-service",
			UID:       types.UID("test-svc-2-uid"),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				TargetPort: intstr.FromInt(3333),
				Protocol:   corev1.ProtocolTCP,
				Name:       "port1",
			}},
			IPFamilies: []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol},
		},
	}
	kccmService := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "kccm-service-cluster1",
			UID:       types.UID("test-svc-3-uid"),
			Labels: map[string]string{
				tenantServiceNameLabelKey: "svc-3",
				clusterNameLabelKey:       "cluster1",
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				TargetPort: intstr.FromInt(4444),
				Protocol:   corev1.ProtocolTCP,
				Name:       "port1",
			}},
			IPFamilies: []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol},
		},
	}
	kccmServiceDifferentCluster := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "kccm-service-cluster2",
			UID:       types.UID("test-svc-3-uid"),
			Labels: map[string]string{
				tenantServiceNameLabelKey: "svc-3",
				clusterNameLabelKey:       "cluster2",
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				TargetPort: intstr.FromInt(4444),
				Protocol:   corev1.ProtocolTCP,
				Name:       "port1",
			}},
			IPFamilies: []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol},
		},
	}

	readyAndServing := func(ready, serving bool) func(eps discoveryv1.EndpointSlice) discoveryv1.EndpointSlice {
		return func(eps discoveryv1.EndpointSlice) discoveryv1.EndpointSlice {
			eps.Endpoints[0].Conditions.Ready = &ready
			eps.Endpoints[0].Conditions.Serving = &serving
			return eps
		}
	}

	defaultIngressEndpointSliceIPv4 := func(machine capiv1.Machine, vm kubevirtv1.VirtualMachine, endpointSliceTransform ...func(discoveryv1.EndpointSlice) discoveryv1.EndpointSlice) discoveryv1.EndpointSlice {
		endpointSlice := discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "default-ingress-passthrough-service-foobar-" + machine.Name + "-ipv4",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: "default-ingress-passthrough-service-foobar",
					discoveryv1.LabelManagedBy:   "control-plane-operator.hypershift.openshift.io",
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         vmTypeMeta.APIVersion,
					Kind:               vmTypeMeta.Kind,
					UID:                vm.UID,
					Name:               vm.Name,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				}},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses: []string{addressesByMachine[machine.Name][0]},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
			}},
			Ports: []discoveryv1.EndpointPort{{
				Port:     ptr.To[int32](2222),
				Protocol: &protocolTCP,
				Name:     ptr.To("port1"),
			}},
		}
		for _, t := range endpointSliceTransform {
			endpointSlice = t(endpointSlice)
		}
		return endpointSlice
	}

	defaultIngressEndpointSliceIPv6 := func(machine capiv1.Machine, vm kubevirtv1.VirtualMachine, endpointSliceTransform ...func(discoveryv1.EndpointSlice) discoveryv1.EndpointSlice) discoveryv1.EndpointSlice {
		endpointSlice := discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "default-ingress-passthrough-service-foobar-" + machine.Name + "-ipv6",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: "default-ingress-passthrough-service-foobar",
					discoveryv1.LabelManagedBy:   "control-plane-operator.hypershift.openshift.io",
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         vmTypeMeta.APIVersion,
					Kind:               vmTypeMeta.Kind,
					UID:                vm.UID,
					Name:               vm.Name,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				}},
			},
			AddressType: discoveryv1.AddressTypeIPv6,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses: []string{addressesByMachine[machine.Name][1]},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
			}},
			Ports: []discoveryv1.EndpointPort{{
				Port:     ptr.To[int32](2222),
				Protocol: &protocolTCP,
				Name:     ptr.To("port1"),
			}},
		}

		for _, t := range endpointSliceTransform {
			endpointSlice = t(endpointSlice)
		}
		return endpointSlice
	}

	kccmEndpointSliceIPv4WithNamespace := func(namespace string, machine capiv1.Machine, vm kubevirtv1.VirtualMachine) discoveryv1.EndpointSlice {
		return discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "kccm-service-cluster1-" + machine.Name + "-ipv4",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: "kccm-service-cluster1",
					discoveryv1.LabelManagedBy:   "control-plane-operator.hypershift.openshift.io",
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         vmTypeMeta.APIVersion,
					Kind:               vmTypeMeta.Kind,
					UID:                vm.UID,
					Name:               vm.Name,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				}},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses: []string{addressesByMachine[machine.Name][0]},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
			}},
			Ports: []discoveryv1.EndpointPort{{
				Port:     ptr.To[int32](4444),
				Protocol: &protocolTCP,
				Name:     ptr.To("port1"),
			}},
		}
	}

	kccmEndpointSliceIPv4 := func(machine capiv1.Machine, vm kubevirtv1.VirtualMachine) discoveryv1.EndpointSlice {
		return kccmEndpointSliceIPv4WithNamespace("ns1", machine, vm)
	}

	kccmTrafficPolicyEndpointSlice := func(machine capiv1.Machine, vm kubevirtv1.VirtualMachine, ipFamily corev1.IPFamily, address, nodeName string) discoveryv1.EndpointSlice {
		family := "ipv4"
		addressType := discoveryv1.AddressTypeIPv4
		if ipFamily == corev1.IPv6Protocol {
			family = "ipv6"
			addressType = discoveryv1.AddressTypeIPv6
		}
		endpoints := []discoveryv1.Endpoint{}
		if address != "" {
			endpoints = []discoveryv1.Endpoint{{
				Addresses: []string{address},
				NodeName:  ptr.To(nodeName),
				Conditions: discoveryv1.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
			}}
		}
		return discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "kccm-service-cluster1-" + machine.Name + "-" + family,
				Labels: map[string]string{
					discoveryv1.LabelServiceName: "kccm-service-cluster1",
					discoveryv1.LabelManagedBy:   "control-plane-operator.hypershift.openshift.io",
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         vmTypeMeta.APIVersion,
					Kind:               vmTypeMeta.Kind,
					UID:                vm.UID,
					Name:               vm.Name,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				}},
			},
			AddressType: addressType,
			Endpoints:   endpoints,
			Ports: []discoveryv1.EndpointPort{{
				Port:     ptr.To[int32](4444),
				Protocol: &protocolTCP,
				Name:     ptr.To("port1"),
			}},
		}
	}

	kccmEndpointSliceIPv6 := func(machine capiv1.Machine, vm kubevirtv1.VirtualMachine) discoveryv1.EndpointSlice {
		return discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "kccm-service-cluster1-" + machine.Name + "-ipv6",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: "kccm-service-cluster1",
					discoveryv1.LabelManagedBy:   "control-plane-operator.hypershift.openshift.io",
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         vmTypeMeta.APIVersion,
					Kind:               vmTypeMeta.Kind,
					UID:                vm.UID,
					Name:               vm.Name,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				}},
			},
			AddressType: discoveryv1.AddressTypeIPv6,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses: []string{addressesByMachine[machine.Name][1]},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
			}},
			Ports: []discoveryv1.EndpointPort{{
				Port:     ptr.To[int32](4444),
				Protocol: &protocolTCP,
				Name:     ptr.To("port1"),
			}},
		}
	}

	withSelector := func(service corev1.Service) corev1.Service {
		service.Spec.Selector = map[string]string{"key1": "value1"}
		return service
	}

	_ = capiv1.AddToScheme(scheme.Scheme)
	_ = hyperv1.AddToScheme(scheme.Scheme)
	_ = kubevirtv1.AddToScheme(scheme.Scheme)
	_ = discoveryv1.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
	machinesWithLinkLocalAddresses := []capiv1.Machine{
		{
			TypeMeta:   machineTypeMeta,
			ObjectMeta: worker1Meta,
			Spec: capiv1.MachineSpec{
				InfrastructureRef: capiv1.ContractVersionedObjectReference{
					Name: vmWorker1Meta.Name,
				},
			},
			Status: capiv1.MachineStatus{
				Phase: string(capiv1.MachinePhaseRunning),
				Addresses: []capiv1.MachineAddress{
					{Type: capiv1.MachineInternalIP, Address: "192.168.1.3"},
					{Type: capiv1.MachineInternalIP, Address: "169.254.0.2"},
					{Type: capiv1.MachineInternalIP, Address: "2001:db8:a0b:12f0::3"},
					{Type: capiv1.MachineInternalIP, Address: "fe80::1"},
				},
			},
		},
		{
			TypeMeta:   machineTypeMeta,
			ObjectMeta: worker2Meta,
			Spec: capiv1.MachineSpec{
				InfrastructureRef: capiv1.ContractVersionedObjectReference{
					Name: vmWorker2Meta.Name,
				},
			},
			Status: capiv1.MachineStatus{
				Phase: string(capiv1.MachinePhaseRunning),
				Addresses: []capiv1.MachineAddress{
					{Type: capiv1.MachineInternalIP, Address: "192.168.1.4"},
					{Type: capiv1.MachineInternalIP, Address: "169.254.169.254"},
					{Type: capiv1.MachineInternalIP, Address: "2001:db8:a0b:12f0::4"},
					{Type: capiv1.MachineInternalIP, Address: "fe80::dead:beef"},
				},
			},
		},
	}

	machinesWithExtraInternalIPs := []capiv1.Machine{
		{
			TypeMeta:   machineTypeMeta,
			ObjectMeta: worker1Meta,
			Spec: capiv1.MachineSpec{
				InfrastructureRef: capiv1.ContractVersionedObjectReference{
					Name: vmWorker1Meta.Name,
				},
			},
			Status: capiv1.MachineStatus{
				Phase: string(capiv1.MachinePhaseRunning),
				Addresses: []capiv1.MachineAddress{
					{Type: capiv1.MachineInternalIP, Address: "192.168.1.3"},
					{Type: capiv1.MachineInternalIP, Address: "10.8.20.2"},
					{Type: capiv1.MachineInternalIP, Address: "2001:db8:a0b:12f0::3"},
					{Type: capiv1.MachineInternalIP, Address: "fd00:10:8::3"},
				},
			},
		},
		{
			TypeMeta:   machineTypeMeta,
			ObjectMeta: worker2Meta,
			Spec: capiv1.MachineSpec{
				InfrastructureRef: capiv1.ContractVersionedObjectReference{
					Name: vmWorker2Meta.Name,
				},
			},
			Status: capiv1.MachineStatus{
				Phase: string(capiv1.MachinePhaseRunning),
				Addresses: []capiv1.MachineAddress{
					{Type: capiv1.MachineInternalIP, Address: "192.168.1.4"},
					{Type: capiv1.MachineInternalIP, Address: "10.8.0.2"},
					{Type: capiv1.MachineInternalIP, Address: "2001:db8:a0b:12f0::4"},
					{Type: capiv1.MachineInternalIP, Address: "fd00:10:8::4"},
				},
			},
		},
	}

	localTrafficMachines := pairOfDualStackMachines(capiv1.MachinePhaseRunning, capiv1.MachinePhaseRunning)
	localTrafficMachines[0].Status.NodeRef = capiv1.MachineNodeReference{Name: "guest-worker-1"}
	localTrafficMachines[1].Status.NodeRef = capiv1.MachineNodeReference{Name: "guest-worker-2"}
	localTrafficVMIs := []kubevirtv1.VirtualMachineInstance{
		{
			ObjectMeta: vmWorker1Meta,
			Status:     kubevirtv1.VirtualMachineInstanceStatus{NodeName: "infra-worker-1"},
		},
		{
			ObjectMeta: vmWorker2Meta,
			Status:     kubevirtv1.VirtualMachineInstanceStatus{NodeName: "infra-worker-2"},
		},
	}
	tenantMirrorService := func(policy corev1.ServiceExternalTrafficPolicy) corev1.Service {
		service := kccmService
		service.Labels = map[string]string{
			tenantServiceNameLabelKey:      "svc-3",
			tenantServiceNamespaceLabelKey: "app",
			clusterNameLabelKey:            "cluster1",
		}
		service.Spec.ExternalTrafficPolicy = policy
		return service
	}
	tenantGuestService := func(policy corev1.ServiceExternalTrafficPolicy) *corev1.Service {
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "svc-3"},
			Spec: corev1.ServiceSpec{
				Type:                  corev1.ServiceTypeLoadBalancer,
				ExternalTrafficPolicy: policy,
			},
		}
	}
	localGuestEndpointSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "svc-3-ipv4",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc-3"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.132.0.20"},
			NodeName:  ptr.To("guest-worker-1"),
			Conditions: discoveryv1.EndpointConditions{
				Ready:   ptr.To(true),
				Serving: ptr.To(true),
			},
		}},
	}

	testCases := []struct {
		name                          string
		machines                      []capiv1.Machine
		virtualMachines               []kubevirtv1.VirtualMachine
		virtualMachineInstances       []kubevirtv1.VirtualMachineInstance
		services                      []corev1.Service
		endpointSlices                []discoveryv1.EndpointSlice
		guestObjects                  []client.Object
		hcp                           *hyperv1.HostedControlPlane
		expectedIngressEndpointSlices []discoveryv1.EndpointSlice
		expectedServices              []corev1.Service
		error                         bool
	}{
		{
			name:     "Without passthrow services",
			hcp:      kubevirtHCP,
			machines: pairOfDualStackRunningMachines,
		},
		{
			name:             "With selector at passthrow service",
			services:         []corev1.Service{withSelector(defaultIngressService)},
			expectedServices: []corev1.Service{withSelector(defaultIngressService)},
			hcp:              kubevirtHCP,
			machines:         pairOfDualStackRunningMachines,
		},
		{
			name:             "With Running machines with internal addresses and passthrow services should create ready/serving enpodintslices",
			machines:         pairOfDualStackRunningMachines,
			virtualMachines:  pairOfVirtualMachines,
			services:         []corev1.Service{defaultIngressService, normalService, kccmService, kccmServiceDifferentCluster},
			expectedServices: []corev1.Service{defaultIngressService, kccmService, normalService, kccmServiceDifferentCluster},
			expectedIngressEndpointSlices: []discoveryv1.EndpointSlice{
				defaultIngressEndpointSliceIPv4(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				defaultIngressEndpointSliceIPv4(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
				defaultIngressEndpointSliceIPv6(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				defaultIngressEndpointSliceIPv6(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv4(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv4(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv6(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv6(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
			},
			hcp: kubevirtHCP,
		},
		{
			name:             "With Failing machine with internal addresses and passthrow service should mark endpointslices as not ready/not serving",
			machines:         pairOfDualStackMachines(capiv1.MachinePhaseRunning, capiv1.MachinePhaseDeleting),
			virtualMachines:  pairOfVirtualMachines,
			services:         []corev1.Service{defaultIngressService},
			expectedServices: []corev1.Service{defaultIngressService},
			expectedIngressEndpointSlices: []discoveryv1.EndpointSlice{
				defaultIngressEndpointSliceIPv4(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0], readyAndServing(true, true)),
				defaultIngressEndpointSliceIPv4(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1], readyAndServing(false, false)),
				defaultIngressEndpointSliceIPv6(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0], readyAndServing(true, true)),
				defaultIngressEndpointSliceIPv6(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1], readyAndServing(false, false)),
			},
			hcp: kubevirtHCP,
		},
		{
			name:             "When machines have link-local addresses it should filter them from EndpointSlice endpoints",
			machines:         machinesWithLinkLocalAddresses,
			virtualMachines:  pairOfVirtualMachines,
			services:         []corev1.Service{defaultIngressService, kccmService},
			expectedServices: []corev1.Service{defaultIngressService, kccmService},
			expectedIngressEndpointSlices: []discoveryv1.EndpointSlice{
				defaultIngressEndpointSliceIPv4(machinesWithLinkLocalAddresses[0], pairOfVirtualMachines[0]),
				defaultIngressEndpointSliceIPv4(machinesWithLinkLocalAddresses[1], pairOfVirtualMachines[1]),
				defaultIngressEndpointSliceIPv6(machinesWithLinkLocalAddresses[0], pairOfVirtualMachines[0]),
				defaultIngressEndpointSliceIPv6(machinesWithLinkLocalAddresses[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv4(machinesWithLinkLocalAddresses[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv4(machinesWithLinkLocalAddresses[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv6(machinesWithLinkLocalAddresses[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv6(machinesWithLinkLocalAddresses[1], pairOfVirtualMachines[1]),
			},
			hcp: kubevirtHCP,
		},
		{
			name:             "When machines have multiple same-family InternalIPs it should use only the first per family",
			machines:         machinesWithExtraInternalIPs,
			virtualMachines:  pairOfVirtualMachines,
			services:         []corev1.Service{defaultIngressService, kccmService},
			expectedServices: []corev1.Service{defaultIngressService, kccmService},
			expectedIngressEndpointSlices: []discoveryv1.EndpointSlice{
				defaultIngressEndpointSliceIPv4(machinesWithExtraInternalIPs[0], pairOfVirtualMachines[0]),
				defaultIngressEndpointSliceIPv4(machinesWithExtraInternalIPs[1], pairOfVirtualMachines[1]),
				defaultIngressEndpointSliceIPv6(machinesWithExtraInternalIPs[0], pairOfVirtualMachines[0]),
				defaultIngressEndpointSliceIPv6(machinesWithExtraInternalIPs[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv4(machinesWithExtraInternalIPs[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv4(machinesWithExtraInternalIPs[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv6(machinesWithExtraInternalIPs[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv6(machinesWithExtraInternalIPs[1], pairOfVirtualMachines[1]),
			},
			hcp: kubevirtHCP,
		},
		{
			name:            "Should remove orphan endpoint slices",
			machines:        pairOfDualStackRunningMachines,
			virtualMachines: pairOfVirtualMachines,
			endpointSlices: []discoveryv1.EndpointSlice{
				defaultIngressEndpointSliceIPv4(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				defaultIngressEndpointSliceIPv4(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
				defaultIngressEndpointSliceIPv6(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				defaultIngressEndpointSliceIPv6(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv4(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv4(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv6(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv6(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv4WithNamespace("ns2", pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
			},
			expectedIngressEndpointSlices: []discoveryv1.EndpointSlice{
				kccmEndpointSliceIPv4WithNamespace("ns2", pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
			},
			hcp: kubevirtHCP,
		},
		{
			name:                    "When a tenant LoadBalancer uses externalTrafficPolicy Local, it should publish Local and only the worker with a local endpoint",
			machines:                localTrafficMachines,
			virtualMachines:         pairOfVirtualMachines,
			virtualMachineInstances: localTrafficVMIs,
			services:                []corev1.Service{tenantMirrorService("")},
			guestObjects: []client.Object{
				tenantGuestService(corev1.ServiceExternalTrafficPolicyLocal),
				localGuestEndpointSlice,
			},
			expectedServices: []corev1.Service{tenantMirrorService(corev1.ServiceExternalTrafficPolicyLocal)},
			expectedIngressEndpointSlices: []discoveryv1.EndpointSlice{
				kccmTrafficPolicyEndpointSlice(localTrafficMachines[0], pairOfVirtualMachines[0], corev1.IPv4Protocol, addressesByMachine[worker1Meta.Name][0], "infra-worker-1"),
				kccmTrafficPolicyEndpointSlice(localTrafficMachines[0], pairOfVirtualMachines[0], corev1.IPv6Protocol, addressesByMachine[worker1Meta.Name][1], "infra-worker-1"),
				kccmTrafficPolicyEndpointSlice(localTrafficMachines[1], pairOfVirtualMachines[1], corev1.IPv4Protocol, "", ""),
				kccmTrafficPolicyEndpointSlice(localTrafficMachines[1], pairOfVirtualMachines[1], corev1.IPv6Protocol, "", ""),
			},
			hcp: kubevirtHCP,
		},
		{
			name:            "When a tenant LoadBalancer uses externalTrafficPolicy Cluster, it should publish Cluster and keep every worker endpoint",
			machines:        pairOfDualStackRunningMachines,
			virtualMachines: pairOfVirtualMachines,
			services:        []corev1.Service{tenantMirrorService(corev1.ServiceExternalTrafficPolicyLocal)},
			guestObjects: []client.Object{
				tenantGuestService(corev1.ServiceExternalTrafficPolicyCluster),
			},
			expectedServices: []corev1.Service{tenantMirrorService(corev1.ServiceExternalTrafficPolicyCluster)},
			expectedIngressEndpointSlices: []discoveryv1.EndpointSlice{
				kccmEndpointSliceIPv4(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv4(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
				kccmEndpointSliceIPv6(pairOfDualStackRunningMachines[0], pairOfVirtualMachines[0]),
				kccmEndpointSliceIPv6(pairOfDualStackRunningMachines[1], pairOfVirtualMachines[1]),
			},
			hcp: kubevirtHCP,
		},
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kubevirtInfraClusterObjects := []client.Object{}
			for _, vm := range tc.virtualMachines {
				virtualMachine := vm // golang bug referencing for loop vars
				kubevirtInfraClusterObjects = append(kubevirtInfraClusterObjects, &virtualMachine)
			}
			for _, vmi := range tc.virtualMachineInstances {
				instance := vmi
				kubevirtInfraClusterObjects = append(kubevirtInfraClusterObjects, &instance)
			}
			for _, svc := range tc.services {
				service := svc // golang bug referencing for loop vars
				kubevirtInfraClusterObjects = append(kubevirtInfraClusterObjects, &service)
			}
			for _, eps := range tc.endpointSlices {
				endpointSlice := eps // golang bug referencing for loop vars
				kubevirtInfraClusterObjects = append(kubevirtInfraClusterObjects, &endpointSlice)
			}
			mgmtClusterObjects := []client.Object{tc.hcp}
			for _, m := range tc.machines {
				machine := m
				mgmtClusterObjects = append(mgmtClusterObjects, &machine)
			}
			g := NewWithT(t)
			r := &reconciler{
				client:                 fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(mgmtClusterObjects...).Build(),
				kubevirtInfraClient:    fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(kubevirtInfraClusterObjects...).Build(),
				guestClient:            fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(tc.guestObjects...).Build(),
				CreateOrUpdateProvider: &simpleCreateOrUpdater{},
			}
			if tc.hcp != nil {
				r.hcpKey = client.ObjectKeyFromObject(tc.hcp)
			}
			for _, m := range tc.machines {
				_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{
					Namespace: m.Namespace,
					Name:      m.Name,
				}})
				g.Expect(err != nil).To(Equal(tc.error), func() string {
					if !tc.error {
						return fmt.Sprintf("unexpected error: %v", err)
					} else {
						return "missing expected error"
					}
				})
			}
			obtainedEndpointSliceList := discoveryv1.EndpointSliceList{}
			g.Expect(r.kubevirtInfraClient.List(t.Context(), &obtainedEndpointSliceList)).To(Succeed())
			_, err := yaml.Marshal(obtainedEndpointSliceList)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(obtainedEndpointSliceList.Items).To(WithTransform(resetResourceVersionFromEndpointSlices, ConsistOf(tc.expectedIngressEndpointSlices)))

			obtainedServicesList := corev1.ServiceList{}
			g.Expect(r.kubevirtInfraClient.List(t.Context(), &obtainedServicesList)).To(Succeed())
			_, err = yaml.Marshal(obtainedServicesList)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(obtainedServicesList.Items).To(WithTransform(resetResourceVersionFromServices, ConsistOf(tc.expectedServices)))
		})
	}
}

type simpleCreateOrUpdater struct{}

func (*simpleCreateOrUpdater) CreateOrUpdate(ctx context.Context, c client.Client, obj client.Object, f controllerutil.MutateFn) (controllerutil.OperationResult, error) {
	return controllerutil.CreateOrUpdate(ctx, c, obj, f)
}

func resetResourceVersionFromServices(services []corev1.Service) []corev1.Service {
	for i := range services {
		services[i].ResourceVersion = ""
	}
	return services
}

func resetResourceVersionFromEndpointSlices(endpointSlices []discoveryv1.EndpointSlice) []discoveryv1.EndpointSlice {
	for i := range endpointSlices {
		endpointSlices[i].ResourceVersion = ""
	}
	return endpointSlices
}

func TestNodeHasReadyEndpoint(t *testing.T) {
	ready := func(nodeName string, ready, serving, terminating *bool) discoveryv1.Endpoint {
		return discoveryv1.Endpoint{
			Addresses: []string{"10.132.0.20"},
			NodeName:  &nodeName,
			Conditions: discoveryv1.EndpointConditions{
				Ready:       ready,
				Serving:     serving,
				Terminating: terminating,
			},
		}
	}
	slices := func(endpoints ...discoveryv1.Endpoint) []discoveryv1.EndpointSlice {
		return []discoveryv1.EndpointSlice{{Endpoints: endpoints}}
	}

	testCases := []struct {
		name     string
		slices   []discoveryv1.EndpointSlice
		nodeName string
		want     bool
	}{
		{
			name:     "When a ready endpoint is on the node, it should match",
			slices:   slices(ready("guest-worker-1", ptr.To(true), ptr.To(true), ptr.To(false))),
			nodeName: "guest-worker-1",
			want:     true,
		},
		{
			name:     "When endpoint conditions are unset, it should match",
			slices:   slices(ready("guest-worker-1", nil, nil, nil)),
			nodeName: "guest-worker-1",
			want:     true,
		},
		{
			name:     "When the endpoint is on another node, it should not match",
			slices:   slices(ready("guest-worker-2", ptr.To(true), ptr.To(true), nil)),
			nodeName: "guest-worker-1",
			want:     false,
		},
		{
			name:     "When the endpoint is not ready, it should not match",
			slices:   slices(ready("guest-worker-1", ptr.To(false), ptr.To(true), ptr.To(false))),
			nodeName: "guest-worker-1",
			want:     false,
		},
		{
			name:     "When the endpoint is not serving, it should not match",
			slices:   slices(ready("guest-worker-1", ptr.To(true), ptr.To(false), ptr.To(false))),
			nodeName: "guest-worker-1",
			want:     false,
		},
		{
			name:     "When the endpoint is terminating, it should not match",
			slices:   slices(ready("guest-worker-1", ptr.To(true), ptr.To(true), ptr.To(true))),
			nodeName: "guest-worker-1",
			want:     false,
		},
		{
			name:     "When the node name is empty, it should not match",
			slices:   slices(ready("guest-worker-1", ptr.To(true), ptr.To(true), nil)),
			nodeName: "",
			want:     false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(nodeHasReadyEndpoint(tc.slices, tc.nodeName)).To(Equal(tc.want))
		})
	}
}
