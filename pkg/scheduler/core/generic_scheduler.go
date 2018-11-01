/*
Copyright 2014 The Kubernetes Authors.

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

package core

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"

	"k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utiltrace "k8s.io/apiserver/pkg/util/trace"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	"k8s.io/kubernetes/pkg/scheduler/core/equivalence"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"
)

//tanle
// GetAllNamespaces gets all namespaces //tanle
func GetAllNamespaces(nodeNameToInfo map[string]*schedulercache.NodeInfo, nodes []*v1.Node) []string {
	names := sets.String{}
	for _, node := range nodes {
		pods := nodeNameToInfo[node.Name].Pods()
		for _, pod := range pods {
			if pod.Namespace != "kube-system" {
				names.Insert(pod.Namespace)
			}
		}
	}
	return names.List()
}

// GetMaxResource retrives the total allocable resources of the cluster //tanle
func GetMaxResource(nodeNameToInfo map[string]*schedulercache.NodeInfo, nodes []*v1.Node) *schedulercache.Resource {
	result := &schedulercache.Resource{}
	for _, node := range nodes {
		nodeInfo := nodeNameToInfo[node.Name]
		result.MilliCPU += nodeInfo.AllocatableResource().MilliCPU
		result.Memory += nodeInfo.AllocatableResource().Memory
		result.ScalarResources[schedulercache.NvidiaGPU] += nodeInfo.AllocatableResource().ScalarResources[schedulercache.NvidiaGPU]
		result.EphemeralStorage += nodeInfo.AllocatableResource().EphemeralStorage
	}
	return result
}

// GetResourceRequest obtains the resource request of a pod
func GetResourceRequest(pod *v1.Pod) *schedulercache.Resource {
	result := &schedulercache.Resource{}
	for _, container := range pod.Spec.Containers {
		result.Add(container.Resources.Requests)
	}

	// take max_resource(sum_pod, any_init_container)
	for _, container := range pod.Spec.InitContainers {
		for rName, rQuantity := range container.Resources.Requests {
			switch rName {
			case v1.ResourceMemory:
				if mem := rQuantity.Value(); mem > result.Memory {
					result.Memory = mem
				}
			case v1.ResourceEphemeralStorage:
				if ephemeralStorage := rQuantity.Value(); ephemeralStorage > result.EphemeralStorage {
					result.EphemeralStorage = ephemeralStorage
				}
			case v1.ResourceCPU:
				if cpu := rQuantity.MilliValue(); cpu > result.MilliCPU {
					result.MilliCPU = cpu
				}
			case schedulercache.NvidiaGPU:
				if gpu := rQuantity.Value(); gpu > result.ScalarResources[schedulercache.NvidiaGPU] {
					result.ScalarResources[schedulercache.NvidiaGPU] = gpu
				}
			}
		}
	}

	return result
}

// IRA_Add adds ResourceList into Resource.
func AddIRA(result *schedulercache.Resource, rl v1.ResourceList) *schedulercache.Resource {
	// result := &schedulercache.Resource{}
	// if r == nil {
	// 	return result
	// }
	milliCPU := int64(0)
	nvidiaGPU := int64(0)
	memory := int64(0)
	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			milliCPU += rQuant.MilliValue()
		case v1.ResourceMemory:
			memory += rQuant.Value()
		case schedulercache.NvidiaGPU:
			nvidiaGPU += rQuant.Value()
		}
	}
	result.ScalarResources[schedulercache.NvidiaGPU] += nvidiaGPU
	result.Memory += memory
	if nvidiaGPU == 0 { // ignore cpu usage of GPU jobs
		result.MilliCPU += milliCPU
	}
	return result
}

// GetResourceUsageByNamespace obatains the resource usage by namespace (user) //tanle
func GetResourceUsageByNamespace(nodeNameToInfo map[string]*schedulercache.NodeInfo, nodes []*v1.Node, ns string) *schedulercache.Resource {
	result := &schedulercache.Resource{}
	for _, node := range nodes {
		// count the scheduled pods.
		pods := nodeNameToInfo[node.Name].Pods()
		for _, pod := range pods {
			if pod.Namespace == ns {
				// glog.Infof("pod: %s status.phase %s", pod.Name, pod.Status.Phase)
				for _, container := range pod.Spec.Containers {
					result = AddIRA(result, container.Resources.Requests)
				}

			}
		}
	}
	// glog.Infof("namespace: %s usage %s", ns, *result)

	return result
}

type Demand struct {
	cpu     float64
	mem     float64
	gpu     float64
	gpu_mem float64
	beta    float64
}

var namespaces = []string{"user1", "user2", "user3", "user4"}

func getDemand(namespace string) *Demand {
	resDemand := &Demand{cpu: 0.0, mem: 0.0, gpu: 0.0, gpu_mem: 0.0}
	// (com, mem, beta) = (80000.0,4000,2.5)
	// (com, mem, beta) = (112000.0,4000,3.5)
	// (com, mem, beta) = (192000.0,4000,6.0)
	// (com, mem, beta) = (288000.0,4000,9.0)
	switch namespace {
	case "user1":
		resDemand.cpu = 16000
		resDemand.mem = 4 * GI
		resDemand.gpu = 1
		resDemand.gpu_mem = 2 * GI
		resDemand.beta = 5 * resDemand.cpu / resDemand.gpu
	case "user2":
		resDemand.cpu = 16000
		resDemand.mem = 8 * GI
		resDemand.gpu = 1
		resDemand.beta = 7 * resDemand.cpu / resDemand.gpu
	case "user3":
		resDemand.cpu = 16000
		resDemand.mem = 6 * GI
		resDemand.gpu = 1
		resDemand.gpu_mem = 2 * GI
		resDemand.beta = 12 * resDemand.cpu / resDemand.gpu
	case "user4":
		resDemand.cpu = 16000
		resDemand.mem = 12 * GI
		resDemand.gpu = 1
		resDemand.gpu_mem = 2 * GI
		resDemand.beta = 18 * resDemand.cpu / resDemand.gpu
	}
	return resDemand
}

const MILLI = 1000
const GI = 1024 * 1024 * 1024

func getTraditionalDemand_GPU(namespace string) *Demand {
	resDemand := &Demand{cpu: 0.0, mem: 0.0, gpu: 0.0, beta: 0.0}
	switch namespace {
	case "user1":
		resDemand.cpu = 1 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 1
	case "user2":
		resDemand.cpu = 1 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 1
	case "user3":
		resDemand.cpu = 1 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 1
	case "user4":
		resDemand.cpu = 1 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 1
	}
	return resDemand
}

func getTraditionalDemand(namespace string) *Demand {
	resDemand := &Demand{cpu: 0.0, mem: 0.0, gpu: 0.0, beta: 0.0}
	switch namespace {
	case "user1":
		resDemand.cpu = 16 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 0
	case "user2":
		resDemand.cpu = 16 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 0
	case "user3":
		resDemand.cpu = 16 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 0
	case "user4":
		resDemand.cpu = 16 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 0
	default:
		resDemand.cpu = 16 * MILLI
		resDemand.mem = 12 * GI
		resDemand.gpu = 0
	}
	return resDemand
}

func AllocateES(namespaces []string, capacity schedulercache.Resource) []*schedulercache.Resource {
	n := len(namespaces)
	shares := make([]*schedulercache.Resource, n)
	for i := 0; i < n; i++ {
		shares[i] = &schedulercache.Resource{}
		shares[i].MilliCPU = int64(float64(capacity.MilliCPU) / float64(n))
		shares[i].Memory = int64(float64(capacity.Memory) / float64(n))
		shares[i].ScalarResources[schedulercache.NvidiaGPU] = int64(float64(capacity.ScalarResources[schedulercache.NvidiaGPU]) / float64(n))
	}
	return shares
}

func AllocateStatic(namespaces []string, capacity schedulercache.Resource) []*schedulercache.Resource {
	n := len(namespaces)
	shares := make([]*schedulercache.Resource, n)

	// shares[0] = &schedulercache.Resource{}
	// shares[0].MilliCPU = 2000
	// shares[0].Memory = 24 * GI
	// shares[0].NvidiaGPU = 2

	// shares[1] = &schedulercache.Resource{}
	// shares[1].MilliCPU = 86000
	// shares[1].Memory = (240 - 24) * GI
	// shares[1].NvidiaGPU = 2

	shares[0] = &schedulercache.Resource{}
	shares[0].MilliCPU = 32000
	shares[0].Memory = 24 * GI
	shares[0].ScalarResources[schedulercache.NvidiaGPU] = 0

	shares[1] = &schedulercache.Resource{}
	shares[1].MilliCPU = 0000
	shares[1].Memory = 12 * GI
	shares[1].ScalarResources[schedulercache.NvidiaGPU] = 0

	shares[2] = &schedulercache.Resource{}
	shares[2].MilliCPU = 16000
	shares[2].Memory = 12 * GI
	shares[2].ScalarResources[schedulercache.NvidiaGPU] = 0

	shares[3] = &schedulercache.Resource{}
	shares[3].MilliCPU = 16000
	shares[3].Memory = 12 * GI
	shares[3].ScalarResources[schedulercache.NvidiaGPU] = 0

	return shares
}

const (
	// minFeasibleNodesToFind is the minimum number of nodes that would be scored
	// in each scheduling cycle. This is a semi-arbitrary value to ensure that a
	// certain minimum of nodes are checked for feasibility. This in turn helps
	// ensure a minimum level of spreading.
	minFeasibleNodesToFind = 100
)

// FailedPredicateMap declares a map[string][]algorithm.PredicateFailureReason type.
type FailedPredicateMap map[string][]algorithm.PredicateFailureReason

// FitError describes a fit error of a pod.
type FitError struct {
	Pod              *v1.Pod
	NumAllNodes      int
	FailedPredicates FailedPredicateMap
}

// ErrNoNodesAvailable is used to describe the error that no nodes available to schedule pods.
var ErrNoNodesAvailable = fmt.Errorf("no nodes available to schedule pods")

const (
	// NoNodeAvailableMsg is used to format message when no nodes available.
	NoNodeAvailableMsg = "0/%v nodes are available"
)

// Error returns detailed information of why the pod failed to fit on each node
func (f *FitError) Error() string {
	reasons := make(map[string]int)
	for _, predicates := range f.FailedPredicates {
		for _, pred := range predicates {
			reasons[pred.GetReason()]++
		}
	}

	sortReasonsHistogram := func() []string {
		reasonStrings := []string{}
		for k, v := range reasons {
			reasonStrings = append(reasonStrings, fmt.Sprintf("%v %v", v, k))
		}
		sort.Strings(reasonStrings)
		return reasonStrings
	}
	reasonMsg := fmt.Sprintf(NoNodeAvailableMsg+": %v.", f.NumAllNodes, strings.Join(sortReasonsHistogram(), ", "))
	return reasonMsg
}

type genericScheduler struct {
	cache                    schedulercache.Cache
	equivalenceCache         *equivalence.Cache
	schedulingQueue          SchedulingQueue
	predicates               map[string]algorithm.FitPredicate
	priorityMetaProducer     algorithm.PriorityMetadataProducer
	predicateMetaProducer    algorithm.PredicateMetadataProducer
	prioritizers             []algorithm.PriorityConfig
	extenders                []algorithm.SchedulerExtender
	lastNodeIndex            uint64
	alwaysCheckAllPredicates bool
	cachedNodeInfoMap        map[string]*schedulercache.NodeInfo
	volumeBinder             *volumebinder.VolumeBinder
	pvcLister                corelisters.PersistentVolumeClaimLister
	disablePreemption        bool
	percentageOfNodesToScore int32
	client                   clientset.Interface //tanle placing the pod on different devices.
}

//[tanle] placeOnOtherDevice
func CreatePodOnOtherDevice(pod *v1.Pod) *v1.Pod {
	toBeGPU := true
	// check the device of the pod
	for _, container := range pod.Spec.Containers {
		if strings.Contains(container.Image, "gpu") {
			toBeGPU = false
			break
		}
	}

	// delete the pod in kube system
	replicatedPod := pod.DeepCopy()

	for cName, container := range replicatedPod.Spec.Containers {
		if toBeGPU {
			container.Image = "lenhattan86/ira:gpu"
		} else {
			container.Image = "lenhattan86/ira:cpu"
		}
		// switch commands
		mainCmd := container.Command[3]
		container.Command[3] = container.Command[2]
		container.Command[2] = mainCmd
		// switch demands
		mainDemand := container.Command[5]
		container.Command[5] = container.Command[4]
		container.Command[4] = mainDemand

		cpuDemand, gpuDemand, memory := schedulercache.GetSecondaryDemand(pod)

		for rName := range container.Resources.Requests {
			quantity := container.Resources.Requests[rName]
			switch rName {
			case v1.ResourceCPU:
				quantity.SetMilli(cpuDemand)
			case schedulercache.NvidiaGPU:
				quantity.Set(gpuDemand)
			case v1.ResourceMemory:
				quantity.Set(memory * GI)
			}
			container.Resources.Requests[rName] = quantity
			container.Resources.Limits[rName] = quantity
		}
		replicatedPod.Spec.Containers[cName] = container
	}

	replicatedPod.ResourceVersion = ""
	replicatedPod.Spec.NodeName = ""
	replicatedPod.Annotations = nil

	return replicatedPod

	// pod.Spec.Containers = replicatedPod.Spec.Containers

	// // p.Client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{})
	// if err := g.client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{}); err != nil {
	// 	runtime.HandleError(fmt.Errorf("unable to DELETE pod in kubectl %T: %v", pod, err))
	// }
	// if _, err := g.client.CoreV1().Pods(replicatedPod.Namespace).Create(replicatedPod); err != nil {
	// 	runtime.HandleError(fmt.Errorf("unable to CREATE pod in kubectl %T: %v", replicatedPod, err))
	// }
}
func (g *genericScheduler) placeOnOtherDevice(pod *v1.Pod, replicatedPod *v1.Pod) {
	pod.Spec.Containers = replicatedPod.Spec.Containers

	// p.Client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{})
	if err := g.client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{}); err != nil {
		runtime.HandleError(fmt.Errorf("unable to DELETE pod in kubectl %T: %v", pod, err))
	}
	if _, err := g.client.CoreV1().Pods(replicatedPod.Namespace).Create(replicatedPod); err != nil {
		runtime.HandleError(fmt.Errorf("unable to CREATE pod in kubectl %T: %v", replicatedPod, err))
	}
}

// Schedule tries to schedule the given pod to one of the nodes in the node list.
// If it succeeds, it will return the name of the node.
// If it fails, it will return a FitError error with reasons.
func (g *genericScheduler) SynClusterInfo(nodeLister algorithm.NodeLister) {
	nodes, err := nodeLister.List()
	if err != nil {
		return
	}
	schedulercache.SynClusterInfo(g.cachedNodeInfoMap, nodes)
}

func (g *genericScheduler) Schedule(pod *v1.Pod, nodeLister algorithm.NodeLister) (string, error) {
	trace := utiltrace.New(fmt.Sprintf("Scheduling %s/%s", pod.Namespace, pod.Name))
	defer trace.LogIfLong(100 * time.Millisecond)

	if err := podPassesBasicChecks(pod, g.pvcLister); err != nil {
		return "", err
	}

	nodes, err := nodeLister.List()
	if err != nil {
		return "", err
	}
	if len(nodes) == 0 {
		return "", ErrNoNodesAvailable
	}

	// Used for all fit and priority funcs.
	err = g.cache.UpdateNodeNameToInfoMap(g.cachedNodeInfoMap)
	if err != nil {
		return "", err
	}

	schedulercache.SynClusterInfo(g.cachedNodeInfoMap, nodes) // tanle syn cluster info manually

	trace.Step("Computing predicates")
	startPredicateEvalTime := time.Now()
	filteredNodes, failedPredicateMap, err := g.findNodesThatFit(pod, nodes)

	// tanle
	if schedulercache.NUM_RESERVE_GPU > 0 {
		if schedulercache.DoNotUseReserveResource(schedulercache.IsGpuPod(pod)) {
			filteredNodes = []*v1.Node{}
			return "", fmt.Errorf("[tanle] not enough reserved resource for this user: %s's pod: %s ", pod.Namespace, pod.Name)
		}
	}

	//[tanle] additional scheduling & device placement
	if schedulercache.ENABLE_OFFLINE_SCHEDULER {
		if len(filteredNodes) >= 0 {
			isAdmit, isSwitch := advancedSchedule(pod, g.cachedNodeInfoMap, nodes)
			if !isAdmit {
				filteredNodes = []*v1.Node{}
				return "", fmt.Errorf("not enough resource for this user: %s's pod: %s ", pod.Namespace, pod.Name)
			} else if isSwitch {
				//switch jobs
				glog.Infof("Place the pod %s on other device", pod.Name)
				fakePod := CreatePodOnOtherDevice(pod)
				availNodes, _, _ := g.findNodesThatFit(fakePod, nodes)
				if len(availNodes) > 0 {
					g.placeOnOtherDevice(pod, fakePod)
					filteredNodes = availNodes
				} else {
					filteredNodes = []*v1.Node{}
					return "", fmt.Errorf("not enough resource for this user: %s's pod: %s ", pod.Namespace, pod.Name)
				}
			}
		}
	}

	if err != nil {
		return "", err
	}

	if len(filteredNodes) == 0 {
		return "", &FitError{
			Pod:              pod,
			NumAllNodes:      len(nodes),
			FailedPredicates: failedPredicateMap,
		}
	}
	metrics.SchedulingAlgorithmPredicateEvaluationDuration.Observe(metrics.SinceInMicroseconds(startPredicateEvalTime))
	metrics.SchedulingLatency.WithLabelValues(metrics.PredicateEvaluation).Observe(metrics.SinceInSeconds(startPredicateEvalTime))

	trace.Step("Prioritizing")
	startPriorityEvalTime := time.Now()
	// When only one node after predicate, just use it.
	if len(filteredNodes) == 1 {
		metrics.SchedulingAlgorithmPriorityEvaluationDuration.Observe(metrics.SinceInMicroseconds(startPriorityEvalTime))
		return filteredNodes[0].Name, nil
	}

	metaPrioritiesInterface := g.priorityMetaProducer(pod, g.cachedNodeInfoMap)
	priorityList, err := PrioritizeNodes(pod, g.cachedNodeInfoMap, metaPrioritiesInterface, g.prioritizers, filteredNodes, g.extenders)
	if err != nil {
		return "", err
	}
	metrics.SchedulingAlgorithmPriorityEvaluationDuration.Observe(metrics.SinceInMicroseconds(startPriorityEvalTime))
	metrics.SchedulingLatency.WithLabelValues(metrics.PriorityEvaluation).Observe(metrics.SinceInSeconds(startPriorityEvalTime))

	trace.Step("Selecting host")
	return g.selectHost(priorityList)
}

var CAPACITY = schedulercache.Resource{MilliCPU: 384 * 1000, Memory: 1152 * GI, ScalarResources: map[v1.ResourceName]int64{schedulercache.NvidiaGPU: 12}}

func advancedSchedule(pod *v1.Pod,
	nodeNameToInfo map[string]*schedulercache.NodeInfo,
	nodes []*v1.Node) (bool, bool) {

	isAdmit := true
	isSwitch := false

	uID := -1
	for u := 0; u < len(namespaces); u++ {
		if namespaces[u] == pod.Namespace {
			uID = u
		}
	}

	if uID < 0 {
		return true, false
	}

	//tanle: DRF or FDRF allocation
	// glog.Infof("findNodesThatFit ")
	// namespaces := GetAllNamespaces(nodeNameToInfo, nodes)

	currentUsage := GetResourceUsageByNamespace(nodeNameToInfo, nodes, pod.Namespace)
	podDemand := GetResourceRequest(pod)
	// capacity := GetMaxResource(nodeNameToInfo, nodes)
	n := len(namespaces)
	shares := make([]*schedulercache.Resource, n)
	switch schedulercache.SCHEDULER {
	case schedulercache.ES:
		shares = AllocateES(namespaces, CAPACITY)
	case schedulercache.Static:
		shares = AllocateStatic(namespaces, CAPACITY)
	case schedulercache.NaiveDRF:
		shares = AllocateNaiveDRF(namespaces, CAPACITY)
		// case schedulercache.AlloX:
		// 	shares = AllocateAlloX(namespaces, CAPACITY)
	}

	milliCPU, gpu, memInGi := schedulercache.GetSecondaryDemand(pod)
	secDemand := &schedulercache.Resource{MilliCPU: milliCPU, ScalarResources: map[v1.ResourceName]int64{schedulercache.NvidiaGPU: gpu}, Memory: memInGi * GI}
	isAdmit, isSwitch = Fit(podDemand, secDemand, currentUsage, shares[uID])

	if !isAdmit {
		// glog.Infof("shares %s", shares)
		// glog.Infof("Rejected pod: %s %s primeDemand %s secDemand %s usage %s ", pod.Namespace, pod.Name, podDemand, secDemand, currentUsage)
		return isAdmit, isSwitch
	}
	// glog.Infof("admit pod: %s %s primeDemand %s secDemand %s usage %s ", pod.Namespace, pod.Name, podDemand, secDemand, currentUsage)
	return isAdmit, isSwitch
}

// Fit fits the demand to the share.
func Fit(priDemand *schedulercache.Resource, secDemand *schedulercache.Resource, currentUsage *schedulercache.Resource, share *schedulercache.Resource) (bool, bool) {
	isFit := true
	isSwitch := false
	if priDemand.Memory+currentUsage.Memory > share.Memory {
		// glog.Infof("demand.Memory: %d, currentUsage.Memory %d, share.Memory %d", demand.Memory, currentUsage.Memory, share.Memory)
		return false, isSwitch
	}

	// for GPU jobs
	if priDemand.ScalarResources[schedulercache.NvidiaGPU]+currentUsage.ScalarResources[schedulercache.NvidiaGPU] > share.ScalarResources[schedulercache.NvidiaGPU] {
		if secDemand.MilliCPU+currentUsage.MilliCPU > share.MilliCPU {
			isFit = false
		} else {
			glog.Infof("enough resource for this GPU job on CPU priDemand %s secDemand %s", priDemand, secDemand)
			isSwitch = true
		}
	}

	// for CPU jobs
	if (priDemand.ScalarResources[schedulercache.NvidiaGPU] == 0) && (priDemand.MilliCPU+currentUsage.MilliCPU > share.MilliCPU) {
		if secDemand.ScalarResources[schedulercache.NvidiaGPU]+currentUsage.ScalarResources[schedulercache.NvidiaGPU] > share.ScalarResources[schedulercache.NvidiaGPU] {
			isFit = false
		} else {
			glog.Infof("enough resource this CPU job on GPU for priDemand %s secDemand %s", priDemand, secDemand)
			isSwitch = true
		}
	}

	return isFit, isSwitch
}

// AllocateNaiveDRF is implemented for DRF with demand (cpu, mem, gpu)
func AllocateNaiveDRF(namespaces []string, capacity schedulercache.Resource) []*schedulercache.Resource {
	const N_RES_DIM = 3
	n := len(namespaces)
	// capacityArray := [3]float64{capacity.MilliCPU, capacity.Memory, capacity.NvidiaGPU}
	maxDemand := make([]float64, n)
	dorminantRates := make([]float64, N_RES_DIM)
	demands := make([]Demand, n)

	for i := 0; i < n; i++ {
		demands[i] = *getTraditionalDemand(namespaces[i])
	}
	// glog.Infof("capacity: %s", capacity)
	// glog.Infof("demands: %s", demands)

	// convert demand based on beta
	for i := 0; i < n; i++ {
		normalizedDemand := [3]float64{0, 0, 0}
		normalizedDemand[0] = float64(demands[i].cpu) / float64(capacity.MilliCPU)
		normalizedDemand[1] = float64(demands[i].mem) / float64(capacity.Memory)
		normalizedDemand[2] = float64(demands[i].gpu) / float64(capacity.ScalarResources[schedulercache.NvidiaGPU])

		// get the dorminant rate = max demand / capacity
		maxDemand[i] = 0.0
		for r := 0; r < N_RES_DIM; r++ {
			if maxDemand[i] < normalizedDemand[r] {
				maxDemand[i] = normalizedDemand[r]
			}
		}

		for r := 0; r < N_RES_DIM; r++ {
			dorminantRates[r] += normalizedDemand[r] / maxDemand[i]
		}
		// glog.Infof("normalizedDemand: ", normalizedDemand)
	}
	// glog.Infof("dorminantRates: ", dorminantRates)
	// get total doriminant share of all users
	dorminantShare := 0.0
	for r := 0; r < N_RES_DIM; r++ {
		if dorminantShare < dorminantRates[r] {
			dorminantShare = dorminantRates[r]
		}
	}
	// compute the share for each user = capacity/dorminantRate * demand/dorminantDemand
	shares := make([]*schedulercache.Resource, n)
	for i := 0; i < n; i++ {
		ratio := dorminantShare * maxDemand[i]
		shares[i] = &schedulercache.Resource{}
		shares[i].MilliCPU = int64(demands[i].cpu / ratio)
		shares[i].Memory = int64(demands[i].mem / ratio)
		shares[i].ScalarResources[schedulercache.NvidiaGPU] = int64(Round(demands[i].gpu/ratio, 0.5, 4))
	}

	return shares
}

//Round: add round functions // tanle
func Round(val float64, roundOn float64, places int) float64 {

	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)

	var round float64
	if val > 0 {
		if div >= roundOn {
			round = math.Ceil(digit)
		} else {
			round = math.Floor(digit)
		}
	} else {
		if div >= roundOn {
			round = math.Floor(digit)
		} else {
			round = math.Ceil(digit)
		}
	}

	return round / pow
}

type IndexSorter struct {
	Target  []float64
	Indices []int
}

func NewSorter(t []float64) IndexSorter {
	iv := make([]int, len(t))
	for i := range iv {
		iv[i] = i
	}
	return IndexSorter{Target: t, Indices: iv}
}
func (s IndexSorter) Len() int           { return len(s.Target) }
func (s IndexSorter) Less(i, j int) bool { return s.Target[i] < s.Target[j] }
func (s IndexSorter) Swap(i, j int) {
	s.Target[i], s.Target[j] = s.Target[j], s.Target[i]
	s.Indices[i], s.Indices[j] = s.Indices[j], s.Indices[i]
}

// AllocateAlloX pricing algorithm
func AllocateAlloX(namespaces []string, capacity schedulercache.Resource) []*schedulercache.Resource {
	nResource := 3
	n := len(namespaces)
	shares := make([]*schedulercache.Resource, n)
	// step 1: sort users based on beta (ascending)
	betas := make([]float64, n)
	demands := make([]Demand, n)
	for i := 0; i < n; i++ {
		demands[i] = *getDemand(namespaces[i])
		betas[i] = demands[i].beta
	}
	// sort.Float64s(betas)
	s := NewSorter(betas)
	sort.Sort(s)
	betas = s.Target
	sortedIds := s.Indices
	sortedDemands := make([]Demand, n)
	for i := 0; i < n; i++ {
		sortedDemands[i] = demands[sortedIds[i]] // sort demands according to betas
	}

	// step 2: initialization
	price := make([]float64, nResource)
	price[0] = 1          // for cpu
	price[1] = betas[n-1] // for GPU
	useralloc := UserAlloc(betas, price)
	prevUserAlloc := useralloc

	currLoad := sumResourceNorm(useralloc) // normalized load
	gpumin := n - 1
	flag := true

	if n == 0 {
		return shares
	} else if n == 1 {
		useralloc[0].cpu = 1.0
		useralloc[0].mem = 1.0
		useralloc[0].gpu = 1.0
	}

	// step 3: pricing
	for flag {
		if currLoad.cpu <= currLoad.gpu {
			prevLoad := sumResourceNorm(prevUserAlloc)
			useralloc = prevUserAlloc
			useralloc[gpumin].cpu = prevUserAlloc[gpumin].cpu + (prevLoad.gpu-prevLoad.cpu)*float64(CAPACITY.MilliCPU)/2
			useralloc[gpumin].gpu = prevUserAlloc[gpumin].gpu - (prevLoad.gpu-prevLoad.cpu)*float64(CAPACITY.ScalarResources[schedulercache.NvidiaGPU])/2
			break
		}

		gpumin = gpumin - 1
		if gpumin < 0 {
			print("###gpumin is negative####")
			break
		}

		price[0] = 1
		price[1] = betas[gpumin]
		prevUserAlloc = useralloc
		useralloc = UserAlloc(betas, price)
		currLoad = sumResourceNorm(useralloc)
	}

	sumAlloc := sumResource(useralloc)

	//
	for i := 0; i < n; i++ {
		demand := demands[i]
		shares[sortedIds[i]] = &schedulercache.Resource{}
		shares[sortedIds[i]].MilliCPU = int64(useralloc[i].cpu * float64(capacity.MilliCPU) / sumAlloc.cpu)
		gpu := Round(useralloc[i].gpu*float64(capacity.ScalarResources[schedulercache.NvidiaGPU])/sumAlloc.gpu, 0.5, 0)
		shares[sortedIds[i]].ScalarResources[schedulercache.NvidiaGPU] = int64(gpu)
		mem := (float64(shares[sortedIds[i]].MilliCPU) / demand.cpu * demand.mem) + (float64(shares[sortedIds[i]].ScalarResources[schedulercache.NvidiaGPU]) / demand.gpu * demand.gpu_mem)
		roundGi := Round(mem/GI, 0.5, 0)
		shares[sortedIds[i]].Memory = int64(roundGi * GI)
	}
	return shares
}

func sumResource(resources []*Demand) *Demand {
	result := &Demand{0, 0, 0, 0, 0}
	for _, res := range resources {
		result.cpu = result.cpu + res.cpu
		result.mem = result.mem + res.mem
		result.gpu = result.gpu + res.gpu
	}
	return result
}

func sumResourceNorm(resources []*Demand) *Demand {
	result := &Demand{0, 0, 0, 0, 0}
	for _, res := range resources {
		result.cpu = result.cpu + res.cpu
		result.mem = result.mem + res.mem
		result.gpu = result.gpu + res.gpu
	}
	result.cpu = result.cpu / (float64)(CAPACITY.MilliCPU)
	result.mem = result.mem / (float64)(CAPACITY.Memory)
	result.gpu = result.gpu / (float64)(CAPACITY.ScalarResources[schedulercache.NvidiaGPU])
	return result
}

func UserAlloc(betas []float64, currentPrices []float64) []*Demand {
	n := len(betas)
	userAlloc := make([]*Demand, n)
	for j := 0; j < n; j++ {
		beta := betas[j]
		alloc := &Demand{0, 0, 0, 0, 0}
		if beta < currentPrices[1] {
			alloc.cpu = 1
			alloc.gpu = 0
		} else // if beta = price, put it in GPU.
		{
			alloc.cpu = 0
			alloc.gpu = 1 / currentPrices[1]
		}
		userAlloc[j] = alloc
	}
	return userAlloc
}

// Prioritizers returns a slice containing all the scheduler's priority
// functions and their config. It is exposed for testing only.
func (g *genericScheduler) Prioritizers() []algorithm.PriorityConfig {
	return g.prioritizers
}

// Predicates returns a map containing all the scheduler's predicate
// functions. It is exposed for testing only.
func (g *genericScheduler) Predicates() map[string]algorithm.FitPredicate {
	return g.predicates
}

// findMaxScores returns the indexes of nodes in the "priorityList" that has the highest "Score".
func findMaxScores(priorityList schedulerapi.HostPriorityList) []int {
	maxScoreIndexes := make([]int, 0, len(priorityList)/2)
	maxScore := priorityList[0].Score
	for i, hp := range priorityList {
		if hp.Score > maxScore {
			maxScore = hp.Score
			maxScoreIndexes = maxScoreIndexes[:0]
			maxScoreIndexes = append(maxScoreIndexes, i)
		} else if hp.Score == maxScore {
			maxScoreIndexes = append(maxScoreIndexes, i)
		}
	}
	return maxScoreIndexes
}

// selectHost takes a prioritized list of nodes and then picks one
// in a round-robin manner from the nodes that had the highest score.
func (g *genericScheduler) selectHost(priorityList schedulerapi.HostPriorityList) (string, error) {
	if len(priorityList) == 0 {
		return "", fmt.Errorf("empty priorityList")
	}

	maxScores := findMaxScores(priorityList)
	ix := int(g.lastNodeIndex % uint64(len(maxScores)))
	g.lastNodeIndex++

	return priorityList[maxScores[ix]].Host, nil
}

// preempt finds nodes with pods that can be preempted to make room for "pod" to
// schedule. It chooses one of the nodes and preempts the pods on the node and
// returns 1) the node, 2) the list of preempted pods if such a node is found,
// 3) A list of pods whose nominated node name should be cleared, and 4) any
// possible error.
func (g *genericScheduler) Preempt(pod *v1.Pod, nodeLister algorithm.NodeLister, scheduleErr error) (*v1.Node, []*v1.Pod, []*v1.Pod, error) {
	// Scheduler may return various types of errors. Consider preemption only if
	// the error is of type FitError.
	fitError, ok := scheduleErr.(*FitError)
	if !ok || fitError == nil {
		return nil, nil, nil, nil
	}
	err := g.cache.UpdateNodeNameToInfoMap(g.cachedNodeInfoMap)
	nodes, _ := nodeLister.List()
	schedulercache.SynClusterInfo(g.cachedNodeInfoMap, nodes) // tanle syn cluster info manually
	if err != nil {
		return nil, nil, nil, err
	}
	if !podEligibleToPreemptOthers(pod, g.cachedNodeInfoMap) {
		glog.V(5).Infof("Pod %v/%v is not eligible for more preemption.", pod.Namespace, pod.Name)
		return nil, nil, nil, nil
	}
	allNodes, err := nodeLister.List()
	if err != nil {
		return nil, nil, nil, err
	}
	if len(allNodes) == 0 {
		return nil, nil, nil, ErrNoNodesAvailable
	}
	potentialNodes := nodesWherePreemptionMightHelp(allNodes, fitError.FailedPredicates)
	if len(potentialNodes) == 0 {
		glog.V(3).Infof("Preemption will not help schedule pod %v/%v on any node.", pod.Namespace, pod.Name)
		// In this case, we should clean-up any existing nominated node name of the pod.
		return nil, nil, []*v1.Pod{pod}, nil
	}
	pdbs, err := g.cache.ListPDBs(labels.Everything())
	if err != nil {
		return nil, nil, nil, err
	}
	nodeToVictims, err := selectNodesForPreemption(pod, g.cachedNodeInfoMap, potentialNodes, g.predicates,
		g.predicateMetaProducer, g.schedulingQueue, pdbs)
	if err != nil {
		return nil, nil, nil, err
	}

	// We will only check nodeToVictims with extenders that support preemption.
	// Extenders which do not support preemption may later prevent preemptor from being scheduled on the nominated
	// node. In that case, scheduler will find a different host for the preemptor in subsequent scheduling cycles.
	nodeToVictims, err = g.processPreemptionWithExtenders(pod, nodeToVictims)
	if err != nil {
		return nil, nil, nil, err
	}

	candidateNode := pickOneNodeForPreemption(nodeToVictims)
	if candidateNode == nil {
		return nil, nil, nil, err
	}

	// Lower priority pods nominated to run on this node, may no longer fit on
	// this node. So, we should remove their nomination. Removing their
	// nomination updates these pods and moves them to the active queue. It
	// lets scheduler find another place for them.
	nominatedPods := g.getLowerPriorityNominatedPods(pod, candidateNode.Name)
	if nodeInfo, ok := g.cachedNodeInfoMap[candidateNode.Name]; ok {
		return nodeInfo.Node(), nodeToVictims[candidateNode].Pods, nominatedPods, err
	}

	return nil, nil, nil, fmt.Errorf(
		"preemption failed: the target node %s has been deleted from scheduler cache",
		candidateNode.Name)
}

// processPreemptionWithExtenders processes preemption with extenders
func (g *genericScheduler) processPreemptionWithExtenders(
	pod *v1.Pod,
	nodeToVictims map[*v1.Node]*schedulerapi.Victims,
) (map[*v1.Node]*schedulerapi.Victims, error) {
	if len(nodeToVictims) > 0 {
		for _, extender := range g.extenders {
			if extender.SupportsPreemption() && extender.IsInterested(pod) {
				newNodeToVictims, err := extender.ProcessPreemption(
					pod,
					nodeToVictims,
					g.cachedNodeInfoMap,
				)
				if err != nil {
					if extender.IsIgnorable() {
						glog.Warningf("Skipping extender %v as it returned error %v and has ignorable flag set",
							extender, err)
						continue
					}
					return nil, err
				}

				// Replace nodeToVictims with new result after preemption. So the
				// rest of extenders can continue use it as parameter.
				nodeToVictims = newNodeToVictims

				// If node list becomes empty, no preemption can happen regardless of other extenders.
				if len(nodeToVictims) == 0 {
					break
				}
			}
		}
	}

	return nodeToVictims, nil
}

// getLowerPriorityNominatedPods returns pods whose priority is smaller than the
// priority of the given "pod" and are nominated to run on the given node.
// Note: We could possibly check if the nominated lower priority pods still fit
// and return those that no longer fit, but that would require lots of
// manipulation of NodeInfo and PredicateMeta per nominated pod. It may not be
// worth the complexity, especially because we generally expect to have a very
// small number of nominated pods per node.
func (g *genericScheduler) getLowerPriorityNominatedPods(pod *v1.Pod, nodeName string) []*v1.Pod {
	pods := g.schedulingQueue.WaitingPodsForNode(nodeName)

	if len(pods) == 0 {
		return nil
	}

	var lowerPriorityPods []*v1.Pod
	podPriority := util.GetPodPriority(pod)
	for _, p := range pods {
		if util.GetPodPriority(p) < podPriority {
			lowerPriorityPods = append(lowerPriorityPods, p)
		}
	}
	return lowerPriorityPods
}

// numFeasibleNodesToFind returns the number of feasible nodes that once found, the scheduler stops
// its search for more feasible nodes.
func (g *genericScheduler) numFeasibleNodesToFind(numAllNodes int32) int32 {
	if numAllNodes < minFeasibleNodesToFind || g.percentageOfNodesToScore <= 0 ||
		g.percentageOfNodesToScore >= 100 {
		return numAllNodes
	}
	numNodes := numAllNodes * g.percentageOfNodesToScore / 100
	if numNodes < minFeasibleNodesToFind {
		return minFeasibleNodesToFind
	}
	return numNodes
}

// Filters the nodes to find the ones that fit based on the given predicate functions
// Each node is passed through the predicate functions to determine if it is a fit
func (g *genericScheduler) findNodesThatFit(pod *v1.Pod, nodes []*v1.Node) ([]*v1.Node, FailedPredicateMap, error) {
	var filtered []*v1.Node
	failedPredicateMap := FailedPredicateMap{}

	if len(g.predicates) == 0 {
		filtered = nodes
	} else {
		allNodes := int32(g.cache.NodeTree().NumNodes)
		numNodesToFind := g.numFeasibleNodesToFind(allNodes)

		// Create filtered list with enough space to avoid growing it
		// and allow assigning.
		filtered = make([]*v1.Node, numNodesToFind)
		errs := errors.MessageCountMap{}
		var (
			predicateResultLock sync.Mutex
			filteredLen         int32
			equivClass          *equivalence.Class
		)

		ctx, cancel := context.WithCancel(context.Background())

		// We can use the same metadata producer for all nodes.
		meta := g.predicateMetaProducer(pod, g.cachedNodeInfoMap)

		if g.equivalenceCache != nil {
			// getEquivalenceClassInfo will return immediately if no equivalence pod found
			equivClass = equivalence.NewClass(pod)
		}

		checkNode := func(i int) {
			var nodeCache *equivalence.NodeCache
			nodeName := g.cache.NodeTree().Next()
			if g.equivalenceCache != nil {
				nodeCache, _ = g.equivalenceCache.GetNodeCache(nodeName)
			}
			fits, failedPredicates, err := podFitsOnNode(
				pod,
				meta,
				g.cachedNodeInfoMap[nodeName],
				g.predicates,
				g.cache,
				nodeCache,
				g.schedulingQueue,
				g.alwaysCheckAllPredicates,
				equivClass,
			)
			if err != nil {
				predicateResultLock.Lock()
				errs[err.Error()]++
				predicateResultLock.Unlock()
				return
			}
			if fits {
				length := atomic.AddInt32(&filteredLen, 1)
				if length > numNodesToFind {
					cancel()
					atomic.AddInt32(&filteredLen, -1)
				} else {
					filtered[length-1] = g.cachedNodeInfoMap[nodeName].Node()
				}
			} else {
				predicateResultLock.Lock()
				failedPredicateMap[nodeName] = failedPredicates
				predicateResultLock.Unlock()
			}
		}

		// Stops searching for more nodes once the configured number of feasible nodes
		// are found.
		workqueue.ParallelizeUntil(ctx, 16, int(allNodes), checkNode)

		filtered = filtered[:filteredLen]
		if len(errs) > 0 {
			return []*v1.Node{}, FailedPredicateMap{}, errors.CreateAggregateFromMessageCountMap(errs)
		}
	}

	if len(filtered) > 0 && len(g.extenders) != 0 {
		for _, extender := range g.extenders {
			if !extender.IsInterested(pod) {
				continue
			}
			filteredList, failedMap, err := extender.Filter(pod, filtered, g.cachedNodeInfoMap)
			if err != nil {
				if extender.IsIgnorable() {
					glog.Warningf("Skipping extender %v as it returned error %v and has ignorable flag set",
						extender, err)
					continue
				} else {
					return []*v1.Node{}, FailedPredicateMap{}, err
				}
			}

			for failedNodeName, failedMsg := range failedMap {
				if _, found := failedPredicateMap[failedNodeName]; !found {
					failedPredicateMap[failedNodeName] = []algorithm.PredicateFailureReason{}
				}
				failedPredicateMap[failedNodeName] = append(failedPredicateMap[failedNodeName], predicates.NewFailureReason(failedMsg))
			}
			filtered = filteredList
			if len(filtered) == 0 {
				break
			}
		}
	}
	return filtered, failedPredicateMap, nil
}

// addNominatedPods adds pods with equal or greater priority which are nominated
// to run on the node given in nodeInfo to meta and nodeInfo. It returns 1) whether
// any pod was found, 2) augmented meta data, 3) augmented nodeInfo.
func addNominatedPods(podPriority int32, meta algorithm.PredicateMetadata,
	nodeInfo *schedulercache.NodeInfo, queue SchedulingQueue) (bool, algorithm.PredicateMetadata,
	*schedulercache.NodeInfo) {
	if queue == nil || nodeInfo == nil || nodeInfo.Node() == nil {
		// This may happen only in tests.
		return false, meta, nodeInfo
	}
	nominatedPods := queue.WaitingPodsForNode(nodeInfo.Node().Name)
	if nominatedPods == nil || len(nominatedPods) == 0 {
		return false, meta, nodeInfo
	}
	var metaOut algorithm.PredicateMetadata
	if meta != nil {
		metaOut = meta.ShallowCopy()
	}
	nodeInfoOut := nodeInfo.Clone()
	for _, p := range nominatedPods {
		if util.GetPodPriority(p) >= podPriority {
			nodeInfoOut.AddPod(p)
			if metaOut != nil {
				metaOut.AddPod(p, nodeInfoOut)
			}
		}
	}
	return true, metaOut, nodeInfoOut
}

// podFitsOnNode checks whether a node given by NodeInfo satisfies the given predicate functions.
// For given pod, podFitsOnNode will check if any equivalent pod exists and try to reuse its cached
// predicate results as possible.
// This function is called from two different places: Schedule and Preempt.
// When it is called from Schedule, we want to test whether the pod is schedulable
// on the node with all the existing pods on the node plus higher and equal priority
// pods nominated to run on the node.
// When it is called from Preempt, we should remove the victims of preemption and
// add the nominated pods. Removal of the victims is done by SelectVictimsOnNode().
// It removes victims from meta and NodeInfo before calling this function.
func podFitsOnNode(
	pod *v1.Pod,
	meta algorithm.PredicateMetadata,
	info *schedulercache.NodeInfo,
	predicateFuncs map[string]algorithm.FitPredicate,
	cache schedulercache.Cache,
	nodeCache *equivalence.NodeCache,
	queue SchedulingQueue,
	alwaysCheckAllPredicates bool,
	equivClass *equivalence.Class,
) (bool, []algorithm.PredicateFailureReason, error) {
	var (
		eCacheAvailable  bool
		failedPredicates []algorithm.PredicateFailureReason
	)

	podsAdded := false
	// We run predicates twice in some cases. If the node has greater or equal priority
	// nominated pods, we run them when those pods are added to meta and nodeInfo.
	// If all predicates succeed in this pass, we run them again when these
	// nominated pods are not added. This second pass is necessary because some
	// predicates such as inter-pod affinity may not pass without the nominated pods.
	// If there are no nominated pods for the node or if the first run of the
	// predicates fail, we don't run the second pass.
	// We consider only equal or higher priority pods in the first pass, because
	// those are the current "pod" must yield to them and not take a space opened
	// for running them. It is ok if the current "pod" take resources freed for
	// lower priority pods.
	// Requiring that the new pod is schedulable in both circumstances ensures that
	// we are making a conservative decision: predicates like resources and inter-pod
	// anti-affinity are more likely to fail when the nominated pods are treated
	// as running, while predicates like pod affinity are more likely to fail when
	// the nominated pods are treated as not running. We can't just assume the
	// nominated pods are running because they are not running right now and in fact,
	// they may end up getting scheduled to a different node.
	for i := 0; i < 2; i++ {
		metaToUse := meta
		nodeInfoToUse := info
		if i == 0 {
			podsAdded, metaToUse, nodeInfoToUse = addNominatedPods(util.GetPodPriority(pod), meta, info, queue)
		} else if !podsAdded || len(failedPredicates) != 0 {
			break
		}
		// Bypass eCache if node has any nominated pods.
		// TODO(bsalamat): consider using eCache and adding proper eCache invalidations
		// when pods are nominated or their nominations change.
		eCacheAvailable = equivClass != nil && nodeCache != nil && !podsAdded
		for _, predicateKey := range predicates.Ordering() {
			var (
				fit     bool
				reasons []algorithm.PredicateFailureReason
				err     error
			)
			//TODO (yastij) : compute average predicate restrictiveness to export it as Prometheus metric
			if predicate, exist := predicateFuncs[predicateKey]; exist {
				if eCacheAvailable {
					fit, reasons, err = nodeCache.RunPredicate(predicate, predicateKey, pod, metaToUse, nodeInfoToUse, equivClass, cache)
				} else {
					fit, reasons, err = predicate(pod, metaToUse, nodeInfoToUse)
				}
				if err != nil {
					return false, []algorithm.PredicateFailureReason{}, err
				}

				if !fit {
					// eCache is available and valid, and predicates result is unfit, record the fail reasons
					failedPredicates = append(failedPredicates, reasons...)
					// if alwaysCheckAllPredicates is false, short circuit all predicates when one predicate fails.
					if !alwaysCheckAllPredicates {
						glog.V(5).Infoln("since alwaysCheckAllPredicates has not been set, the predicate " +
							"evaluation is short circuited and there are chances " +
							"of other predicates failing as well.")
						break
					}
				}
			}
		}
	}

	return len(failedPredicates) == 0, failedPredicates, nil
}

// PrioritizeNodes prioritizes the nodes by running the individual priority functions in parallel.
// Each priority function is expected to set a score of 0-10
// 0 is the lowest priority score (least preferred node) and 10 is the highest
// Each priority function can also have its own weight
// The node scores returned by the priority function are multiplied by the weights to get weighted scores
// All scores are finally combined (added) to get the total weighted scores of all nodes
func PrioritizeNodes(
	pod *v1.Pod,
	nodeNameToInfo map[string]*schedulercache.NodeInfo,
	meta interface{},
	priorityConfigs []algorithm.PriorityConfig,
	nodes []*v1.Node,
	extenders []algorithm.SchedulerExtender,
) (schedulerapi.HostPriorityList, error) {
	// If no priority configs are provided, then the EqualPriority function is applied
	// This is required to generate the priority list in the required format
	if len(priorityConfigs) == 0 && len(extenders) == 0 {
		result := make(schedulerapi.HostPriorityList, 0, len(nodes))
		for i := range nodes {
			hostPriority, err := EqualPriorityMap(pod, meta, nodeNameToInfo[nodes[i].Name])
			if err != nil {
				return nil, err
			}
			result = append(result, hostPriority)
		}
		return result, nil
	}

	var (
		mu   = sync.Mutex{}
		wg   = sync.WaitGroup{}
		errs []error
	)
	appendError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
	}

	results := make([]schedulerapi.HostPriorityList, len(priorityConfigs), len(priorityConfigs))

	for i, priorityConfig := range priorityConfigs {
		if priorityConfig.Function != nil {
			// DEPRECATED
			wg.Add(1)
			go func(index int, config algorithm.PriorityConfig) {
				defer wg.Done()
				var err error
				results[index], err = config.Function(pod, nodeNameToInfo, nodes)
				if err != nil {
					appendError(err)
				}
			}(i, priorityConfig)
		} else {
			results[i] = make(schedulerapi.HostPriorityList, len(nodes))
		}
	}
	processNode := func(index int) {
		nodeInfo := nodeNameToInfo[nodes[index].Name]
		var err error
		for i := range priorityConfigs {
			if priorityConfigs[i].Function != nil {
				continue
			}
			results[i][index], err = priorityConfigs[i].Map(pod, meta, nodeInfo)
			if err != nil {
				appendError(err)
				return
			}
		}
	}
	workqueue.Parallelize(16, len(nodes), processNode)
	for i, priorityConfig := range priorityConfigs {
		if priorityConfig.Reduce == nil {
			continue
		}
		wg.Add(1)
		go func(index int, config algorithm.PriorityConfig) {
			defer wg.Done()
			if err := config.Reduce(pod, meta, nodeNameToInfo, results[index]); err != nil {
				appendError(err)
			}
			if glog.V(10) {
				for _, hostPriority := range results[index] {
					glog.Infof("%v -> %v: %v, Score: (%d)", pod.Name, hostPriority.Host, config.Name, hostPriority.Score)
				}
			}
		}(i, priorityConfig)
	}
	// Wait for all computations to be finished.
	wg.Wait()
	if len(errs) != 0 {
		return schedulerapi.HostPriorityList{}, errors.NewAggregate(errs)
	}

	// Summarize all scores.
	result := make(schedulerapi.HostPriorityList, 0, len(nodes))

	for i := range nodes {
		result = append(result, schedulerapi.HostPriority{Host: nodes[i].Name, Score: 0})
		for j := range priorityConfigs {
			result[i].Score += results[j][i].Score * priorityConfigs[j].Weight
		}
	}

	if len(extenders) != 0 && nodes != nil {
		combinedScores := make(map[string]int, len(nodeNameToInfo))
		for _, extender := range extenders {
			if !extender.IsInterested(pod) {
				continue
			}
			wg.Add(1)
			go func(ext algorithm.SchedulerExtender) {
				defer wg.Done()
				prioritizedList, weight, err := ext.Prioritize(pod, nodes)
				if err != nil {
					// Prioritization errors from extender can be ignored, let k8s/other extenders determine the priorities
					return
				}
				mu.Lock()
				for i := range *prioritizedList {
					host, score := (*prioritizedList)[i].Host, (*prioritizedList)[i].Score
					combinedScores[host] += score * weight
				}
				mu.Unlock()
			}(extender)
		}
		// wait for all go routines to finish
		wg.Wait()
		for i := range result {
			result[i].Score += combinedScores[result[i].Host]
		}
	}

	if glog.V(10) {
		for i := range result {
			glog.V(10).Infof("Host %s => Score %d", result[i].Host, result[i].Score)
		}
	}
	return result, nil
}

// EqualPriorityMap is a prioritizer function that gives an equal weight of one to all nodes
func EqualPriorityMap(_ *v1.Pod, _ interface{}, nodeInfo *schedulercache.NodeInfo) (schedulerapi.HostPriority, error) {
	node := nodeInfo.Node()
	if node == nil {
		return schedulerapi.HostPriority{}, fmt.Errorf("node not found")
	}
	return schedulerapi.HostPriority{
		Host:  node.Name,
		Score: 1,
	}, nil
}

// pickOneNodeForPreemption chooses one node among the given nodes. It assumes
// pods in each map entry are ordered by decreasing priority.
// It picks a node based on the following criteria:
// 1. A node with minimum number of PDB violations.
// 2. A node with minimum highest priority victim is picked.
// 3. Ties are broken by sum of priorities of all victims.
// 4. If there are still ties, node with the minimum number of victims is picked.
// 5. If there are still ties, the first such node is picked (sort of randomly).
// The 'minNodes1' and 'minNodes2' are being reused here to save the memory
// allocation and garbage collection time.
func pickOneNodeForPreemption(nodesToVictims map[*v1.Node]*schedulerapi.Victims) *v1.Node {
	if len(nodesToVictims) == 0 {
		return nil
	}
	minNumPDBViolatingPods := math.MaxInt32
	var minNodes1 []*v1.Node
	lenNodes1 := 0
	for node, victims := range nodesToVictims {
		if len(victims.Pods) == 0 {
			// We found a node that doesn't need any preemption. Return it!
			// This should happen rarely when one or more pods are terminated between
			// the time that scheduler tries to schedule the pod and the time that
			// preemption logic tries to find nodes for preemption.
			return node
		}
		numPDBViolatingPods := victims.NumPDBViolations
		if numPDBViolatingPods < minNumPDBViolatingPods {
			minNumPDBViolatingPods = numPDBViolatingPods
			minNodes1 = nil
			lenNodes1 = 0
		}
		if numPDBViolatingPods == minNumPDBViolatingPods {
			minNodes1 = append(minNodes1, node)
			lenNodes1++
		}
	}
	if lenNodes1 == 1 {
		return minNodes1[0]
	}

	// There are more than one node with minimum number PDB violating pods. Find
	// the one with minimum highest priority victim.
	minHighestPriority := int32(math.MaxInt32)
	var minNodes2 = make([]*v1.Node, lenNodes1)
	lenNodes2 := 0
	for i := 0; i < lenNodes1; i++ {
		node := minNodes1[i]
		victims := nodesToVictims[node]
		// highestPodPriority is the highest priority among the victims on this node.
		highestPodPriority := util.GetPodPriority(victims.Pods[0])
		if highestPodPriority < minHighestPriority {
			minHighestPriority = highestPodPriority
			lenNodes2 = 0
		}
		if highestPodPriority == minHighestPriority {
			minNodes2[lenNodes2] = node
			lenNodes2++
		}
	}
	if lenNodes2 == 1 {
		return minNodes2[0]
	}

	// There are a few nodes with minimum highest priority victim. Find the
	// smallest sum of priorities.
	minSumPriorities := int64(math.MaxInt64)
	lenNodes1 = 0
	for i := 0; i < lenNodes2; i++ {
		var sumPriorities int64
		node := minNodes2[i]
		for _, pod := range nodesToVictims[node].Pods {
			// We add MaxInt32+1 to all priorities to make all of them >= 0. This is
			// needed so that a node with a few pods with negative priority is not
			// picked over a node with a smaller number of pods with the same negative
			// priority (and similar scenarios).
			sumPriorities += int64(util.GetPodPriority(pod)) + int64(math.MaxInt32+1)
		}
		if sumPriorities < minSumPriorities {
			minSumPriorities = sumPriorities
			lenNodes1 = 0
		}
		if sumPriorities == minSumPriorities {
			minNodes1[lenNodes1] = node
			lenNodes1++
		}
	}
	if lenNodes1 == 1 {
		return minNodes1[0]
	}

	// There are a few nodes with minimum highest priority victim and sum of priorities.
	// Find one with the minimum number of pods.
	minNumPods := math.MaxInt32
	lenNodes2 = 0
	for i := 0; i < lenNodes1; i++ {
		node := minNodes1[i]
		numPods := len(nodesToVictims[node].Pods)
		if numPods < minNumPods {
			minNumPods = numPods
			lenNodes2 = 0
		}
		if numPods == minNumPods {
			minNodes2[lenNodes2] = node
			lenNodes2++
		}
	}
	// At this point, even if there are more than one node with the same score,
	// return the first one.
	if lenNodes2 > 0 {
		return minNodes2[0]
	}
	glog.Errorf("Error in logic of node scoring for preemption. We should never reach here!")
	return nil
}

// selectNodesForPreemption finds all the nodes with possible victims for
// preemption in parallel.
func selectNodesForPreemption(pod *v1.Pod,
	nodeNameToInfo map[string]*schedulercache.NodeInfo,
	potentialNodes []*v1.Node,
	predicates map[string]algorithm.FitPredicate,
	metadataProducer algorithm.PredicateMetadataProducer,
	queue SchedulingQueue,
	pdbs []*policy.PodDisruptionBudget,
) (map[*v1.Node]*schedulerapi.Victims, error) {

	nodeToVictims := map[*v1.Node]*schedulerapi.Victims{}
	var resultLock sync.Mutex

	// We can use the same metadata producer for all nodes.
	meta := metadataProducer(pod, nodeNameToInfo)
	checkNode := func(i int) {
		nodeName := potentialNodes[i].Name
		var metaCopy algorithm.PredicateMetadata
		if meta != nil {
			metaCopy = meta.ShallowCopy()
		}
		pods, numPDBViolations, fits := selectVictimsOnNode(pod, metaCopy, nodeNameToInfo[nodeName], predicates, queue, pdbs)
		if fits {
			resultLock.Lock()
			victims := schedulerapi.Victims{
				Pods:             pods,
				NumPDBViolations: numPDBViolations,
			}
			nodeToVictims[potentialNodes[i]] = &victims
			resultLock.Unlock()
		}
	}
	workqueue.Parallelize(16, len(potentialNodes), checkNode)
	return nodeToVictims, nil
}

// filterPodsWithPDBViolation groups the given "pods" into two groups of "violatingPods"
// and "nonViolatingPods" based on whether their PDBs will be violated if they are
// preempted.
// This function is stable and does not change the order of received pods. So, if it
// receives a sorted list, grouping will preserve the order of the input list.
func filterPodsWithPDBViolation(pods []interface{}, pdbs []*policy.PodDisruptionBudget) (violatingPods, nonViolatingPods []*v1.Pod) {
	for _, obj := range pods {
		pod := obj.(*v1.Pod)
		pdbForPodIsViolated := false
		// A pod with no labels will not match any PDB. So, no need to check.
		if len(pod.Labels) != 0 {
			for _, pdb := range pdbs {
				if pdb.Namespace != pod.Namespace {
					continue
				}
				selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
				if err != nil {
					continue
				}
				// A PDB with a nil or empty selector matches nothing.
				if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
					continue
				}
				// We have found a matching PDB.
				if pdb.Status.PodDisruptionsAllowed <= 0 {
					pdbForPodIsViolated = true
					break
				}
			}
		}
		if pdbForPodIsViolated {
			violatingPods = append(violatingPods, pod)
		} else {
			nonViolatingPods = append(nonViolatingPods, pod)
		}
	}
	return violatingPods, nonViolatingPods
}

// selectVictimsOnNode finds minimum set of pods on the given node that should
// be preempted in order to make enough room for "pod" to be scheduled. The
// minimum set selected is subject to the constraint that a higher-priority pod
// is never preempted when a lower-priority pod could be (higher/lower relative
// to one another, not relative to the preemptor "pod").
// The algorithm first checks if the pod can be scheduled on the node when all the
// lower priority pods are gone. If so, it sorts all the lower priority pods by
// their priority and then puts them into two groups of those whose PodDisruptionBudget
// will be violated if preempted and other non-violating pods. Both groups are
// sorted by priority. It first tries to reprieve as many PDB violating pods as
// possible and then does them same for non-PDB-violating pods while checking
// that the "pod" can still fit on the node.
// NOTE: This function assumes that it is never called if "pod" cannot be scheduled
// due to pod affinity, node affinity, or node anti-affinity reasons. None of
// these predicates can be satisfied by removing more pods from the node.
func selectVictimsOnNode(
	pod *v1.Pod,
	meta algorithm.PredicateMetadata,
	nodeInfo *schedulercache.NodeInfo,
	fitPredicates map[string]algorithm.FitPredicate,
	queue SchedulingQueue,
	pdbs []*policy.PodDisruptionBudget,
) ([]*v1.Pod, int, bool) {
	potentialVictims := util.SortableList{CompFunc: util.HigherPriorityPod}
	nodeInfoCopy := nodeInfo.Clone()

	removePod := func(rp *v1.Pod) {
		nodeInfoCopy.RemovePod(rp)
		if meta != nil {
			meta.RemovePod(rp)
		}
	}
	addPod := func(ap *v1.Pod) {
		nodeInfoCopy.AddPod(ap)
		if meta != nil {
			meta.AddPod(ap, nodeInfoCopy)
		}
	}
	// As the first step, remove all the lower priority pods from the node and
	// check if the given pod can be scheduled.
	podPriority := util.GetPodPriority(pod)
	for _, p := range nodeInfoCopy.Pods() {
		if util.GetPodPriority(p) < podPriority {
			potentialVictims.Items = append(potentialVictims.Items, p)
			removePod(p)
		}
	}
	potentialVictims.Sort()
	// If the new pod does not fit after removing all the lower priority pods,
	// we are almost done and this node is not suitable for preemption. The only condition
	// that we should check is if the "pod" is failing to schedule due to pod affinity
	// failure.
	// TODO(bsalamat): Consider checking affinity to lower priority pods if feasible with reasonable performance.
	if fits, _, err := podFitsOnNode(pod, meta, nodeInfoCopy, fitPredicates, nil, nil, queue, false, nil); !fits {
		if err != nil {
			glog.Warningf("Encountered error while selecting victims on node %v: %v", nodeInfo.Node().Name, err)
		}
		return nil, 0, false
	}
	var victims []*v1.Pod
	numViolatingVictim := 0
	// Try to reprieve as many pods as possible. We first try to reprieve the PDB
	// violating victims and then other non-violating ones. In both cases, we start
	// from the highest priority victims.
	violatingVictims, nonViolatingVictims := filterPodsWithPDBViolation(potentialVictims.Items, pdbs)
	reprievePod := func(p *v1.Pod) bool {
		addPod(p)
		fits, _, _ := podFitsOnNode(pod, meta, nodeInfoCopy, fitPredicates, nil, nil, queue, false, nil)
		if !fits {
			removePod(p)
			victims = append(victims, p)
			glog.V(5).Infof("Pod %v is a potential preemption victim on node %v.", p.Name, nodeInfo.Node().Name)
		}
		return fits
	}
	for _, p := range violatingVictims {
		if !reprievePod(p) {
			numViolatingVictim++
		}
	}
	// Now we try to reprieve non-violating victims.
	for _, p := range nonViolatingVictims {
		reprievePod(p)
	}
	return victims, numViolatingVictim, true
}

// nodesWherePreemptionMightHelp returns a list of nodes with failed predicates
// that may be satisfied by removing pods from the node.
func nodesWherePreemptionMightHelp(nodes []*v1.Node, failedPredicatesMap FailedPredicateMap) []*v1.Node {
	potentialNodes := []*v1.Node{}
	for _, node := range nodes {
		unresolvableReasonExist := false
		failedPredicates, found := failedPredicatesMap[node.Name]
		// If we assume that scheduler looks at all nodes and populates the failedPredicateMap
		// (which is the case today), the !found case should never happen, but we'd prefer
		// to rely less on such assumptions in the code when checking does not impose
		// significant overhead.
		// Also, we currently assume all failures returned by extender as resolvable.
		for _, failedPredicate := range failedPredicates {
			switch failedPredicate {
			case
				predicates.ErrNodeSelectorNotMatch,
				predicates.ErrPodAffinityRulesNotMatch,
				predicates.ErrPodNotMatchHostName,
				predicates.ErrTaintsTolerationsNotMatch,
				predicates.ErrNodeLabelPresenceViolated,
				// Node conditions won't change when scheduler simulates removal of preemption victims.
				// So, it is pointless to try nodes that have not been able to host the pod due to node
				// conditions. These include ErrNodeNotReady, ErrNodeUnderPIDPressure, ErrNodeUnderMemoryPressure, ....
				predicates.ErrNodeNotReady,
				predicates.ErrNodeNetworkUnavailable,
				predicates.ErrNodeUnderDiskPressure,
				predicates.ErrNodeUnderPIDPressure,
				predicates.ErrNodeUnderMemoryPressure,
				predicates.ErrNodeOutOfDisk,
				predicates.ErrNodeUnschedulable,
				predicates.ErrNodeUnknownCondition,
				predicates.ErrVolumeZoneConflict,
				predicates.ErrVolumeNodeConflict,
				predicates.ErrVolumeBindConflict:
				unresolvableReasonExist = true
				break
			}
		}
		if !found || !unresolvableReasonExist {
			glog.V(3).Infof("Node %v is a potential node for preemption.", node.Name)
			potentialNodes = append(potentialNodes, node)
		}
	}
	return potentialNodes
}

// podEligibleToPreemptOthers determines whether this pod should be considered
// for preempting other pods or not. If this pod has already preempted other
// pods and those are in their graceful termination period, it shouldn't be
// considered for preemption.
// We look at the node that is nominated for this pod and as long as there are
// terminating pods on the node, we don't consider this for preempting more pods.
func podEligibleToPreemptOthers(pod *v1.Pod, nodeNameToInfo map[string]*schedulercache.NodeInfo) bool {
	nomNodeName := pod.Status.NominatedNodeName
	if len(nomNodeName) > 0 {
		if nodeInfo, found := nodeNameToInfo[nomNodeName]; found {
			for _, p := range nodeInfo.Pods() {
				if p.DeletionTimestamp != nil && util.GetPodPriority(p) < util.GetPodPriority(pod) {
					// There is a terminating pod on the nominated node.
					return false
				}
			}
		}
	}
	return true
}

// podPassesBasicChecks makes sanity checks on the pod if it can be scheduled.
func podPassesBasicChecks(pod *v1.Pod, pvcLister corelisters.PersistentVolumeClaimLister) error {
	// Check PVCs used by the pod
	namespace := pod.Namespace
	manifest := &(pod.Spec)
	for i := range manifest.Volumes {
		volume := &manifest.Volumes[i]
		if volume.PersistentVolumeClaim == nil {
			// Volume is not a PVC, ignore
			continue
		}
		pvcName := volume.PersistentVolumeClaim.ClaimName
		pvc, err := pvcLister.PersistentVolumeClaims(namespace).Get(pvcName)
		if err != nil {
			// The error has already enough context ("persistentvolumeclaim "myclaim" not found")
			return err
		}

		if pvc.DeletionTimestamp != nil {
			return fmt.Errorf("persistentvolumeclaim %q is being deleted", pvc.Name)
		}
	}

	return nil
}

// NewGenericScheduler creates a genericScheduler object.
func NewGenericScheduler(
	cache schedulercache.Cache,
	eCache *equivalence.Cache,
	podQueue SchedulingQueue,
	predicates map[string]algorithm.FitPredicate,
	predicateMetaProducer algorithm.PredicateMetadataProducer,
	prioritizers []algorithm.PriorityConfig,
	priorityMetaProducer algorithm.PriorityMetadataProducer,
	extenders []algorithm.SchedulerExtender,
	volumeBinder *volumebinder.VolumeBinder,
	pvcLister corelisters.PersistentVolumeClaimLister,
	alwaysCheckAllPredicates bool,
	disablePreemption bool,
	percentageOfNodesToScore int32,
	client clientset.Interface) algorithm.ScheduleAlgorithm {

	// numOfUsers := 3
	glog.Infof("version 1.11: 2018.10.29 4:00 EUROSYS1.3")
	// glog.Infof("Offline ENABLE_OFFLINE_SCHEDULER: %s", ENABLE_OFFLINE_SCHEDULER)
	glog.Infof("ENABLE_PROFILING %v", schedulercache.ENABLE_PROFILING)
	if schedulercache.ENABLE_ONLINE_SCHEDULER {
		glog.Infof("============= IRA =============")
		glog.Infof("IS_TEST: %v", schedulercache.IS_TEST)
		glog.Infof("QUEUED_UP_JOBS: %v", schedulercache.QUEUED_UP_JOBS)
		glog.Infof("NUM_USERS: %v", schedulercache.NUM_USERS)
		glog.Infof("SCHEDULER %s", schedulercache.SCHEDULER)
		glog.Infof("NUM_RESERVE_CPU_NODE %s", schedulercache.NUM_RESERVE_CPU_NODE)
		glog.Infof("ENABLE_MOCKUP_GPU %s", schedulercache.ENABLE_MOCKUP_GPU)
		if schedulercache.ENABLE_MOCKUP_GPU {
			glog.Infof("NUM_MOCKUP_GPUS_PER_NODE: %d", schedulercache.NUM_MOCKUP_GPUS_PER_NODE)
		}
		schedulercache.SynClusterInfo(make(map[string]*schedulercache.NodeInfo), nil) // tanle syn cluster info manually
		schedulercache.InitParameters()
	}

	// glog.Infof("CAPACITY %s", CAPACITY)
	// glog.Infof("namespaces %s", namespaces)
	// glog.Infof("numOfUsers %s", numOfUsers)
	// schedulercache.InitAllNameSpaces(numOfUsers)

	// n := len(namespaces)
	// shares := make([]*schedulercache.Resource, n)
	// switch SCHEDULER {
	// case ES:
	// 	shares = AllocateES(namespaces, CAPACITY)
	// case Static:
	// 	shares = AllocateStatic(namespaces, CAPACITY)
	// case AlloX:
	// 	shares = AllocateAlloX(namespaces, CAPACITY)
	// case NaiveDRF:
	// 	shares = AllocateNaiveDRF(namespaces, CAPACITY)
	// }
	// for _, username := range namespaces {
	// 	glog.Infof("%s's demand %s", username, getDemand(username))
	// 	glog.Infof("%s's traditional demand %s", username, getTraditionalDemand(username))
	// }
	// glog.Infof("shares %s", shares)

	return &genericScheduler{
		cache:                    cache,
		equivalenceCache:         eCache,
		schedulingQueue:          podQueue,
		predicates:               predicates,
		predicateMetaProducer:    predicateMetaProducer,
		prioritizers:             prioritizers,
		priorityMetaProducer:     priorityMetaProducer,
		extenders:                extenders,
		cachedNodeInfoMap:        make(map[string]*schedulercache.NodeInfo),
		volumeBinder:             volumeBinder,
		pvcLister:                pvcLister,
		alwaysCheckAllPredicates: alwaysCheckAllPredicates,
		disablePreemption:        disablePreemption,
		percentageOfNodesToScore: percentageOfNodesToScore,
		client: client,
	}
}
