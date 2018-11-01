/*
Copyright 2015 The Kubernetes Authors.

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

package cache

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

///// Implemenation for AlloX /////

/*************** IMPORTANT PARAMETERs *******************/
const SCHEDULER = ES
const AlphaFair = 0.5 //
const NUM_USERS = 4
const SCHEDULER_PERIOD = 100 // Miliseconds

const AVG_BETA = 95.0 * MILLI // based on simulator.

var ENABLE_PROFILING = true

var USING_FIFO = false

const NUM_RESERVE_CPU_NODE = 1 //for master
const MASTER_CPU_CORES = 20    //for master

var QUEUED_UP_JOBS = 40 //
var AlloX_DEBUG = true

// TODO: WORK AROUND  = break deadlock at begginning.
const GPU_CAP = 4
const CPU_CAP = 8*20000 + GPU_CAP*4000
const MEM_CAP = 64 * GI

const (
	ES       string = "ES"
	Static   string = "Static"   // offline
	NaiveDRF string = "NaiveDRF" // offline
	AlloX    string = "AlloX"
	DRF      string = "DRF"
	DRFFIFO  string = "DRFFIFO"
	DRFExt   string = "DRFExt"
	SJF      string = "SJF"
	// FS       string = "FS"
)

const NvidiaGPU = "nvidia.com/gpu"

type ProcessingTime struct {
	pod            *v1.Pod
	isGPU          bool
	processingTime int64
}

const IS_TEST = false
const ENABLE_JOB_SCHEDULING = true
const ENABLE_ONLINE_SCHEDULER = true
const ENABLE_OFFLINE_SCHEDULER = false
const ENABLE_MOCKUP_GPU = false
const NUM_MOCKUP_GPUS_PER_NODE = 1
const NUM_RESERVE_CPU = 0
const NUM_RESERVE_GPU = 0

const PROFILING_STR = "profiling"

var FairScoreMap = make(map[string]float64)
var DEBUG = true

// for flow scheduler
var AvailableTimes = make(map[int64]int64)
var ArrivalTimes = make(map[string]int64)
var MachinePodMap = make(map[string]int64)
var PodMachineMap = make(map[int64]string)
var NumOfNodes = 0
var NumOfCPUNodes = 0
var NumOfGPUNodes = 0
var CPU_DEMAND_PER_JOB = 20

var START_TIME = time.Now()
var ARRIVAL_TIME = 0

const SECOND = 1000000000

func InitAllNameSpaces(numOfUser int) {
	for i := 1; i <= numOfUser; i++ {
		FairScoreMap["user"+strconv.Itoa(i)] = 0.0
	}

}

// var CAPACITY = &Resource{MilliCPU: 0, NvidiaGPU: 0, Memory: 0}
var CAPACITY = &Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}

func UpdateFairScore(pod *v1.Pod, isStart bool) {

	if strings.Contains(pod.Name, PROFILING_STR) {
		return
	}

	isCpu := !IsGpuPod(pod)
	p1 := GetCpuComplTime(pod)
	p2 := GetGpuComplTime(pod)
	fairVal := 0.0
	if CAPACITY.MilliCPU == 0 {
		glog.Errorf("[tanle] ERROR CAPACITY has not updated yet")
		return
	}
	if p1 < p2 {
		c, _, m := GetDemand(pod, false)
		if isCpu {
			fairVal = math.Max(float64(c)/float64(CAPACITY.MilliCPU), float64(m)/float64(CAPACITY.Memory))
		} else {
			fairVal = float64(p1) / float64(p2) * math.Max(float64(c)/float64(CAPACITY.MilliCPU), float64(m)/float64(CAPACITY.Memory))
		}
	} else {
		_, g, m := GetDemand(pod, true)
		if isCpu {
			fairVal = float64(p2) / float64(p1) * math.Max(float64(g)/float64(CAPACITY.ScalarResources[NvidiaGPU]), float64(m)/float64(CAPACITY.Memory))
		} else {
			fairVal = math.Max(float64(g)/float64(CAPACITY.ScalarResources[NvidiaGPU]), float64(m)/float64(CAPACITY.Memory))
		}
	}

	// start a pod or finish a pod
	currTime := GetCurrentTime()
	if isStart {
		FairScoreMap[pod.Namespace] += fairVal
		processTime := int64(0)
		if IsGpuPod(pod) {
			processTime = GetGpuComplTime(pod)
		} else {
			processTime = GetCpuComplTime(pod)
		}
		AvailableTimes[MachinePodMap[pod.Name]] = currTime + processTime
	} else {
		FairScoreMap[pod.Namespace] -= fairVal
		AvailableTimes[MachinePodMap[pod.Name]] = GetCurrentTime() // if this machine is available.
		// delete pod from machine.
		machineId := MachinePodMap[pod.Name]
		delete(MachinePodMap, pod.Name)
		delete(PodMachineMap, machineId)
	}
	glog.Infof("[tanle] FairScoreMap:  %v", FairScoreMap)
}

type FIFO struct {
	*cache.FIFO
}

var schedPodQueue = FIFO{FIFO: cache.NewFIFO(cache.MetaNamespaceKeyFunc)}

func DoNotUseReserveResource(isGPU bool) bool {
	usage, capacity := GetResourceUsageAndCapacity()
	if isGPU {
		if capacity.ScalarResources[NvidiaGPU]-usage.ScalarResources[NvidiaGPU] <= NUM_RESERVE_GPU {
			return true
		}
	} else {
		if capacity.MilliCPU-usage.MilliCPU <= NUM_RESERVE_CPU*MILLI {
			return true
		}
	}
	return false
}

func schedPodQueueItems() string {
	str := ""
	i := 0
	allPods := schedPodQueue.List()
	for _, p := range allPods {
		str = str + "," + p.(*v1.Pod).Name
		i++
	}
	str = "schedPodQueue.size()=" + strconv.Itoa(i) + ":" + str
	return str
}

func AddToSchedPodQueue(pod *v1.Pod) {
	allPods := schedPodQueue.List()
	// isExist := false
	// var temp *v1.Pod
	for _, p := range allPods {
		if pod.Name == p.(*v1.Pod).Name {
			schedPodQueue.Delete(p)
			break
		}
	}
	schedPodQueue.AddIfNotPresent(pod)
}

func DeleteFromSchedPodQueue(pod *v1.Pod) {
	allPods := schedPodQueue.List()
	for _, p := range allPods {
		if pod.Name == p.(*v1.Pod).Name {
			schedPodQueue.Delete(p)
		}
	}
}

// tanle
func GetSecondaryDemand(pod *v1.Pod) (int64, int64, int64) {
	milliCPU := int64(0)
	gpu := int64(0)
	memInGi := int64(0)
	for _, container := range pod.Spec.Containers {
		// switch demands
		secDemand := container.Command[5]
		strDemands := strings.Split(secDemand, ",")
		cpuDemand, err := strconv.ParseInt(strings.TrimSpace(strDemands[0]), 10, 64)
		if err != nil {
			glog.Infof("Failed  to convert cpuDemand %s to int64", strDemands[0])
		}
		gpuDemand, err := strconv.ParseInt(strings.TrimSpace(strDemands[1]), 10, 64)
		if err != nil {
			glog.Infof("Failed to convert gpuDemand %s to int64", strDemands[1])
		}
		memory, err := strconv.ParseInt(strings.TrimSpace(strDemands[2]), 10, 64)
		if err != nil {
			glog.Infof("Failed to convert memory %s to int64", strDemands[2])
		}

		milliCPU += cpuDemand
		gpu += gpuDemand
		memInGi += memory
	}

	return milliCPU, gpu, memInGi
}
func GetPrimaryDemand(pod *v1.Pod) (int64, int64, int64) {
	milliCPU := int64(0)
	gpu := int64(0)
	memInGi := int64(0)
	for _, container := range pod.Spec.Containers {
		// switch demands
		secDemand := container.Command[4]
		strDemands := strings.Split(secDemand, ",")
		cpuDemand, err := strconv.ParseInt(strings.TrimSpace(strDemands[0]), 10, 64)
		if err != nil {
			glog.Infof("Failed  to convert cpuDemand %s to int64", strDemands[0])
		}
		gpuDemand, err := strconv.ParseInt(strings.TrimSpace(strDemands[1]), 10, 64)
		if err != nil {
			glog.Infof("Failed to convert gpuDemand %s to int64", strDemands[1])
		}
		memory, err := strconv.ParseInt(strings.TrimSpace(strDemands[2]), 10, 64)
		if err != nil {
			glog.Infof("Failed to convert memory %s to int64", strDemands[2])
		}

		milliCPU += cpuDemand
		gpu += gpuDemand
		memInGi += memory
	}

	return milliCPU, gpu, memInGi
}

var NodeNameToInfo = map[string]*NodeInfo{}
var nodes = []*v1.Node{}

func InitParameters() {
	InitAllNameSpaces(NUM_USERS)
	START_TIME = time.Now()
	switch SCHEDULER {
	case DRFFIFO:
		ENABLE_PROFILING = false
		USING_FIFO = true
	}
}

func SynClusterInfo(nn map[string]*NodeInfo, n []*v1.Node) {
	// glog.Infof("[tanle] SynClusterInfo %v/%v", CAPACITY)
	if nn != nil {
		NodeNameToInfo = nn
	}

	if n != nil {
		nodes = n
	}

	_, CAPACITY = GetResourceUsageAndCapacity()
}

func RequestedResources() (*Resource, *Resource) {
	requested := &Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}
	capacity := &Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}
	for _, node := range nodes {
		capTem := NodeNameToInfo[node.Name].AllocatableResource()
		pods := NodeNameToInfo[node.Name].Pods()
		for _, pod := range pods {
			iContainer := 0
			for _, container := range pod.Spec.Containers {
				requested = SumResource(requested, container.Resources.Requests, false)
				iContainer++
			}
		}
		capacity = sumRes(capacity, &capTem)
	}
	// glog.Infof("[tanle] RequestedResources: %v/%v", requested, capacity)
	return requested, capacity
}

func GetResourceUsageAndCapacity() (*Resource, *Resource) {
	usage := &Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}
	capacity := &Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}

	if len(nodes) == 0 || len(NodeNameToInfo) == 0 {
		if ENABLE_MOCKUP_GPU {
			return &Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}, &Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 1}}
		}
		// workaround: makesure there is GPU at begining.
		capacity = &Resource{MilliCPU: CPU_CAP, Memory: MEM_CAP, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: GPU_CAP}}
		glog.Infof("[tanle] WORK around capacity %v", capacity)
	} else {
		allPods := make([]*v1.Pod, 0)
		for _, node := range nodes {
			// count the scheduled pods.
			capTem := NodeNameToInfo[node.Name].AllocatableResource()
			// usageTemp := NodeNameToInfo[node.Name].RequestedResource()
			// glog.Infof("[tanle] RequestedResource %v", usageTemp)
			mockupGPUCap := int64(NUM_MOCKUP_GPUS_PER_NODE)

			pods := NodeNameToInfo[node.Name].Pods()
			for _, pod := range pods {
				// DeleteFromSchedPodQueue(pod) // workaround~~
				if !ContainsPod(allPods, pod) {
					allPods = append(allPods, pod)
					for _, container := range pod.Spec.Containers {
						usage = SumResource(usage, container.Resources.Requests, false)
						if ENABLE_MOCKUP_GPU && strings.Contains(container.Image, "gpu") {
							usage.ScalarResources[NvidiaGPU]++
							mockupGPUCap--
						}
					}
				}
			}

			// usage = sumRes(usage, &usageTemp)
			capacity = sumRes(capacity, &capTem)

			if ENABLE_MOCKUP_GPU {
				capacity.ScalarResources[NvidiaGPU] += mockupGPUCap
			}
		}
	}
	/*
		for _, item := range schedPodQueue.List() {
			pod := item.(*v1.Pod)
			if !ContainsPod(allPods, pod) {
				for _, container := range pod.Spec.Containers {
					usage = SumResource(usage, container.Resources.Requests, true)
					if ENABLE_MOCKUP_GPU && strings.Contains(container.Image, "gpu") {
						usage.ScalarResources[NvidiaGPU]++
						if ENABLE_MOCKUP_GPU {
							capacity.ScalarResources[NvidiaGPU]--
						}
					}
				}
			}
		}*/
	CAPACITY = capacity
	NumOfCPUNodes = int(int(capacity.MilliCPU) / MILLI / CPU_DEMAND_PER_JOB)
	NumOfGPUNodes = int(capacity.ScalarResources[NvidiaGPU])
	NumOfNodes = NumOfGPUNodes + NumOfCPUNodes - NUM_RESERVE_CPU_NODE
	return usage, capacity
}

// func updateAvailableTimes() int64 {
// 	iGpuNode := int64(0)
// 	iCpuNode := int64(NumOfGPUNodes)

// 	curTime := GetCurrentTime()
// 	for iM := 0; iM < NumOfNodes; iM++ {
// 		AvailableTimes[int64(iM)] = int64(curTime)
// 	}

// 	for _, node := range nodes {
// 		pods := NodeNameToInfo[node.Name].Pods()
// 		for _, pod := range pods {
// 			if !strings.Contains(pod.Namespace, "kube-system") {
// 				if IsGpuPod(pod) {
// 					AvailableTimes[iGpuNode] = GetGpuComplTime(pod)
// 					iGpuNode++
// 				} else {
// 					AvailableTimes[iCpuNode] = GetCpuComplTime(pod)
// 					iCpuNode++
// 				}
// 			}
// 		}
// 	}
// 	glog.Infof("[tanle] updateAvailableTimes() %v, %v/%v", AvailableTimes, NumOfGPUNodes, NumOfCPUNodes)
// 	return curTime
// }

func GetCurrentTime() int64 {
	return int64(time.Since(START_TIME) / SECOND)
}

func GetAllocatableResource() *Resource {
	result := &Resource{MilliCPU: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}, Memory: 0}
	for _, node := range nodes {
		// count the scheduled pods.
		nodeInfo := NodeNameToInfo[node.Name]
		temp := nodeInfo.AllocatableResource()
		result = sumRes(result, &temp)
	}
	return result
}

// GetResourceUsageByNamespace obatains the resource usage by namespace (user) //tanle
func GetResourceUsageByNamespace(ns string) *Resource {
	result, allPods := GetRealResourceUsageByNamespace(ns)

	if false {
		for _, item := range schedPodQueue.List() {
			pod := item.(*v1.Pod)
			if !ContainsPod(allPods, pod) {
				if pod.Namespace == ns {
					for _, container := range pod.Spec.Containers {
						result = SumResource(result, container.Resources.Requests, false)
					}
				}
			} else {
				DeleteFromSchedPodQueue(pod) //workaround
			}
		}
		// glog.Infof("[tanle] GetResourceUsageByNamespace %v:%v", ns, result)
	}

	return result
}

func ContainsPod(allPods []*v1.Pod, pod *v1.Pod) bool {
	for _, p := range allPods {
		if p.Name == pod.Name {
			return true
		}
	}
	return false
}

func DeletePodFromCache(p *v1.Pod) {
	for _, node := range nodes {
		pods := NodeNameToInfo[node.Name].Pods()
		// TODO: May not need the following lines
		for i := 0; i < len(pods); i++ {
			pod := pods[i]
			if pod.Name == p.Name {
				// count the scheduled pods.
				NodeNameToInfo[node.Name].RemovePod(pod)
				// newPods := NodeNameToInfo[node.Name].pods
				// newPods[i] = newPods[len(newPods)-1]
				// newPods = newPods[:len(newPods)-1]
				// NodeNameToInfo[node.Name].pods = newPods
				return
			}
		}
	}
}

func GetRealResourceUsageByNamespace(ns string) (*Resource, []*v1.Pod) {
	result := &Resource{MilliCPU: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}, Memory: 0}
	if len(nodes) == 0 || len(NodeNameToInfo) == 0 {
		return result, nil
	}

	allPods := make([]*v1.Pod, 0)
	for _, node := range nodes {
		// count the scheduled pods.
		pods := NodeNameToInfo[node.Name].Pods()
		for _, pod := range pods {
			if !ContainsPod(allPods, pod) {
				allPods = append(allPods, pod)
				if pod.Namespace == ns {
					for _, container := range pod.Spec.Containers {
						result = SumResource(result, container.Resources.Requests, false)
					}
				}
			}
		}
	}
	// glog.Infof("[tanle] GetRealResourceUsageByNamespace %v:%v", ns, result)
	return result, allPods
}

func Contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}
	_, ok := set[item]
	return ok
}

func GetAllNameSpaces() []string {
	namespaces := []string{"user1"}
	return namespaces
}

func GetNameSpaces() []string {
	namespaces := make([]string, 0)

	if len(nodes) == 0 || len(NodeNameToInfo) == 0 {
		return namespaces
	}

	for _, node := range nodes {
		// count the scheduled pods.
		pods := NodeNameToInfo[node.Name].Pods()
		for _, pod := range pods {
			if !Contains(namespaces, pod.Namespace) {
				namespaces = append(namespaces, pod.Namespace)
			}
		}
	}

	return namespaces
}

func GetActiveUsers(pods []*v1.Pod) []string {
	namespaces := GetNameSpaces()
	for _, pod := range pods {
		if !Contains(namespaces, pod.Namespace) {
			namespaces = append(namespaces, pod.Namespace)
		}
	}
	return namespaces
}

func GetQueueUsers(pods []*v1.Pod) []string {
	namespaces := make([]string, 0)
	for _, pod := range pods {
		if !Contains(namespaces, pod.Namespace) {
			namespaces = append(namespaces, pod.Namespace)
		}
	}
	return namespaces
}

func GetAllUsers() []string {
	namespaces := make([]string, 0)
	for i := 1; i <= NUM_USERS; i++ {
		namespaces = append(namespaces, "user"+strconv.Itoa(i))
	}
	return namespaces
}

// IRA_Add adds ResourceList into Resource but ignore the usage of CPU demand on GPU jobs.
func SumResource(result *Resource, rl v1.ResourceList, ignoreGPUcpu bool) *Resource {
	milliCPU := int64(0)
	nvidiaGPU := int64(0)
	memory := int64(0)
	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			milliCPU += rQuant.MilliValue()
		case v1.ResourceMemory:
			memory += rQuant.Value()
		case NvidiaGPU:
			nvidiaGPU += rQuant.Value()
			// default:
			// 	glog.Infof("[tanle] resource: %v=%v", rName, rQuant)
		}
	}
	result.ScalarResources[NvidiaGPU] += nvidiaGPU
	result.Memory += memory
	if ignoreGPUcpu {
		if nvidiaGPU == 0 { // ignore cpu usage of GPU jobs
			result.MilliCPU += milliCPU
		}
	} else {
		result.MilliCPU += milliCPU
	}
	return result
}

func sumRes(a *Resource, b *Resource) *Resource {
	result := &Resource{MilliCPU: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}, Memory: 0}
	result.ScalarResources[NvidiaGPU] = a.ScalarResources[NvidiaGPU] + b.ScalarResources[NvidiaGPU]
	result.MilliCPU = a.MilliCPU + b.MilliCPU
	result.Memory = a.Memory + b.Memory
	result.EphemeralStorage = a.EphemeralStorage + b.EphemeralStorage
	return result
}

func GetDemand(pod *v1.Pod, isGpu bool) (int64, int64, int64) {
	milliCPU := int64(0)
	gpu := int64(0)
	memInGi := int64(0)
	for _, container := range pod.Spec.Containers {
		cmdIdx := 4
		if isGpu && strings.Contains(container.Image, "gpu") {
			cmdIdx = 4 // primary
		} else {
			cmdIdx = 5 // secondary
		}

		// switch demands
		secDemand := container.Command[cmdIdx]

		strDemands := strings.Split(secDemand, ",")
		cpuDemand, err := strconv.ParseInt(strDemands[0], 10, 64)
		if err != nil {
			glog.Infof("Failed  to convert cpuDemand %s to int64", strDemands[0])
		}
		gpuDemand, err := strconv.ParseInt(strDemands[1], 10, 64)
		if err != nil {
			glog.Infof("Failed to convert gpuDemand %s to int64", strDemands[1])
		}
		memory, err := strconv.ParseInt(strDemands[2], 10, 64)
		if err != nil {
			glog.Infof("Failed to convert memory %s to int64", strDemands[2])
		}

		milliCPU += cpuDemand
		gpu += gpuDemand
		memInGi += memory
	}

	return milliCPU, gpu, memInGi
}

// tanle
func GetGpuComplTime(pod *v1.Pod) int64 {
	cmdIdx := 4
	for _, container := range pod.Spec.Containers {
		// switch demands
		if strings.Contains(container.Image, "gpu") {
			cmdIdx = 4 // primary
		} else {
			cmdIdx = 5 // secondary
		}
		if len(container.Command) < 5 {
			return -1
		}
		strDemands := strings.Split(container.Command[cmdIdx], ",")
		if len(strDemands) > 3 {
			str := strings.TrimSpace(strDemands[3])
			cTime, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				glog.Infof("Failed  to convert cpuDemand %s to int64", strDemands[3])
			}
			return cTime
		}
	}
	return -1
}

func GetShortestComplTime(pod *v1.Pod) (int64, bool) {
	isGPU := true
	cpuComplt := int64(GetCpuComplTime(pod))
	shortestComplt := int64(GetGpuComplTime(pod))
	if shortestComplt > cpuComplt {
		isGPU = false
		shortestComplt = cpuComplt
	}

	return shortestComplt, isGPU
}

func GetCpuComplTime(pod *v1.Pod) int64 {
	cmdIdx := 4
	for _, container := range pod.Spec.Containers {
		// switch demands
		if strings.Contains(container.Image, "cpu") {
			cmdIdx = 4 // primary
		} else {
			cmdIdx = 5 // secondary
		}
		if len(container.Command) < 5 {
			return -1
		}
		strDemands := strings.Split(container.Command[cmdIdx], ",")
		if len(strDemands) > 3 {
			str := strings.TrimSpace(strDemands[3])
			cTime, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				glog.Infof("Failed  to convert cpuDemand %s to int64", str)
			} else {
				return cTime
			}
		}
	}
	return -1
}

const MILLI = 1000
const GI = 1024 * 1024 * 1024

func IsGpuPod(pod *v1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		if strings.Contains(container.Image, "gpu") {
			return true
		}
	}
	return false
}

// func (g *genericScheduler) SubmitOnOtherDevice(pod *v1.Pod, replicatedPod *v1.Pod) {
// 	pod.Spec.Containers = replicatedPod.Spec.Containers

// 	// p.Client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{})
// 	if err := g.client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{}); err != nil {
// 		runtime.HandleError(fmt.Errorf("unable to DELETE pod in kubectl %T: %v", pod, err))
// 	}
// 	if _, err := g.client.CoreV1().Pods(replicatedPod.Namespace).Create(replicatedPod); err != nil {
// 		runtime.HandleError(fmt.Errorf("unable to CREATE pod in kubectl %T: %v", replicatedPod, err))
// 	}
// }

func SubmitOnOtherDevice(client clientset.Interface, pod *v1.Pod, replicatedPod *v1.Pod) {
	// pod.Spec.Containers = replicatedPod.Spec.Containers
	// foregroundDelete := metav1.DeletePropagationForeground
	// if err := client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{PropagationPolicy: &foregroundDelete}); err != nil {
	if err := client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{}); err != nil {
		runtime.HandleError(fmt.Errorf("unable to DELETE pod in kubectl %T: %v", pod, err))
	}
	if _, err := client.CoreV1().Pods(replicatedPod.Namespace).Create(replicatedPod); err != nil {
		runtime.HandleError(fmt.Errorf("unable to CREATE pod in kubectl %T: %v", replicatedPod, err))
	}
	// if p, err := client.CoreV1().Pods(replicatedPod.Namespace).Update(replicatedPod); err == nil {
	// 	pod = p
	// } else {
	// 	// this does notwork --> cannot update resource requests.
	// 	runtime.HandleError(fmt.Errorf("unable to Update pod in kubectl %T: %v", replicatedPod, err))
	// }
}

func CreatePodOnOtherDevice_bk(pod *v1.Pod, toBeGPU bool) (*v1.Pod, bool) {
	// check the device of the pod
	for _, container := range pod.Spec.Containers {
		if strings.Contains(container.Image, "gpu") && toBeGPU {
			return pod, false
		}
		if strings.Contains(container.Image, "cpu") && !toBeGPU {
			return pod, false
		}
	}

	// replicatedPod := pod.DeepCopy()
	for cName, container := range pod.Spec.Containers {
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

		cpuDemand, gpuDemand, memory := GetSecondaryDemand(pod)

		for rName := range container.Resources.Requests {
			quantity := container.Resources.Requests[rName]
			switch rName {
			case v1.ResourceCPU:
				quantity.SetMilli(cpuDemand)
			case NvidiaGPU:
				if ENABLE_MOCKUP_GPU {
					quantity.Set(0)
				} else {
					quantity.Set(gpuDemand)
				}
			case v1.ResourceMemory:
				quantity.Set(memory * GI)
			}
			container.Resources.Requests[rName] = quantity
			container.Resources.Limits[rName] = quantity
		}
		pod.Spec.Containers[cName] = container
	}

	// replicatedPod.ResourceVersion = ""
	// replicatedPod.Spec.NodeName = ""
	// replicatedPod.Annotations = nil
	// replicatedPod.ResourceVersion = pod.ResourceVersion
	// replicatedPod.Spec.NodeName = pod.Spec.NodeName
	// replicatedPod.Annotations = pod.Annotations

	return pod, true
}

func CreatePodOnOtherDevice(pod *v1.Pod, toBeGPU bool) (*v1.Pod, bool) {
	// check the device of the pod
	for _, container := range pod.Spec.Containers {
		if strings.Contains(container.Image, "gpu") && toBeGPU {
			return pod, false
		}
		if strings.Contains(container.Image, "cpu") && !toBeGPU {
			return pod, false
		}
	}

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

		cpuDemand, gpuDemand, memory := GetSecondaryDemand(pod)

		for rName := range container.Resources.Requests {
			quantity := container.Resources.Requests[rName]
			switch rName {
			case v1.ResourceCPU:
				quantity.SetMilli(cpuDemand)
			case NvidiaGPU:
				if ENABLE_MOCKUP_GPU {
					quantity.Set(0)
				} else {
					quantity.Set(gpuDemand)
				}
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
	// replicatedPod.ResourceVersion = pod.ResourceVersion
	// replicatedPod.Spec.NodeName = pod.Spec.NodeName
	// replicatedPod.Annotations = pod.Annotations

	return replicatedPod, true
}

func EqualShare(allPods []*v1.Pod, client clientset.Interface) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	users := GetQueueUsers(allPods)
	// glog.Infof("[tanle] active users: %v", users)
	// get resource usage on each user
	resourceMap := make(map[string]*Resource)
	for _, user := range users {
		resourceMap[user] = GetResourceUsageByNamespace(user)
	}

	var pod *v1.Pod
	shortestGPUComplt := int64(math.MaxInt64)
	isGPUAvaiable := false
	usage, capacity := GetResourceUsageAndCapacity()
	// glog.Infof("[tanle] GetResourceUsageAndCapacity usage/capacity %v/", usage, capacity)

	isGPUAvaiable = (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvaiable := (capacity.MilliCPU - usage.MilliCPU) > (NUM_RESERVE_CPU)*MILLI

	if isGPUAvaiable {
		gpuShare := capacity.ScalarResources[NvidiaGPU] / int64(NUM_USERS)
		var minUser string
		minGpuUsage := int64(math.MaxInt64)
		// pick the user with the least GPU
		for _, user := range users {
			gpuTemp := resourceMap[user].ScalarResources[NvidiaGPU]
			if minGpuUsage > gpuTemp {
				minUser = user
				minGpuUsage = gpuTemp
			}
		}

		// static allocation
		if minGpuUsage < gpuShare {
			// glog.Infof("[tanle] pick user: %v", minUser)
			// pick the pod with the shortest GPU complt.
			// pod := allPods[0]
			if USING_FIFO {
				for _, p := range allPods {
					if minUser == p.Namespace {
						pod = p
						// isGPU = IsGpuPod(pod)
						break
					}
				}
			} else {
				for _, p := range allPods {
					complTime := GetGpuComplTime(p)
					if minUser == p.Namespace && GetGpuComplTime(p) < shortestGPUComplt {
						pod = p
						shortestGPUComplt = complTime
					}
				}
			}
			repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			// glog.Infof("[tanle] pick pod: %v/%v on GPU: %v", repPod.Namespace, repPod.Name, IsGpuPod(repPod))
			return pod
		}

	}

	if isCPUAvaiable {
		cpuShare := (capacity.MilliCPU - (MASTER_CPU_CORES * MILLI)) / int64(NUM_USERS)
		roundCPUShare := cpuShare / int64(CPU_DEMAND_PER_JOB*1000)

		var minUser string
		minCpuUsage := int64(math.MaxInt64)
		// pick the user with the least CPU
		for _, user := range users {
			cpuTemp := resourceMap[user].MilliCPU
			if minCpuUsage > cpuTemp {
				minUser = user
				minCpuUsage = cpuTemp
			}
		}

		// static allocation
		if minCpuUsage/int64((CPU_DEMAND_PER_JOB-1)*1000) < roundCPUShare {
			// glog.Infof("[tanle] pick user: %v", minUser)
			// pick the pod with the shortest CPU complt.
			if USING_FIFO {
				for _, p := range allPods {
					if minUser == p.Namespace {
						pod = p
						// isGPU = IsGpuPod(pod)
						break
					}
				}
			} else {
				shortestCPUComplt := int64(math.MaxInt64)
				for _, p := range allPods {
					complTime := GetCpuComplTime(p)
					if minUser == p.Namespace && GetCpuComplTime(p) < shortestCPUComplt {
						pod = p
						shortestCPUComplt = complTime
					}
				}
			}
			repPod, isRep := CreatePodOnOtherDevice(pod, false)
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			// glog.Infof("[tanle] pick pod: %v/%v on CPU: %v", repPod.Namespace, repPod.Name, !IsGpuPod(repPod))
			return pod
		}
	}

	return nil
}

// EqualShare: ES
func OnlineEqualShare(allPods []*v1.Pod, client clientset.Interface) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	// glog.Infof("[tanle] active users: %v", activeUsers)
	// get resource usage on each user
	resourceMap := make(map[string]*Resource)
	for _, user := range activeUsers {
		resourceMap[user] = GetResourceUsageByNamespace(user)
		// resourceMap[user] = GetRealResourceUsageByNamespace(user)
		// glog.Infof("[tanle] resourceMap[%v]:%v", user, resourceMap[user])
	}

	var pod *v1.Pod
	shortestGPUComplt := int64(math.MaxInt64)
	isGPUAvaiable := false
	usage, capacity := GetResourceUsageAndCapacity()

	// do nothing if capacity is not syned.
	// if capacity.MilliCPU == 0 {
	// 	return nil
	// }

	isGPUAvaiable = (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvaiable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU*MILLI

	if isGPUAvaiable {
		var minUser string
		minGpuUsage := int64(math.MaxInt64)
		// pick the user with the least GPU
		for _, user := range activeUsers {
			gpuTemp := resourceMap[user].ScalarResources[NvidiaGPU]
			if minGpuUsage > gpuTemp {
				minUser = user
				minGpuUsage = gpuTemp
			}
		}
		// glog.Infof("[tanle] pick user: %v", minUser)
		// pick the pod with the shortest GPU complt.
		// pod := allPods[0]
		if USING_FIFO {
			for _, p := range allPods {
				if minUser == p.Namespace {
					pod = p
					// isGPU = IsGpuPod(pod)
					break
				}
			}
		} else {
			for _, p := range allPods {
				complTime := GetGpuComplTime(p)
				if minUser == p.Namespace && GetGpuComplTime(p) < shortestGPUComplt {
					pod = p
					shortestGPUComplt = complTime
				}
			}
		}
		repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
		if isRep {
			SubmitOnOtherDevice(client, pod, repPod)
			pod = repPod
		}
		// glog.Infof("[tanle] pick pod: %v/%v on GPU: %v", repPod.Namespace, repPod.Name, IsGpuPod(repPod))
		return pod
	} else if isCPUAvaiable {
		var minUser string
		minCpuUsage := int64(math.MaxInt64)
		// pick the user with the least CPU
		for _, user := range activeUsers {
			cpuTemp := resourceMap[user].MilliCPU
			if minCpuUsage > cpuTemp {
				minUser = user
				minCpuUsage = cpuTemp
			}
		}
		// glog.Infof("[tanle] pick user: %v", minUser)
		// pick the pod with the shortest CPU complt.
		if USING_FIFO {
			for _, p := range allPods {
				if minUser == p.Namespace {
					pod = p
					// isGPU = IsGpuPod(pod)
					break
				}
			}
		} else {
			shortestCPUComplt := int64(math.MaxInt64)
			for _, p := range allPods {
				complTime := GetCpuComplTime(p)
				if minUser == p.Namespace && GetCpuComplTime(p) < shortestCPUComplt {
					pod = p
					shortestCPUComplt = complTime
				}
			}
		}
		repPod, isRep := CreatePodOnOtherDevice(pod, false)
		if isRep {
			SubmitOnOtherDevice(client, pod, repPod)
			pod = repPod
		}
		// glog.Infof("[tanle] pick pod: %v/%v on CPU: %v", repPod.Namespace, repPod.Name, !IsGpuPod(repPod))
		return pod
	}

	return nil
}

func OnlineDRF(allPods []*v1.Pod, client clientset.Interface) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	_, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	if capacity.MilliCPU == 0 {
		glog.Infof("[tanle] OnlineDRF %v", capacity)
		return nil
	}
	// glog.Infof("[tanle] active users: %v", activeUsers)
	// get resource usage on each user
	resourceMap := make(map[string]*Resource)
	for _, user := range activeUsers {
		resourceMap[user] = GetResourceUsageByNamespace(user)
	}
	var minUser string
	minDominantShare := math.MaxFloat64

	// pick the user with the least Dorminant Share
	for _, user := range activeUsers {
		cpuTemp := 0.0
		gpuTemp := 0.0
		memTemp := 0.0
		if capacity.ScalarResources[NvidiaGPU] > 0 {
			gpuTemp = float64(resourceMap[user].ScalarResources[NvidiaGPU]) / float64(capacity.ScalarResources[NvidiaGPU])
		}
		if capacity.MilliCPU > 0 {
			cpuTemp = float64(resourceMap[user].MilliCPU) / float64(capacity.MilliCPU)
			memTemp = float64(resourceMap[user].Memory) / float64(capacity.Memory)
		}
		dominantShare := math.Max(math.Max(cpuTemp, gpuTemp), memTemp)
		if minDominantShare > dominantShare {
			minUser = user
			minDominantShare = dominantShare
		}
	}
	// glog.Infof("[tanle] pick user: %v", minUser)
	// pick the pod with the shortest GPU complt.
	// pod := allPods[0]
	isGPU := true
	var pod *v1.Pod
	if USING_FIFO {
		// Always put this pod on GPU.
		isGPU = true
		// match FIFO with the simulator :: WORKAROUND
		sort.SliceStable(allPods, func(i, j int) bool {
			pod1 := allPods[i]
			pod2 := allPods[j]
			if len(pod1.Name) != len(pod2.Name) {
				return len(pod1.Name) < len(pod2.Name)
			}
			return pod1.Name < pod2.Name
		})

		for _, p := range allPods {
			if minUser == p.Namespace {
				pod = p
				// _, isGPU = GetShortestComplTime(p)
				break
			}
		}
	} else {
		shortestComplt := int64(math.MaxInt64)
		// glog.Infof("[tanle] usage/capacity %v/%v", usage, capacity)
		complTime := shortestComplt
		for _, p := range allPods {
			isGPUTemp := true
			complTime, isGPUTemp = GetShortestComplTime(p)
			if minUser == p.Namespace && complTime < shortestComplt {
				pod = p
				shortestComplt = complTime
				isGPU = isGPUTemp
			}
		}
	}

	// glog.Infof("[tanle] pick pod: %v/%v isGPU: %v", pod.Namespace, pod.Name, IsGpuPod(pod))
	repPod, isRep := CreatePodOnOtherDevice(pod, isGPU) //put job on GPU
	if isRep {
		SubmitOnOtherDevice(client, pod, repPod)
		pod = repPod
	}

	return pod
}

func OnlineDRFExt(allPods []*v1.Pod, client clientset.Interface) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	usage, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	// if capacity.MilliCPU == 0 {
	// 	return nil
	// }
	// virtualCpuUsage := usage.MilliCPU + usage.NvidiaGPU*AVG_BETA
	virtualCPUCap := capacity.MilliCPU + capacity.ScalarResources[NvidiaGPU]*AVG_BETA
	// glog.Infof("[tanle] active users: %v", activeUsers)
	// get resource usage on each user
	resourceMap := make(map[string]*Resource)
	for _, user := range activeUsers {
		resourceMap[user] = GetResourceUsageByNamespace(user)
		// resourceMap[user] = GetRealResourceUsageByNamespace(user)
		// glog.Infof("[tanle] resourceMap[%v]:%v", user, resourceMap[user])
	}
	var minUser string
	minDominantShare := math.MaxFloat64

	// pick the user with the least Dorminant Share
	for _, user := range activeUsers {
		cpuTemp := float64(resourceMap[user].MilliCPU)
		gpuTemp := float64(resourceMap[user].ScalarResources[NvidiaGPU])
		memTemp := 0.0

		virtualCPU := 0.0
		if capacity.Memory > 0 {
			memTemp = float64(resourceMap[user].Memory) / float64(capacity.Memory)
			virtualCPU = (cpuTemp + gpuTemp*AVG_BETA) / float64(virtualCPUCap)
		}

		dominantShare := math.Max(virtualCPU, memTemp)
		if minDominantShare > dominantShare {
			minUser = user
			minDominantShare = dominantShare
		}
	}
	// glog.Infof("[tanle] pick user: %v", minUser)

	isGPUAvaiable := (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvaiable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU*MILLI
	var pod *v1.Pod
	if isGPUAvaiable {
		shortestGPUComplt := int64(math.MaxInt64)
		for _, p := range allPods {
			complTime := GetGpuComplTime(p)
			if minUser == p.Namespace && complTime < shortestGPUComplt {
				pod = p
				shortestGPUComplt = complTime
			}
		}
		repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
		if isRep {
			SubmitOnOtherDevice(client, pod, repPod)
			pod = repPod
		}
		// glog.Infof("[tanle] pick pod: %v/%v on GPU: %v", pod.Namespace, pod.Name, IsGpuPod(pod))
		return pod
	} else if isCPUAvaiable {
		shortestCPUComplt := int64(math.MaxInt64)
		for _, p := range allPods {
			complTime := GetCpuComplTime(p)
			if minUser == p.Namespace && complTime < shortestCPUComplt {
				pod = p
				shortestCPUComplt = complTime
			}
		}
		repPod, isRep := CreatePodOnOtherDevice(pod, false) //put job on CPU
		if isRep {
			SubmitOnOtherDevice(client, pod, repPod)
			pod = repPod
		}
		// glog.Infof("[tanle] pick pod: %v/%v on CPU: %v", pod.Namespace, pod.Name, !IsGpuPod(pod))
		return pod
	}

	return nil
}

var NEXT_ROUND = true
var MIN_USERS = make([]string, 0)

func IsProfilingJob(pod *v1.Pod) bool {
	if strings.Contains(pod.Name, PROFILING_STR) {
		return true
	}
	return false
}

func ProfilingSchedule(allPods []*v1.Pod) *v1.Pod {
	if !ENABLE_PROFILING {
		return nil
	}

	if len(allPods) > 0 {
		for _, pod := range allPods {
			if IsProfilingJob(pod) {
				return pod
			}
		}
	}
	return nil
}

func OnlineAlloX_pickUser(allPods []*v1.Pod, client clientset.Interface, alpha float64) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	if NEXT_ROUND {
		activeUsers := GetQueueUsers(allPods)
		nUsers := int(math.Ceil(alpha * float64(len(activeUsers))))
		if len(activeUsers) < nUsers {
			nUsers = len(activeUsers)
		}
		// sort active users based con fair score list
		sort.SliceStable(activeUsers, func(i, j int) bool {
			user1 := activeUsers[i]
			user2 := activeUsers[j]
			score1 := FairScoreMap[user1]
			score2 := FairScoreMap[user2]
			return score1 < score2
		})

		// pick the set of users with lowest fairscore
		minUsers := make([]string, 0)

		for i := 0; i < nUsers; i++ {
			minUsers = append(minUsers, activeUsers[i])
		}
		MIN_USERS = minUsers
		NEXT_ROUND = false
	}

	// glog.Infof("[tanle] allox picks users: %v", MIN_USERS)
	usage, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	if capacity.MilliCPU == 0 {
		return nil
	}
	isGPUAvaiable := (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvaiable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU*MILLI
	// isCPUAvaiable := GetAllocatableResource().MilliCPU > 0
	var pod *v1.Pod

	if !isGPUAvaiable && !isCPUAvaiable {
		NEXT_ROUND = true
		return pod
	}

	ProcessingTimes := []ProcessingTime{}
	for _, p := range allPods {
		for _, user := range MIN_USERS {
			if p.Namespace == user {
				gpuProcessingTime := ProcessingTime{p, true, GetGpuComplTime(p)}
				cpuProcessingTime := ProcessingTime{p, false, GetCpuComplTime(p)}
				ProcessingTimes = append(ProcessingTimes, gpuProcessingTime)
				ProcessingTimes = append(ProcessingTimes, cpuProcessingTime)
			}
		}
	}
	if len(ProcessingTimes) < 1 {
		NEXT_ROUND = true
		return pod
	}

	sort.SliceStable(ProcessingTimes, func(i, j int) bool {
		p1 := ProcessingTimes[i].processingTime
		p2 := ProcessingTimes[j].processingTime
		return p1 < p2
	})

	for _, pt := range ProcessingTimes {
		if pt.isGPU && isGPUAvaiable {
			pod = pt.pod
			repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			break
		} else if !pt.isGPU && isCPUAvaiable {
			pod = pt.pod
			repPod, isRep := CreatePodOnOtherDevice(pod, false) //put job on CPU
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			break
		}
	}
	// glog.Infof("[tanle] pick pod: %v/%v on isGPU: %v", pod.Namespace, pod.Name, IsGpuPod(pod))
	return pod
}

func OnlineSJF(allPods []*v1.Pod, client clientset.Interface, alpha float64) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	nUsers := int(math.Ceil(alpha * float64(len(activeUsers))))
	if len(activeUsers) < nUsers {
		nUsers = len(activeUsers)
	}
	// sort active users based con fair score list
	sort.SliceStable(activeUsers, func(i, j int) bool {
		user1 := activeUsers[i]
		user2 := activeUsers[j]
		score1 := FairScoreMap[user1]
		score2 := FairScoreMap[user2]
		return score1 < score2
	})

	usage, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	if capacity.MilliCPU == 0 {
		return nil
	}
	// pick the set of users with lowest fairscore
	minUsers := make([]string, 0)

	for i := 0; i < nUsers; i++ {
		minUsers = append(minUsers, activeUsers[i])
	}
	// glog.Infof("[tanle] FairScoreMap:  %v", FairScoreMap)
	// glog.Infof("[tanle] allox picks users: %v", minUsers)
	isGPUAvaiable := (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvaiable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU*MILLI
	// isCPUAvaiable := GetAllocatableResource().MilliCPU > 0
	var pod *v1.Pod

	ProcessingTimes := []ProcessingTime{}
	for _, p := range allPods {
		for _, user := range minUsers {
			if p.Namespace == user {
				gpuProcessingTime := ProcessingTime{p, true, GetGpuComplTime(p)}
				cpuProcessingTime := ProcessingTime{p, false, GetCpuComplTime(p)}
				ProcessingTimes = append(ProcessingTimes, gpuProcessingTime)
				ProcessingTimes = append(ProcessingTimes, cpuProcessingTime)
			}
		}
	}
	if len(ProcessingTimes) < 1 {
		return pod
	}
	sort.SliceStable(ProcessingTimes, func(i, j int) bool {
		p1 := ProcessingTimes[i].processingTime
		p2 := ProcessingTimes[j].processingTime
		return p1 < p2
	})

	for _, pt := range ProcessingTimes {
		if pt.isGPU && isGPUAvaiable {
			pod = pt.pod
			repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			break
		} else if !pt.isGPU && isCPUAvaiable {
			pod = pt.pod
			repPod, isRep := CreatePodOnOtherDevice(pod, false) //put job on CPU
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			break
		}
	}
	// glog.Infof("[tanle] pick pod: %v/%v on isGPU: %v", pod.Namespace, pod.Name, IsGpuPod(pod))
	return pod
}

func OnlineAllox(allPods []*v1.Pod, client clientset.Interface, alpha float64) *v1.Pod {
	_, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	if capacity.MilliCPU == 0 {
		glog.Errorf("[tanle] OnlineAlloX: %v ==> deadlock", capacity)
		return nil
	}

	numAvailMachines := 0
	for iM := NumOfNodes - 1; iM >= 0; iM-- {
		_, busy := PodMachineMap[int64(iM)]
		if !busy {
			numAvailMachines++
		}
	}
	if numAvailMachines == 0 {
		return nil
	}

	// do nothing if no available machines.

	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	// nUsers := int(math.Ceil(alpha * float64(len(activeUsers))))
	nUsers := int(math.Ceil(alpha * NUM_USERS))
	nUsers = int(math.Min(float64(nUsers), float64(len(activeUsers))))
	nJobs := len(allPods)
	if len(activeUsers) < nUsers {
		nUsers = len(activeUsers)
	}
	// glog.Infof("[tanle] FairScoreMap:  %v", FairScoreMap)
	// sort active users based con fair score list
	sort.SliceStable(activeUsers, func(i, j int) bool {
		user1 := activeUsers[i]
		user2 := activeUsers[j]
		score1 := FairScoreMap[user1]
		score2 := FairScoreMap[user2]
		return score1 < score2
	})

	// pick the set of users with lowest fairscore
	minUsers := make([]string, 0)

	for i := 0; i < nUsers; i++ {
		minUsers = append(minUsers, activeUsers[i])
	}

	// glog.Infof("[tanle] allox picks users: %v with alpha %v", minUsers, alpha)
	// isGPUAvaiable := (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	// isCPUAvaiable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU
	// isCPUAvaiable := GetAllocatableResource().MilliCPU > 0
	var pod *v1.Pod
	// step 1: update AvailableTimes

	currentTime := GetCurrentTime()
	for iM, aTime := range AvailableTimes {
		if aTime < currentTime {
			AvailableTimes[iM] = currentTime
		}
	}

	// step 2: create D, P, and Q
	D := make([][]int, NumOfNodes)
	P := make([][]int, NumOfNodes)
	for i := 0; i < NumOfNodes; i++ {
		D[i] = make([]int, nJobs)
		P[i] = make([]int, nJobs)
		for j := 0; j < nJobs; j++ {
			D[i][j] = 0
			temp := int(AvailableTimes[int64(i)]) - int(ArrivalTimes[allPods[j].Name]) // TODO: use arrival time of a job instead of START_TIME.
			if temp > 0 {
				D[i][j] = temp
			}
			if i < NumOfGPUNodes {
				P[i][j] = int(GetGpuComplTime(allPods[j]))
			} else {
				P[i][j] = int(GetCpuComplTime(allPods[j]))
			}
		}
	}
	// create matrix Q

	Q := make([][]int, NumOfNodes*nJobs)
	for i := 0; i < NumOfNodes*nJobs; i++ {
		Q[i] = make([]int, nJobs)
		for j := 0; j < nJobs; j++ {
			iN := i/NumOfNodes + 1
			iM := i % NumOfNodes
			Q[i][j] = D[iM][j] + iN*P[iM][j]
		}
	}
	// step 3: solve HungarianAlgorithm
	if AlloX_DEBUG {
		glog.Infof("input  D: %v", D)
		glog.Infof("input  P: %v", P)
	}
	sols, _ := schedutil.Solve(Q)
	if AlloX_DEBUG {
		glog.Infof("input  sols: %v", sols)
		AlloX_DEBUG = false
	}
	if len(sols) < 1 {
		glog.Errorf("P: %v", P)
		glog.Errorf("Delay matrix: %v", D)
		glog.Infof("input  AvailableTimes: %v", AvailableTimes)
		glog.Infof("input  ArrivalTimes: %v", ArrivalTimes)
		glog.Infof("input  Q: %v", Q)
		glog.Infof("solution: %v", sols)
		return nil
	}
	// step 4: find the pod to be scheduled
	for iM := NumOfNodes - 1; iM >= 0; iM-- {
		for k := nJobs - 1; k >= 0; k-- {
			j := sols[k*NumOfNodes+iM]
			// check if the machine is available
			_, busy := PodMachineMap[int64(iM)]
			if j >= 0 && !busy {
				if iM < NumOfGPUNodes {
					pod = allPods[j]
					repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
					if isRep {
						SubmitOnOtherDevice(client, pod, repPod)
						pod = repPod
					}
					// glog.Infof("[tanle] pick pod: %v/%v on GPU: %v on node %v in %v seconds", pod.Namespace, pod.Name, IsGpuPod(pod), iM, GetGpuComplTime(pod))
					MachinePodMap[pod.Name] = int64(iM)
					PodMachineMap[int64(iM)] = pod.Name
					return pod
				} else {
					pod = allPods[j]
					repPod, isRep := CreatePodOnOtherDevice(pod, false) //put job on CPU
					if isRep {
						SubmitOnOtherDevice(client, pod, repPod)
						pod = repPod
					}
					// glog.Infof("[tanle] pick pod: %v/%v on CPU: %v on node %v in %v seconds", pod.Namespace, pod.Name, !IsGpuPod(pod), iM, GetCpuComplTime(pod))
					MachinePodMap[pod.Name] = int64(iM)
					PodMachineMap[int64(iM)] = pod.Name
					return pod
				}
			}
		}
	}

	return nil
}

// if isGPUAvaiable {
// 	shortestGPUComplt := int64(math.MaxInt64)
// 	for _, p := range allPods {
// 		complTime, iGPU := GetShortestComplTime(p)
// 		if iGPU && Contains(minUsers, p.Namespace) && complTime < shortestGPUComplt {
// 			pod = p
// 			shortestGPUComplt = complTime
// 		}
// 	}
// 	repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
// 	if isRep {
// 		SubmitOnOtherDevice(client, pod, repPod)
// 		pod = repPod
// 	}
// 	glog.Infof("[tanle] pick pod: %v/%v on GPU: %v", pod.Namespace, pod.Name, IsGpuPod(pod))
// } else {
// 	shortestCPUComplt := int64(math.MaxInt64)
// 	for _, p := range allPods {
// 		complTime := GetCpuComplTime(p)
// 		if Contains(minUsers, p.Namespace) && complTime < shortestCPUComplt {
// 			pod = p
// 			shortestCPUComplt = complTime
// 		}
// 	}
// 	repPod, isRep := CreatePodOnOtherDevice(pod, false) //put job on CPU
// 	if isRep {
// 		SubmitOnOtherDevice(client, pod, repPod)
// 		pod = repPod
// 	}
// 	glog.Infof("[tanle] pick pod: %v/%v on CPU: %v", pod.Namespace, pod.Name, !IsGpuPod(pod))
// }

// CreateNodeNameToInfoMap obtains a list of pods and pivots that list into a map where the keys are node names
// and the values are the aggregated information for that node.
func CreateNodeNameToInfoMap(pods []*v1.Pod, nodes []*v1.Node) map[string]*NodeInfo {
	NodeNameToInfo := make(map[string]*NodeInfo)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if _, ok := NodeNameToInfo[nodeName]; !ok {
			NodeNameToInfo[nodeName] = NewNodeInfo()
		}
		NodeNameToInfo[nodeName].AddPod(pod)
	}
	imageExistenceMap := createImageExistenceMap(nodes)

	for _, node := range nodes {
		if _, ok := NodeNameToInfo[node.Name]; !ok {
			NodeNameToInfo[node.Name] = NewNodeInfo()
		}
		nodeInfo := NodeNameToInfo[node.Name]
		nodeInfo.SetNode(node)
		nodeInfo.imageStates = getNodeImageStates(node, imageExistenceMap)
	}
	return NodeNameToInfo
}

// getNodeImageStates returns the given node's image states based on the given imageExistence map.
func getNodeImageStates(node *v1.Node, imageExistenceMap map[string]sets.String) map[string]*ImageStateSummary {
	imageStates := make(map[string]*ImageStateSummary)

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			imageStates[name] = &ImageStateSummary{
				Size:     image.SizeBytes,
				NumNodes: len(imageExistenceMap[name]),
			}
		}
	}
	return imageStates
}

// createImageExistenceMap returns a map recording on which nodes the images exist, keyed by the images' names.
func createImageExistenceMap(nodes []*v1.Node) map[string]sets.String {
	imageExistenceMap := make(map[string]sets.String)
	for _, node := range nodes {
		for _, image := range node.Status.Images {
			for _, name := range image.Names {
				if _, ok := imageExistenceMap[name]; !ok {
					imageExistenceMap[name] = sets.NewString(node.Name)
				} else {
					imageExistenceMap[name].Insert(node.Name)
				}
			}
		}
	}
	return imageExistenceMap
}
