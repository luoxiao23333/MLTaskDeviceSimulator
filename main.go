package main

import (
	"Device/utils"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MCMOTVideoPath = "MCMOT/input.avi"
const MCMOTVideoName = "input.avi"

const slamVideoPath = "slam/input.mp4"
const slamVideoName = "input.mp4"

const fusionImageDir = "fusion/data/00/"
const fusionImageName = "input.png"

const detImageDir = "det/data/00/"
const detImageName = "input.png"

const completTaskImageDir = "complete_task/data/00/"
const complteTaskImageName = "input.png"

const clusterURL = "http://192.168.1.101:8081"
const newTaskURL = clusterURL + "/new_task"
const completeTaskURL = clusterURL + "/complete_task"

const receivePort = ":8080"

// map[taskID]chan bool
var fusionLockMap sync.Map
var detLockMap sync.Map
var completTaskLoackMap sync.Map

type MetricsStat struct {
	CPULimit       string        `csv:"CPULimit"`
	GpuLimit       string        `csv:"GPULimit"`
	AvgLatency     time.Duration `csv:"AvgLatency"`
	AvgCPUUsage    float64       `csv:"AvgCPUUsage"`
	MaxCPUUsage    int64         `csv:"MaxCPUUsage"`
	AvgMemoryUsage float64       `csv:"AvgMemoryUsage"`
	MaxMemoryUsage int64         `csv:"MaxMemoryUsage"`
	HighLoadRatio  float64       `csv:"HighLoadRatio"`
	TaskNumber     int           `csv:"TaskNumber"`
}

var waitGroup sync.WaitGroup

type CreateInfo struct {
	CpuLimits     map[string]int `json:"cpu_limit"`
	WorkerNumbers map[string]int `json:"worker_numbers"`
	TaskName      string         `json:"task_name"`
	GpuLimits     map[string]int `json:"gpu_limit"`
	GpuMemory     map[string]int `json:"gpu_memory"`
}

type router struct{}

func (r *router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/mcmot":
		MCMOTFinished(w, req)
	case "/slam":
		slamFinished(w, req)
	case "/fusion":
		fusionFinished(w, req)
	case "/det":
		detFinished(w, req)
	case "/complete_task":
		complteTaskFinish(w, req)
	// 其他路由处理函数
	default:
		http.NotFound(w, req)
	}
}

func main() {
	go func() {
		/*
			http.HandleFunc("/mcmot", MCMOTFinished)
			http.HandleFunc("/slam", slamFinished)
			http.HandleFunc("/fusion", fusionFinished)
			err := http.ListenAndServe(receivePort, nil)
			if err != nil {
				log.Panic(err)
			}*/

		server := &http.Server{
			Addr:         receivePort,
			ReadTimeout:  1 * time.Minute,
			WriteTimeout: 1 * time.Minute,
			IdleTimeout:  1 * time.Minute,
			Handler:      &router{},
		}

		if err := server.ListenAndServe(); err != nil {
			log.Panicf("listen: %s\n", err)
		}
	}()

	/*
		var nodeList = []string{"controller", "as1"}
		coreMap := map[string]float64{
			"controller": 32,
			"as1":        16,
		}
	*/

	restartScheduler()
	warmUp()
	var nodeList = []string{"gpu1", "gpu1", "gpu1"}
	var gpuLimits = []int{33, 50, 100}
	var taskNumbers = []int{3, 2, 1}
	testDET(nodeList, gpuLimits, taskNumbers)

	//testCompleteTaskForAllConfig()

}

func testCompleteTaskForAllConfig() {
	var sumMetrics []CompleteMetrics

	restartScheduler()
	time.Sleep(5 * time.Second)

	warmUp()
	restartScheduler()

	enableTestDifferentTaskNumber := false

	if enableTestDifferentTaskNumber {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			testCompleteTask("controller", 0, 50, false)
			wg.Done()
		}()
		testCompleteTask("controller", 0, 50, true)
		wg.Wait()

		wg = sync.WaitGroup{}
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				testCompleteTask("controller", 0, 25, false)
				wg.Done()
			}()
		}
		testCompleteTask("controller", 0, 50, true)
		wg.Wait()
	}

	createRange := func(lower int, upper int, step int) []int {
		upper += step
		nums := make([]int, (upper-lower)/step)
		for i := range nums {
			nums[i] = lower + step*i
		}
		return nums
	}

	cpuLimits := map[string][]int{
		"controller": createRange(1000, 5000, 200),
		"as1":        createRange(400, 3000, 200),
	}

	workerNumbers := map[int]int{
		25: 4, 33: 3, 50: 2, 100: 1,
	}

	for _, fusionNodeName := range []string{"controller", "as1"} {
		for _, gpuLimit := range []int{33, 50, 100} {
			for _, cpuLimit := range cpuLimits[fusionNodeName] {

				log.Printf("for cpu[%v] gpu[%v] fusion node[%v]",
					cpuLimit, gpuLimit, fusionNodeName)

			startPoint:
				wg := sync.WaitGroup{}
				workerNumber := workerNumbers[gpuLimit]
				wg.Add(workerNumber)
				for i := 0; i < workerNumber-1; i++ {
					go func() {
						testCompleteTask(fusionNodeName, cpuLimit, gpuLimit, false)
						wg.Done()
					}()
				}

				go func() {
					metrics := testCompleteTask(fusionNodeName, cpuLimit, gpuLimit, true)
					sumMetrics = append(sumMetrics, metrics)
					wg.Done()
				}()

				done := make(chan bool, 1)
				go func() {
					wg.Wait()
					done <- true
				}()

				select {
				case <-done:
					log.Printf("Task Done")
				case <-time.After(300 * time.Second):
					close(done)
					log.Printf("Timeout! Restart and Warm Up Again")
					restartScheduler()
					warmUp()
					restartScheduler()
					goto startPoint
				}

				time.Sleep(5 * time.Second)
			}
			restartScheduler()
			time.Sleep(20 * time.Second)
		}

		file, err := os.Create(fmt.Sprintf("complete_task/sum_metrics_%v.csv", fusionNodeName))
		if err != nil {
			log.Panic(err)
		}

		_, err = file.Write(utils.MarshalCSV(sumMetrics))
		if err != nil {
			log.Panic(err)
		}

		if err = file.Close(); err != nil {
			log.Panic(err)
		}
	}

	/*
		for _, fusionNodeName := range []string{"as1"} {
			for cpuLimit := 5000; cpuLimit <= 5000; cpuLimit += 100 {
				for _, gpuLimit := range []int{16, 20, 25, 33, 50, 100} {
					log.Printf("for cpu[%v] gpu[%v] fusion node[%v]",
						cpuLimit, gpuLimit, fusionNodeName)
					metrics := testCompleteTask(fusionNodeName, cpuLimit, gpuLimit)
					sumMetrics = append(sumMetrics, metrics)
					time.Sleep(10 * time.Second)
				}
			}
		}
	*/
}

func warmUp() {
	for warmUpIter := 0; warmUpIter < 2; warmUpIter++ {
	warmUpPoint:
		fmt.Printf("\r warm up %v/%v", warmUpIter+1, 2)
		wg := sync.WaitGroup{}
		wg.Add(3)
		for i := 0; i < 3; i++ {
			go func() {
				testCompleteTask("as1", 0, 33, false)
				wg.Done()
			}()
		}
		done := make(chan bool, 1)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			continue
		case <-time.After(300 * time.Second):
			close(done)
			log.Printf("Timeout in warm up! Restart and Warm Up Again")
			restartScheduler()
			goto warmUpPoint
		}
	}
	fmt.Println()
}

func restartScheduler() {
	cmd := exec.Command("kubectl", "delete", "pod", "--all")

	// 设置环境变量
	env := []string{"KUBECONFIG=/etc/kubernetes/admin.conf"}
	cmd.Env = append(cmd.Env, env...)

	// 执行命令
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println(out.String())
	log.Println("Restart Done!. Wait for 10 secs")
	time.Sleep(10 * time.Second)
}

func testDET(nodeList []string, gpuLimits, taskNumbers []int) {

	const imageLength = 465
	var metricsMap = make(map[int]MetricsStat)

	for index, nodeName := range nodeList {
		gpuLimit := gpuLimits[index]
		taskNumber := taskNumbers[index]
		detailMetricsMap := make(map[string]*ResourceUsage)
		log.Printf("creating pod... wait for det start up\n")
		createGPUWorkers(gpuLimit, nodeName, taskNumber, "det")
		waitTime := int(time.Second) * 60
		log.Printf("Wait for extra %v", time.Duration(waitTime))
		time.Sleep(time.Duration(waitTime))
		taskWG := sync.WaitGroup{}
		taskWG.Add(taskNumber)
		taskIDs := make([]string, taskNumber)
		for i := range taskIDs {
			taskIDs[i] = "-1"
		}
		var latencies []time.Duration
		for i := 0; i < taskNumber; i++ {
			go func(i int) {
				startTime := time.Now()
				queryTime := time.Now()
				eachFrameTime := time.Now()
				for imageIndex := 0; imageIndex < imageLength; imageIndex++ {
					var status string
					if taskIDs[i] == "-1" {
						status = "Begin"
					} else if imageIndex == imageLength-1 {
						status = "Last"
						notifier, _ := detLockMap.Load(taskIDs[i])
						<-notifier.(chan bool)
						if i == taskNumber-1 {
							latencies = append(latencies, time.Since(eachFrameTime))
						}
					} else {
						status = "Running"
						notifier, _ := detLockMap.LoadOrStore(taskIDs[i], make(chan bool))
						<-notifier.(chan bool)
						if i == taskNumber-1 {
							latencies = append(latencies, time.Since(eachFrameTime))
						}
					}

					duration := time.Since(startTime)
					needToWait := time.Duration(int64(float64(imageIndex)*(1000./30.))) * time.Millisecond
					if duration < needToWait {
						diff := needToWait - duration
						log.Printf("Wait %v", time.Duration(diff))
						time.Sleep(time.Duration(diff))
					}

					imagePath := detImageDir + utils.IntToNDigitsString(imageIndex, 6) + ".png"
					taskIDs[i] = sendDETRequest(nodeName, imagePath, status, taskIDs[i])
					eachFrameTime = time.Now()

					if status == "Running" {
						queryDuration := time.Since(queryTime)
						if i == taskNumber-1 && queryDuration.Seconds() >= 2 {
							//usage := queryMetrics(taskIDs[i])
							//detailMetricsMap[usage.CollectedTime] = usage
							queryTime = time.Now()
						}
					} else if status == "Last" {
						notifier, _ := detLockMap.Load(taskIDs[i])
						<-notifier.(chan bool)
						detLockMap.Delete(taskIDs[i])
					}

				}
				taskWG.Done()
			}(i)
		}
		taskWG.Wait()

		avgLatency := 0.
		for _, l := range latencies {
			avgLatency += float64(l.Milliseconds())
		}
		avgLatency = avgLatency / float64(len(latencies))

		log.Printf("Avg latency is %v ms for gpu limits: %v", avgLatency, gpuLimit)
		var maxCPU int64 = 0
		var maxMem int64 = 0
		var detailResults []ResourceUsage
		for _, item := range detailMetricsMap {
			if item.CPU > maxCPU {
				maxCPU = item.CPU
			}
			if item.Memory > maxMem {
				maxMem = item.Memory
			}
			detailResults = append(detailResults, *item)
		}

		file, err := os.Create(fmt.Sprintf("det/detail_metrics/%v_%v_%v.csv",
			"det", nodeName, gpuLimit))
		if err != nil {
			log.Panic(err)
		}

		_, err = file.Write(utils.MarshalCSV(detailResults))
		if err != nil {
			log.Panic(err)
		}

		if err = file.Close(); err != nil {
			log.Panic(err)
		}

		file, err = os.Create(fmt.Sprintf("det/detail_metrics/latencies_%v_%v_%v.csv",
			"det", nodeName, gpuLimit))
		if err != nil {
			log.Panic(err)
		}

		var latenciesString string
		for _, latency := range latencies {
			latenciesString = latenciesString + fmt.Sprintf("%v\n", latency.Milliseconds())
		}

		_, err = file.Write([]byte(latenciesString))
		if err != nil {
			log.Panic(err)
		}

		if err = file.Close(); err != nil {
			log.Panic(err)
		}

		log.Printf("Max cpu usage %v", maxCPU)

		var avgCPU float64 = 0
		var avgMem float64 = 0
		count := 0
		for _, item := range detailMetricsMap {
			if float64(item.CPU) >= float64(maxCPU)*0.8 {
				avgCPU += float64(item.CPU)
				avgMem += float64(item.Memory)
				count += 1
			}
		}
		avgCPU /= float64(count)
		avgMem /= float64(count)

		metricsMap[gpuLimit] = MetricsStat{
			CPULimit:       "0",
			GpuLimit:       strconv.Itoa(gpuLimit),
			AvgLatency:     time.Duration(avgLatency) * time.Millisecond,
			AvgCPUUsage:    avgCPU,
			MaxCPUUsage:    maxCPU,
			AvgMemoryUsage: avgMem,
			MaxMemoryUsage: maxMem,
			HighLoadRatio:  float64(len(detailMetricsMap)) / float64(count),
			TaskNumber:     taskNumber,
		}
	}

	var results []MetricsStat
	for _, metrics := range metricsMap {
		log.Printf("Node:%v, metrics data is %v",
			"gpu1", metrics)
		results = append(results, metrics)
	}

	file, err := os.Create("det/detMetrics_" + nodeName + ".csv")
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			log.Panic(err)
		}
	}(file)

	marshalData := utils.MarshalCSV(results)

	_, err = file.Write(marshalData)
	if err != nil {
		log.Panic(err)
	}
}

func testFusion(nodeList []string, coreMap map[string]float64) {
	detectResult := utils.SplitDetectResult(fusionImageDir + "detect_result.txt")

	nodeWG := sync.WaitGroup{}
	nodeWG.Add(2)
	log.Printf("Totally got %v detect result", len(detectResult))
	for _, nodeName := range nodeList {
		var metricsMap = make(map[int]MetricsStat)
		go func(cores float64, nodeName string) {
			for cpuLimit := 1000; cpuLimit <= 6500; cpuLimit += 100 {
				detailMetricsMap := make(map[string]*ResourceUsage)
				taskNumber := int(math.Floor((cores * 1000 * 0.8) / float64(cpuLimit)))
				taskNumber = 1
				log.Printf("creating pod... wait for SLAM start up\n")
				createWorkers(cpuLimit, int(cores), nodeName, taskNumber, "fusion")
				waitTime := taskNumber * int(time.Second) * 60
				log.Printf("Wait for extra %v", time.Duration(waitTime))
				time.Sleep(time.Duration(waitTime))
				taskWG := sync.WaitGroup{}
				taskWG.Add(taskNumber)
				taskIDs := make([]string, taskNumber)
				for i := range taskIDs {
					taskIDs[i] = "-1"
				}
				var latencies []time.Duration
				for i := 0; i < taskNumber; i++ {
					go func(i int) {
						startTime := time.Now()
						queryTime := time.Now()
						eachFrameTime := time.Now()
						for imageIndex := 0; imageIndex < len(detectResult); imageIndex++ {
							var status string
							if taskIDs[i] == "-1" {
								status = "Begin"
							} else if imageIndex == len(detectResult)-1 {
								status = "Last"
								notifier, _ := fusionLockMap.Load(taskIDs[i])
								<-notifier.(chan bool)
								if i == taskNumber-1 {
									latencies = append(latencies, time.Since(eachFrameTime))
								}
							} else {
								status = "Running"
								notifier, _ := fusionLockMap.LoadOrStore(taskIDs[i], make(chan bool))
								<-notifier.(chan bool)
								if i == taskNumber-1 {
									latencies = append(latencies, time.Since(eachFrameTime))
								}
							}

							duration := time.Since(startTime)
							needToWait := time.Duration(int64(float64(imageIndex)*(1000./30.))) * time.Millisecond
							if duration < needToWait {
								diff := needToWait - duration
								log.Printf("Wait %v", time.Duration(diff))
								time.Sleep(time.Duration(diff))
							}

							imagePath := fusionImageDir + utils.IntToNDigitsString(imageIndex, 6) + ".png"
							taskIDs[i] = sendFusionRequest(nodeName, imagePath, detectResult[imageIndex],
								status, taskIDs[i])
							eachFrameTime = time.Now()

							if status == "Running" {
								queryDuration := time.Since(queryTime)
								if i == taskNumber-1 && queryDuration.Seconds() >= 2 {
									usage := queryMetrics(taskIDs[i])
									detailMetricsMap[usage.CollectedTime] = usage
									queryTime = time.Now()
								}
							} else if status == "Last" {
								notifier, _ := fusionLockMap.Load(taskIDs[i])
								<-notifier.(chan bool)
								fusionLockMap.Delete(taskIDs[i])
							}

						}
						taskWG.Done()
					}(i)
				}
				taskWG.Wait()

				avgLatency := 0.
				for _, l := range latencies {
					avgLatency += float64(l.Milliseconds())
				}
				avgLatency = avgLatency / float64(len(latencies))

				log.Printf("Avg latency is %v ms for cpu limits: %v", avgLatency, cpuLimit)
				var maxCPU int64 = 0
				var maxMem int64 = 0
				var detailResults []ResourceUsage
				for _, item := range detailMetricsMap {
					if item.CPU > maxCPU {
						maxCPU = item.CPU
					}
					if item.Memory > maxMem {
						maxMem = item.Memory
					}
					detailResults = append(detailResults, *item)
				}

				file, err := os.Create(fmt.Sprintf("fusion/detail_metrics/%v_%v_%v.csv",
					"fusion", nodeName, cpuLimit))
				if err != nil {
					log.Panic(err)
				}

				_, err = file.Write(utils.MarshalCSV(detailResults))
				if err != nil {
					log.Panic(err)
				}

				if err = file.Close(); err != nil {
					log.Panic(err)
				}

				log.Printf("Max cpu usage %v", maxCPU)

				var avgCPU float64 = 0
				var avgMem float64 = 0
				count := 0
				for _, item := range detailMetricsMap {
					if float64(item.CPU) >= float64(maxCPU)*0.8 {
						avgCPU += float64(item.CPU)
						avgMem += float64(item.Memory)
						count += 1
					}
				}
				avgCPU /= float64(count)
				avgMem /= float64(count)

				metricsMap[cpuLimit] = MetricsStat{
					CPULimit:       strconv.Itoa(cpuLimit),
					AvgLatency:     time.Duration(avgLatency) * time.Millisecond,
					AvgCPUUsage:    avgCPU,
					MaxCPUUsage:    maxCPU,
					AvgMemoryUsage: avgMem,
					MaxMemoryUsage: maxMem,
					HighLoadRatio:  float64(len(detailMetricsMap)) / float64(count),
					TaskNumber:     taskNumber,
				}

			}

			var results []MetricsStat
			for cpu := 1000; cpu <= 6500; cpu += 100 {
				metrics := metricsMap[cpu]
				log.Printf("Node:%v, metrics data is %v",
					nodeName, metrics)
				results = append(results, metrics)
			}

			file, err := os.Create("fusion/fusionMetrics_" + nodeName + ".csv")
			if err != nil {
				panic(err)
			}
			defer func(file *os.File) {
				err = file.Close()
				if err != nil {
					log.Panic(err)
				}
			}(file)

			marshalData := utils.MarshalCSV(results)

			_, err = file.Write(marshalData)
			if err != nil {
				log.Panic(err)
			}
			nodeWG.Done()
		}(coreMap[nodeName], nodeName)
	}
	nodeWG.Wait()
}

func testSingleVideo(nodeList []string, coreMap map[string]float64) {
	waitGroup.Add(2)

	for _, nodeName := range nodeList {
		go func(nodeName string, cores float64) {
			// test from 1000 to 7900 and unlimit
			var results []MetricsStat
			// map[int]MetricsStat
			var metricsMap = make(map[int]MetricsStat)
			for cpu := 1000; cpu <= 6000; cpu += 100 {
				var cpuLimit string
				if cpu == 6000 {
					cpuLimit = "0"
				} else {
					cpuLimit = strconv.Itoa(cpu) + "m"
				}
				var slamID string
				var slamIDs []string
				wg := sync.WaitGroup{}
				lock := sync.Mutex{}
				taskNumber := int(math.Floor((cores * 1000 * 0.8) / float64(cpu)))
				//taskNumber = 1
				wg.Add(taskNumber)
				for i := 0; i < taskNumber; i++ {
					go func() {
						slamID = sendSlamRequest(cpuLimit, nodeName)
						lock.Lock()
						slamIDs = append(slamIDs, slamID)
						lock.Unlock()
						wg.Done()
					}()
				}
				wg.Wait()
				slamID = slamIDs[len(slamIDs)-1]
				start := time.Now()
				dataMap := make(map[string]*ResourceUsage)
				var end time.Duration
				for {
					usage := queryMetrics(slamID)
					dataMap[usage.CollectedTime] = usage
					if usage.Available == false && usage.CollectedTime == "Task has been ended" {
						end = time.Since(start)
						for {
							done := true
							for _, id := range slamIDs {
								if id != slamID {
									check := queryMetrics(slamID)
									if !(check.Available == false && check.CollectedTime == "Task has been ended") {
										done = false
									}
								}
							}
							if done {
								goto collectedEnd
							}
						}
					}
					time.Sleep(1 * time.Second)
				}
			collectedEnd:
				log.Printf("Execution time is %v for cpu limits: %v", end, cpu)
				log.Printf("Wait for 1 min to let all pod deleted")
				time.Sleep(1 * time.Minute)
				var maxCPU int64 = 0
				var maxMem int64 = 0
				var detailResults []ResourceUsage
				for _, item := range dataMap {
					if item.CPU > maxCPU {
						maxCPU = item.CPU
					}
					if item.Memory > maxMem {
						maxMem = item.Memory
					}
					detailResults = append(detailResults, *item)
				}

				file, err := os.Create(fmt.Sprintf("slam/detail_metrics/%v_%v_%v.csv",
					"slam", nodeName, cpuLimit))
				if err != nil {
					log.Panic(err)
				}

				_, err = file.Write(utils.MarshalCSV(detailResults))
				if err != nil {
					log.Panic(err)
				}

				if err = file.Close(); err != nil {
					log.Panic(err)
				}

				log.Printf("Max cpu usage %v", maxCPU)

				var avgCPU float64 = 0
				var avgMem float64 = 0
				count := 0
				for _, item := range dataMap {
					if float64(item.CPU) >= float64(maxCPU)*0.8 {
						avgCPU += float64(item.CPU)
						avgMem += float64(item.Memory)
						count += 1
					}
				}
				avgCPU /= float64(count)
				avgMem /= float64(count)

				metricsMap[cpu] = MetricsStat{
					CPULimit:       cpuLimit,
					AvgLatency:     end,
					AvgCPUUsage:    avgCPU,
					MaxCPUUsage:    maxCPU,
					AvgMemoryUsage: avgMem,
					MaxMemoryUsage: maxMem,
					HighLoadRatio:  float64(len(dataMap)) / float64(count),
					TaskNumber:     taskNumber,
				}
			}

			for cpu := 1000; cpu <= 6000; cpu += 100 {
				metrics, _ := metricsMap[cpu]
				log.Printf("Node:%v, metrics data is %v",
					nodeName, metrics)
				results = append(results, metrics)
			}

			file, err := os.Create("slam/slamMetrics_" + nodeName + ".csv")
			if err != nil {
				panic(err)
			}
			defer func(file *os.File) {
				err = file.Close()
				if err != nil {
					log.Panic(err)
				}
			}(file)

			marshalData := utils.MarshalCSV(results)

			_, err = file.Write(marshalData)
			if err != nil {
				log.Panic(err)
			}

			waitGroup.Done()
		}(nodeName, coreMap[nodeName])
	}

	waitGroup.Wait()
}

// ResourceUsage
// Storage 和持久卷绑定，pod删除不消失
// StorageEphemeral pod删除就释放
// Measure resource in range [ CollectedTime - Window, CollectedTime ]
type ResourceUsage struct {
	CPU              int64  `json:"CPU" csv:"CPU"`
	Memory           int64  `json:"Memory" csv:"Memory"`
	Storage          int64  `json:"Storage" csv:"Storage"`
	StorageEphemeral int64  `json:"StorageEphemeral" csv:"StorageEphemeral"`
	CollectedTime    string `json:"CollectedTime" csv:"CollectedTime"`
	Window           int64  `json:"Window" csv:"Window"`
	Available        bool   `json:"Available" csv:"Available"`
	PodName          string `json:"PodName" csv:"PodName"`
}

type CompleteTaskInfo struct {
	DETNodeName        string `json:"det_node_name"`
	DETTaskID          string `json:"det_task_id"`
	FusionNodeName     string `json:"fusion_node_name"`
	FusionTaskID       string `json:"fusion_task_id"`
	Status             string `json:"status"`
	DeleteDETWorker    bool   `json:"delete_det_worker"`
	DeleteFusionWorker bool   `json:"delete_fusion_worker"`
}

type CompleteMetrics struct {
	FusionNodeName string  `csv:"fusion_node_name"`
	GpuLimit       int     `csv:"gpu_limit"`
	CpuLimit       int     `csv:"cpu_limit"`
	AvgLatency     float64 `csv:"avg_latency"`
}

func testCompleteTask(fusionNodeName string, cpuLimit, gpuLimit int,
	writeResult bool) CompleteMetrics {
	fusionResult := ""

	detWorkerCreateInfo := &CreateInfo{
		CpuLimits: map[string]int{
			"gpu1": 0,
		},
		WorkerNumbers: map[string]int{
			"gpu1": 1,
		},
		TaskName: "det",
		GpuLimits: map[string]int{
			"gpu1": gpuLimit,
		},
		GpuMemory: map[string]int{
			"gpu1": 4000,
		},
	}
	fusionWorkerCreateInfo := &CreateInfo{
		CpuLimits: map[string]int{
			fusionNodeName: cpuLimit,
		},
		WorkerNumbers: map[string]int{
			fusionNodeName: 1,
		},
		TaskName: "fusion",
		GpuLimits: map[string]int{
			fusionNodeName: 0,
		},
		GpuMemory: map[string]int{
			fusionNodeName: 0,
		},
	}

	taskInfo := &CompleteTaskInfo{
		DETNodeName:        "gpu1",
		DETTaskID:          "-1",
		FusionNodeName:     fusionNodeName,
		FusionTaskID:       "-1",
		Status:             "Begin",
		DeleteDETWorker:    false,
		DeleteFusionWorker: false,
	}

	//log.Printf("creating pod... wait for det start up\n")
	createGeneralWorkers(detWorkerCreateInfo)
	createGeneralWorkers(fusionWorkerCreateInfo)

	const imageLength = 465
	var metricsMap = make(map[int]MetricsStat)

	detailMetricsMap := make(map[string]*ResourceUsage)

	waitTime := int(time.Second) * 30
	if writeResult {
		log.Printf("Wait for extra %v", time.Duration(waitTime))
	}
	time.Sleep(time.Duration(waitTime))

	var latencies []time.Duration
	queryTime := time.Now()
	eachFrameTime := time.Now()
	for imageIndex := 0; imageIndex < imageLength; imageIndex++ {
		if writeResult {
			fmt.Printf("\rDoing image %v/%v", imageIndex, imageLength)
		}
		os.Stdin.Sync()
		if taskInfo.FusionTaskID == "-1" {
			taskInfo.Status = "Begin"
		} else if imageIndex == imageLength-1 {
			taskInfo.Status = "Last"
			notifier, _ := completTaskLoackMap.LoadOrStore(taskInfo.FusionTaskID, make(chan bool))
			result := <-notifier.(chan string)
			fusionResult = fusionResult + result
			latencies = append(latencies, time.Since(eachFrameTime))
		} else {
			taskInfo.Status = "Running"
			notifier, _ := completTaskLoackMap.LoadOrStore(taskInfo.FusionTaskID, make(chan string))
			result := <-notifier.(chan string)
			fusionResult = fusionResult + result
			latencies = append(latencies, time.Since(eachFrameTime))
		}

		eachFrameTime = time.Now()
		imagePath := completTaskImageDir + utils.IntToNDigitsString(imageIndex, 6) + ".png"
		detTaskID, fusionTaskID := sendCompletTaskRequest(*taskInfo, imagePath)

		if taskInfo.Status == "Begin" {
			taskInfo.DETTaskID = detTaskID
			taskInfo.FusionTaskID = fusionTaskID
		}

		//log.Printf("task info is [%v]", taskInfo)

		if taskInfo.Status == "Running" {
			queryDuration := time.Since(queryTime)
			if queryDuration.Seconds() >= 2 {
				//usage := queryMetrics(taskInfo.FusionTaskID)
				//detailMetricsMap[usage.CollectedTime] = usage
				queryTime = time.Now()
			}
		} else if taskInfo.Status == "Last" {
			notifier, _ := completTaskLoackMap.Load(taskInfo.FusionTaskID)
			result := <-notifier.(chan string)
			fusionResult = fusionResult + result
			completTaskLoackMap.Delete(taskInfo.FusionTaskID)
		}

	}

	if writeResult {

		avgLatency := 0.
		for _, l := range latencies {
			avgLatency += float64(l.Milliseconds())
		}
		avgLatency = avgLatency / float64(len(latencies))

		log.Printf("Avg latency is %v ms for gpu limits: %v", avgLatency, gpuLimit)
		var maxCPU int64 = 0
		var maxMem int64 = 0
		var detailResults []ResourceUsage
		for _, item := range detailMetricsMap {
			if item.CPU > maxCPU {
				maxCPU = item.CPU
			}
			if item.Memory > maxMem {
				maxMem = item.Memory
			}
			detailResults = append(detailResults, *item)
		}

		file, err := os.Create(fmt.Sprintf("complete_task/detail_metrics/%v_%v_%v_%v.csv",
			"fusion", fusionNodeName, gpuLimit, cpuLimit))
		if err != nil {
			log.Panic(err)
		}

		_, err = file.Write(utils.MarshalCSV(detailResults))
		if err != nil {
			log.Panic(err)
		}

		if err = file.Close(); err != nil {
			log.Panic(err)
		}

		file, err = os.Create(fmt.Sprintf("complete_task/detail_metrics/latencies_%v_%v_%v.csv",
			fusionNodeName, gpuLimit, cpuLimit))
		if err != nil {
			log.Panic(err)
		}

		var latenciesString string
		for _, latency := range latencies {
			latenciesString = latenciesString + fmt.Sprintf("%v\n", latency.Milliseconds())
		}

		_, err = file.Write([]byte(latenciesString))
		if err != nil {
			log.Panic(err)
		}

		if err = file.Close(); err != nil {
			log.Panic(err)
		}

		log.Printf("Max cpu usage %v", maxCPU)

		var avgCPU float64 = 0
		var avgMem float64 = 0
		count := 0
		for _, item := range detailMetricsMap {
			if float64(item.CPU) >= float64(maxCPU)*0.8 {
				avgCPU += float64(item.CPU)
				avgMem += float64(item.Memory)
				count += 1
			}
		}
		avgCPU /= float64(count)
		avgMem /= float64(count)

		metricsMap[gpuLimit] = MetricsStat{
			CPULimit:       "0",
			GpuLimit:       strconv.Itoa(gpuLimit),
			AvgLatency:     time.Duration(avgLatency) * time.Millisecond,
			AvgCPUUsage:    avgCPU,
			MaxCPUUsage:    maxCPU,
			AvgMemoryUsage: avgMem,
			MaxMemoryUsage: maxMem,
			HighLoadRatio:  float64(len(detailMetricsMap)) / float64(count),
			TaskNumber:     1,
		}

		var results []MetricsStat
		for _, metrics := range metricsMap {
			log.Printf("Node:%v, metrics data is %v",
				"gpu1", metrics)
			results = append(results, metrics)
		}

		file, err = os.Create(fmt.Sprintf("complete_task/fusion_result/%v_%v_%v.csv",
			fusionNodeName, gpuLimit, cpuLimit))
		if err != nil {
			log.Panic(err)
		}

		_, err = file.Write([]byte(fusionResult))
		if err != nil {
			log.Panic(err)
		}

		if err = file.Close(); err != nil {
			log.Panic(err)
		}
		return CompleteMetrics{
			FusionNodeName: fusionNodeName,
			GpuLimit:       gpuLimit,
			CpuLimit:       cpuLimit,
			AvgLatency:     avgLatency,
		}
	}

	return CompleteMetrics{}
}

func createGeneralWorkers(info *CreateInfo) {
	rawInfo, err := json.Marshal(info)
	if err != nil {
		log.Panic(err)
	}

	buffer := bytes.NewBuffer(rawInfo)

	//log.Printf("sent info is %v", string(rawInfo))

	_, err = http.Post(clusterURL+"/create_workers", "application/json", buffer)
	if err != nil {
		log.Panic(err)
	}
}

func createGPUWorkers(gpu int, nodeName string, taskNumber int, taskName string) {
	info := CreateInfo{
		CpuLimits: map[string]int{
			nodeName: 0,
		},
		WorkerNumbers: map[string]int{
			nodeName: taskNumber,
		},
		TaskName: taskName,
		GpuLimits: map[string]int{
			nodeName: gpu,
		},
		GpuMemory: map[string]int{
			nodeName: 2400,
		},
	}

	rawInfo, err := json.Marshal(&info)
	if err != nil {
		log.Panic(err)
	}

	buffer := bytes.NewBuffer(rawInfo)

	log.Printf("sent info is %v", string(rawInfo))

	_, err = http.Post(clusterURL+"/create_workers", "application/json", buffer)
	if err != nil {
		log.Panic(err)
	}
}

func createWorkers(cpu, cores int, nodeName string, taskNumber int, taskName string) {
	info := CreateInfo{
		CpuLimits: map[string]int{
			nodeName: cpu,
		},
		WorkerNumbers: map[string]int{
			nodeName: taskNumber,
		},
		TaskName: taskName,
		GpuLimits: map[string]int{
			nodeName: 0,
		},
	}

	rawInfo, err := json.Marshal(&info)
	if err != nil {
		log.Panic(err)
	}

	buffer := bytes.NewBuffer(rawInfo)

	log.Printf("sent info is %v", string(rawInfo))

	_, err = http.Post(clusterURL+"/create_workers", "application/json", buffer)
	if err != nil {
		log.Panic(err)
	}
}

func updateCPU(cpu int, nodeName string) {
	updateInfo := fmt.Sprintf("%v:%v", nodeName, cpu)
	_, err := http.Post(clusterURL+"/update_cpu", "text/plain", strings.NewReader(updateInfo))
	if err != nil {
		log.Panic(err)
	}
}

func queryMetrics(taskID string) *ResourceUsage {
	bufferElem := utils.GetBuffer()
	buffer := bufferElem.Buffer
	_, err := buffer.Write([]byte(taskID))
	if err != nil {
		log.Panic(err)
	}
	response, err := http.Post(clusterURL+"/query_metric", "application/json", buffer)
	if err != nil {
		log.Panic(err)
	}

	data, err := io.ReadAll(response.Body)
	if err != nil {
		log.Panic(err)
	}
	usage := &ResourceUsage{}
	err = json.Unmarshal(data, usage)
	if err != nil {
		log.Panic(err)
	}

	response.Body.Close()
	utils.ReturnBuffer(bufferElem)

	return usage
}

func fusionFinished(w http.ResponseWriter, r *http.Request) {
	multipartReader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := multipartReader.ReadForm(1024 * 1024 * 100)
	if err != nil {
		log.Panic(err)
	}

	taskID := form.Value["task_id"][0]
	//_ = form.Value["fusion_result"][0]

	notifier, ok := fusionLockMap.LoadOrStore(taskID, make(chan bool, 1))
	if !ok {
		log.Panic("Not Load chan")
	}

	notifier.(chan bool) <- true

	//log.Printf("Task id %v, result:\n%v", taskID, result)

	_, err = w.Write([]byte("OK"))
	if err != nil {
		log.Panic(err)
	}
}

func detFinished(w http.ResponseWriter, r *http.Request) {
	multipartReader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := multipartReader.ReadForm(1024 * 1024 * 100)
	if err != nil {
		log.Panic(err)
	}

	taskID := form.Value["task_id"][0]

	notifier, ok := detLockMap.LoadOrStore(taskID, make(chan bool, 1))
	if !ok {
		log.Panic("Not Load chan")
	}

	notifier.(chan bool) <- true

	//log.Printf("Task id %v, result:\n%v", taskID, result)

	_, err = w.Write([]byte("OK"))
	if err != nil {
		log.Panic(err)
	}
}

func complteTaskFinish(w http.ResponseWriter, r *http.Request) {
	multipartReader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := multipartReader.ReadForm(1024 * 1024 * 2)
	if err != nil {
		log.Panic(err)
	}

	//detTaskID := form.Value["det_task_id"][0]
	fusionTaskID := form.Value["fusion_task_id"][0]
	fusionResult := form.Value["fusion_result"][0]

	notifier, ok := completTaskLoackMap.LoadOrStore(fusionTaskID, make(chan string, 1))
	if !ok {
		log.Panic("Not Load chan")
	}

	notifier.(chan string) <- fusionResult

	//log.Printf("Task id %v, result:\n%v", taskID, result)

	_, err = w.Write([]byte("OK"))
	if err != nil {
		log.Panic(err)
	}
}

func slamFinished(w http.ResponseWriter, r *http.Request) {
	multipartReader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	_, err = multipartReader.ReadForm(1024 * 1024 * 100)
	if err != nil {
		log.Panic(err)
	}

	log.Println("Task completed!")
	//saveFile("key_frames", "slam/KeyFrameTrajectory.txt", form)

	//log.Println(form.Value["container_output"][0])

	_, err = w.Write([]byte("OK"))
	if err != nil {
		log.Panic(err)
	}
}

func MCMOTFinished(w http.ResponseWriter, r *http.Request) {
	multipartReader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := multipartReader.ReadForm(1024 * 1024 * 100)
	if err != nil {
		log.Panic(err)
	}

	log.Println("Task completed!")
	saveFile("video", "MCMOT/output.mp4", form)
	saveFile("bbox_txt", "MCMOT/output.txt", form)
	saveFile("bbox_xlsx", "MCMOT/output.xlsx", form)

	log.Println(form.Value["container_output"][0])

	_, err = w.Write([]byte("OK"))
	if err != nil {
		log.Panic(err)
	}
}

func sendODRequest() {
	body := &bytes.Buffer{}
	multipartWriter := multipart.NewWriter(body)

	writeValue := func(key, value string) {
		err := multipartWriter.WriteField(key, value)
		if err != nil {
			log.Panic(err)
		}
	}

	writeFile := func(key, filePath, fileName string) {
		filePart, err := multipartWriter.CreateFormFile(key, fileName)
		if err != nil {
			log.Panic(err)
		}
		file, err := os.Open(filePath)
		if err != nil {
			log.Panic(err)
		}
		defer func(file *os.File) {
			err = file.Close()
			if err != nil {
				log.Panic(err)
			}
		}(file)

		_, err = io.Copy(filePart, file)
		if err != nil {
			log.Panic(err)
		}
	}

	writeValue("task_name", "mcmot")

	writeFile("video", MCMOTVideoPath, MCMOTVideoName)

	err := multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	postResp, err := http.Post(newTaskURL, multipartWriter.FormDataContentType(), body)
	if err != nil {
		log.Panic(err)
	}

	_, err = io.ReadAll(postResp.Body)
	if err != nil {
		log.Panic(err)
	}

	//log.Println(string(content))

}

func sendSlamRequest(cpuLimit, nodeName string) string {
	body := &bytes.Buffer{}
	multipartWriter := multipart.NewWriter(body)

	writeValue := func(key, value string) {
		err := multipartWriter.WriteField(key, value)
		if err != nil {
			log.Panic(err)
		}
	}

	writeFile := func(key, filePath, fileName string) {
		filePart, err := multipartWriter.CreateFormFile(key, fileName)
		if err != nil {
			log.Panic(err)
		}
		file, err := os.Open(filePath)
		if err != nil {
			log.Panic(err)
		}
		defer func(file *os.File) {
			err = file.Close()
			if err != nil {
				log.Panic(err)
			}
		}(file)

		_, err = io.Copy(filePart, file)
		if err != nil {
			log.Panic(err)
		}
	}

	writeValue("task_name", "slam")
	writeValue("cpu_limit", cpuLimit)
	writeValue("node_name", nodeName)

	writeFile("video", slamVideoPath, slamVideoName)

	err := multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	postResp, err := http.Post(newTaskURL, multipartWriter.FormDataContentType(), body)
	if err != nil {
		log.Panic(err)
	}

	content, err := io.ReadAll(postResp.Body)
	if err != nil {
		log.Panic(err)
	}

	log.Println(string(content))

	return string(content)

}

func sendFusionRequest(nodeName, imagePath, detectResult, status, taskID string) string {
	bufferElem := utils.GetBuffer()
	body := bufferElem.Buffer
	multipartWriter := multipart.NewWriter(body)

	writeValue := func(key, value string) {
		err := multipartWriter.WriteField(key, value)
		if err != nil {
			log.Panic(err)
		}
	}

	writeFile := func(key, filePath, fileName string) {
		filePart, err := multipartWriter.CreateFormFile(key, fileName)
		if err != nil {
			log.Panic(err)
		}
		file, err := os.Open(filePath)
		if err != nil {
			log.Panic(err)
		}
		defer func(file *os.File) {
			err = file.Close()
			if err != nil {
				log.Panic(err)
			}
		}(file)

		_, err = io.Copy(filePart, file)
		if err != nil {
			log.Panic(err)
		}
	}

	writeValue("task_name", "fusion")
	writeValue("node_name", nodeName)
	writeValue("detect_result", detectResult)
	writeValue("status", status)
	writeValue("task_id", taskID)
	if status == "Last" {
		writeValue("delete", "True")
	}

	writeFile("frame", imagePath, fusionImageName)

	err := multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	postResp, err := http.Post(newTaskURL, multipartWriter.FormDataContentType(), body)
	if err != nil {
		log.Panic(err)
	}

	utils.ReturnBuffer(bufferElem)

	content, err := io.ReadAll(postResp.Body)
	if err != nil {
		log.Panic(err)
	}

	postResp.Body.Close()

	//log.Println(string(content))

	return string(content)

}

func sendDETRequest(nodeName, imagePath, status, taskID string) string {
	bufferElem := utils.GetBuffer()
	body := bufferElem.Buffer
	multipartWriter := multipart.NewWriter(body)

	writeValue := func(key, value string) {
		err := multipartWriter.WriteField(key, value)
		if err != nil {
			log.Panic(err)
		}
	}

	writeFile := func(key, filePath, fileName string) {
		filePart, err := multipartWriter.CreateFormFile(key, fileName)
		if err != nil {
			log.Panic(err)
		}
		file, err := os.Open(filePath)
		if err != nil {
			log.Panic(err)
		}
		defer func(file *os.File) {
			err = file.Close()
			if err != nil {
				log.Panic(err)
			}
		}(file)

		_, err = io.Copy(filePart, file)
		if err != nil {
			log.Panic(err)
		}
	}

	writeValue("task_name", "det")
	writeValue("node_name", nodeName)
	writeValue("status", status)
	writeValue("task_id", taskID)
	if status == "Last" {
		writeValue("delete", "True")
	}

	writeFile("frame", imagePath, fusionImageName)

	err := multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	postResp, err := http.Post(newTaskURL, multipartWriter.FormDataContentType(), body)
	if err != nil {
		log.Panic(err)
	}

	utils.ReturnBuffer(bufferElem)

	content, err := io.ReadAll(postResp.Body)
	if err != nil {
		log.Panic(err)
	}

	postResp.Body.Close()

	//log.Println(string(content))

	return string(content)

}

func sendCompletTaskRequest(taskInfo CompleteTaskInfo, imagePath string) (string, string) {
	bufferElem := utils.GetBuffer()
	body := bufferElem.Buffer
	multipartWriter := multipart.NewWriter(body)

	writeValue := func(key, value string) {
		err := multipartWriter.WriteField(key, value)
		if err != nil {
			log.Panic(err)
		}
	}

	writeFile := func(key, filePath, fileName string) {
		filePart, err := multipartWriter.CreateFormFile(key, fileName)
		if err != nil {
			log.Panic(err)
		}
		file, err := os.Open(filePath)
		if err != nil {
			log.Panic(err)
		}
		defer func(file *os.File) {
			err = file.Close()
			if err != nil {
				log.Panic(err)
			}
		}(file)

		_, err = io.Copy(filePart, file)
		if err != nil {
			log.Panic(err)
		}
	}

	if taskInfo.Status == "Last" {
		taskInfo.DeleteDETWorker = true
		taskInfo.DeleteFusionWorker = true
	}

	jsonByte, err := json.Marshal(taskInfo)
	if err != nil {
		log.Panic(err)
	}

	writeValue("json", string(jsonByte))

	writeFile("frame", imagePath, fusionImageName)

	err = multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	postResp, err := http.Post(completeTaskURL, multipartWriter.FormDataContentType(), body)
	if err != nil {
		log.Panic(err)
	}

	utils.ReturnBuffer(bufferElem)

	content, err := io.ReadAll(postResp.Body)
	if err != nil {
		log.Panic(err)
	}

	postResp.Body.Close()

	results := strings.Split(string(content), ":")

	detTaskID := results[0]
	fusionTaskID := results[1]

	return detTaskID, fusionTaskID
}

func saveFile(fieldName, fileName string, form *multipart.Form) {
	file, err := form.File[fieldName][0].Open()
	if err != nil {
		log.Panic(err)
	}

	newFile, err := os.Create(fileName)
	if err != nil {
		log.Panic(err)
	}
	_, err = io.Copy(newFile, file)
	if err != nil {
		log.Panic(err)
	}

	err = file.Close()
	if err != nil {
		log.Panic(err)
	}
	err = newFile.Close()
	if err != nil {
		log.Panic(err)
	}
}
