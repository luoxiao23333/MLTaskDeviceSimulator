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

const clusterURL = "http://192.168.1.101:8081"
const newTaskURL = clusterURL + "/new_task"

const receivePort = ":8080"

// map[taskID]chan bool
var fusionLockMap sync.Map

type MetricsStat struct {
	CPULimit       string        `csv:"CPULimit"`
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
	// batchSize     map[string]int
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

	var nodeList = []string{"controller", "as1"}
	coreMap := map[string]float64{
		"controller": 32,
		"as1":        16,
	}

	testFusion(nodeList, coreMap)
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
				createWorkers(cpuLimit, int(cores), nodeName, taskNumber)
				waitTime := taskNumber * int(time.Second) * 10
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

func createWorkers(cpu, cores int, nodeName string, taskNumber int) {
	info := CreateInfo{
		CpuLimits: map[string]int{
			nodeName: cpu,
		},
		WorkerNumbers: map[string]int{
			nodeName: taskNumber,
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
