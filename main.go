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
	"sync"
	"time"
)

const MCMOTVideoPath = "MCMOT/input.avi"
const MCMOTVideoName = "input.avi"

const slamVideoPath = "slam/input.mp4"
const slamVideoName = "input.mp4"

const clusterURL = "http://192.168.1.101:8081"
const newTaskURL = clusterURL + "/new_task"

const receivePort = ":8080"

type MetricsStat struct {
	CPULimit       string        `csv:"CPULimit"`
	AET            time.Duration `csv:"AET"`
	AvgCPUUsage    float64       `csv:"AvgCPUUsage"`
	MaxCPUUsage    int64         `csv:"MaxCPUUsage"`
	AvgMemoryUsage float64       `csv:"AvgMemoryUsage"`
	MaxMemoryUsage int64         `csv:"MaxMemoryUsage"`
	HighLoadRatio  float64       `csv:"HighLoadRatio"`
}

var waitGroup sync.WaitGroup

func main() {
	go func() {
		http.HandleFunc("/mcmot", MCMOTFinished)
		http.HandleFunc("/slam", slamFinished)
		err := http.ListenAndServe(receivePort, nil)
		if err != nil {
			log.Panic(err)
		}
	}()

	//sendODRequest()

	var nodeList = []string{"controller", "as1"}
	coreMap := map[string]float64{
		"controller": 32,
		"as1":        16,
	}
	waitGroup.Add(2)

	for _, nodeName := range nodeList {
		go func(nodeName string, cores float64) {
			// test from 1000 to 7900 and unlimit
			var results []MetricsStat
			// map[int]MetricsStat
			var metricsMap = make(map[int]MetricsStat)
			for cpu := 1000; cpu <= 8000; cpu += 100 {
				var cpuLimit string
				if cpu == 8000 {
					cpuLimit = "0"
				} else {
					cpuLimit = strconv.Itoa(cpu) + "m"
				}
				var slamID string
				var slamIDs []string
				wg := sync.WaitGroup{}
				lock := sync.Mutex{}
				taskNumber := int(math.Floor((cores * 1000 * 0.7) / float64(cpu)))
				if taskNumber < 6 {
					taskNumber = 6
				}
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
				start := time.Now()
				dataMap := make(map[string]*ResourceUsage)
				for {
					usage := queryMetrics(slamID)
					if usage == nil {
						done := false
						for _, id := range slamIDs {
							if id != slamID {
								check := queryMetrics(slamID)
								if check != nil {
									done = true
								}
							}
						}
						if done {
							break
						}
					}
					dataMap[usage.CollectedTime] = usage
					if usage.Available == false && len(dataMap) > 1 {
						break
					}
					time.Sleep(1 * time.Second)
				}
				log.Printf("Execution time is %v for cpu limits: %v", time.Since(start), cpu)
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
					AET:            time.Since(start),
					AvgCPUUsage:    avgCPU,
					MaxCPUUsage:    maxCPU,
					AvgMemoryUsage: avgMem,
					MaxMemoryUsage: maxMem,
					HighLoadRatio:  float64(len(dataMap)) / float64(count),
				}
			}

			for cpu := 1000; cpu <= 8000; cpu += 100 {
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

func queryMetrics(taskID string) *ResourceUsage {
	buffer := &bytes.Buffer{}
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

	return usage
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

	content, err := io.ReadAll(postResp.Body)
	if err != nil {
		log.Panic(err)
	}

	log.Println(string(content))

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
