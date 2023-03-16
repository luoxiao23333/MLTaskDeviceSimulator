package main

import (
	"bytes"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
)

const videoPath = "input.avi"
const videoName = "input.avi"
const clusterURL = "http://192.168.1.101:8081"
const newTaskURL = clusterURL + "/new_task"

const receivePort = ":8080"

// TODO in receive part, multipart form ["video"] is nil
// may scheduler not load, or may device load nil

func main() {
	sendODRequest()

	http.HandleFunc("/object_detection", objectDetection)
	err := http.ListenAndServe(receivePort, nil)
	if err != nil {
		log.Panic(err)
	}
}

func objectDetection(w http.ResponseWriter, r *http.Request) {
	multipartReader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := multipartReader.ReadForm(1024 * 1024 * 100)
	if err != nil {
		log.Panic(err)
	}

	log.Println("Task completed!")
	saveFile("video", "output.mp4", form)
	saveFile("bbox_txt", "output.txt", form)
	saveFile("bbox_xlsx", "output.xlsx", form)

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

	writeValue("task_name", "object_detection")

	writeFile("video", videoPath, videoName)

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

	// TODO parse post.Body

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
