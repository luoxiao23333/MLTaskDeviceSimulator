package utils

import (
	"bytes"
	"log"
	"sync"
	"time"
)

var bufferLock *sync.Mutex = &sync.Mutex{}
var bufferPool *[]*BufferElem = nil
var buferAvalable []bool

const bufferSize = 10

type BufferElem struct {
	Buffer *bytes.Buffer
	index  int
}

func GetBuffer() *BufferElem {
	bufferLock.Lock()
	if bufferPool == nil {
		bufferPool = &[]*BufferElem{}
		for i := 0; i < bufferSize; i++ {
			*bufferPool = append(*bufferPool, &BufferElem{
				Buffer: &bytes.Buffer{},
				index:  i,
			})
			buferAvalable = append(buferAvalable, true)
		}
	}

	var chooseBuffer *BufferElem = nil
	for {
		for i := 0; i < bufferSize; i++ {
			if buferAvalable[i] {
				buferAvalable[i] = false
				chooseBuffer = (*bufferPool)[i]
				break
			}
		}
		if chooseBuffer != nil {
			break
		}
		bufferLock.Unlock()
		log.Printf("No avalable buffer, wait for 5 second")
		time.Sleep(5 * time.Second)
		bufferLock.Lock()
	}

	bufferLock.Unlock()
	return chooseBuffer
}

func ReturnBuffer(bufferElem *BufferElem) {
	bufferLock.Lock()
	buferAvalable[bufferElem.index] = true
	bufferElem.Buffer.Reset()
	bufferLock.Unlock()
}
