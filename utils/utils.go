package utils

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

func MarshalCSV(data interface{}) []byte {
	var lines [][]string

	rv := reflect.ValueOf(data)
	if rv.Kind() != reflect.Slice {
		log.Panic(fmt.Errorf("not a slice"))
	}

	rt := rv.Type().Elem()
	var fields []string
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("csv")
		if tag != "" {
			fields = append(fields, tag)
		} else {
			fields = append(fields, field.Name)
		}
	}
	lines = append(lines, fields)

	for i := 0; i < rv.Len(); i++ {
		var row []string
		for j := 0; j < rt.NumField(); j++ {
			field := rt.Field(j)
			if tag := field.Tag.Get("csv"); tag != "" {
				row = append(row, fmt.Sprintf("%v", rv.Index(i).Field(j)))
			} else {
				row = append(row, fmt.Sprintf("%v", rv.Index(i).Field(j)))
			}
		}
		lines = append(lines, row)
	}

	var sb strings.Builder
	writer := csv.NewWriter(&sb)
	err := writer.WriteAll(lines)
	if err != nil {
		log.Panic(err)
	}
	if err = writer.Error(); err != nil {
		log.Panic(err)
	}

	return []byte(sb.String())
}

func SplitDetectResult(filePath string) []string {

	// 打开文件并创建一个 scanner 来逐行读取数据
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			log.Panic(err)
		}
	}(file)

	scanner := bufio.NewScanner(file)

	// 初始化当前组的字符串变量和结果数组
	currentGroup := ""
	var results []string

	// 遍历每一行数据进行处理
	for scanner.Scan() {
		line := scanner.Text()

		// 如果遇到分隔符 -1，则将当前组加入结果数组，并重置当前组为空字符串
		if line == "-1" {
			if currentGroup != "" {
				currentGroup += "\n-1\n"
				results = append(results, currentGroup)
				currentGroup = ""
			} else {
				results = append(results, "-1\n")
			}
			continue
		}

		// 如果当前组非空，则在后面添加换行符，否则不添加
		if currentGroup != "" {
			currentGroup += "\n"
		}

		// 将当前行数据添加到当前组中
		currentGroup += line
	}

	// 如果最后一组还未被加入结果数组，则将其加入
	if currentGroup != "" {
		results = append(results, currentGroup)
	}

	return results

}

// IntToNDigitsString cover n to string
// like IntToNDigitsString(303, 6) == "000303"
func IntToNDigitsString(n int, digits int) string {
	s := strconv.Itoa(n)
	for len(s) < digits {
		s = "0" + s
	}
	return s
}

func ExecuteWithTimeout() {}
