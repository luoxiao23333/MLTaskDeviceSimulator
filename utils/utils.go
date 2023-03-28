package utils

import (
	"encoding/csv"
	"fmt"
	"log"
	"reflect"
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
