package gordb

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
)

func recordToData(fields Schema, records [][]string) ([][]Value, error) {
	result := make([][]Value, len(records))
	for i, row := range records {
		result[i] = make([]Value, len(row))
		for j, v := range row {
			kind, value := inferenceType(v)
			if kind != fields[j].Kind {
				return nil, fmt.Errorf("Type is different. line:%d,col:%d value:%v(type:%s), want:%s", i+2, j+1, v, kind, fields[j].Kind)
			}
			result[i][j] = value
		}
	}
	return result, nil
}

// CSVRelational
func NewCSVRelationalStream(r io.ReadSeeker) (*Relation, error) {
	fields, err := Inference(r)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	data, err := recordToData(fields, rows[1:])
	if err != nil {
		return nil, err
	}
	return &Relation{
		Fields: fields,
		Data:   data,
	}, nil
}

type Field struct {
	Name string
	reflect.Kind
}

type Schema []Field

const inferenceRowSize = 4

func Inference(r io.ReadSeeker) (Schema, error) {
	reader := csv.NewReader(r)
	defer r.Seek(0, 0)
	records := make([][]string, 0, inferenceRowSize)
	i := 0
	for ; i < inferenceRowSize; i++ {
		record, err := reader.Read()
		records = append(records, record)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("inference err: %s", err)
		}
	}

	record := records[0]
	s := make(Schema, len(record))
	for i, field := range record {
		s[i].Name = field
	}
	for _, record := range records[1:] {
		for i, field := range record {
			kind, _ := inferenceType(field)
			if s[i].Kind == reflect.Invalid {
				s[i].Kind = kind
				continue
			}
			if s[i].Kind == reflect.Int64 && kind == reflect.Float64 {
				s[i].Kind = kind
				continue
			}
			if s[i].Kind == reflect.String && kind != reflect.String {
				return s, fmt.Errorf("inference type detect error. Kind:%s->%s", s[i].Kind, kind)
				continue
			}

		}
	}
	return s, nil
}

func inferenceType(s string) (reflect.Kind, interface{}) {
	if i, err := strconv.ParseInt(s, 10, 0); err == nil {
		return reflect.Int64, i
	}
	if strings.IndexByte(s, byte('.')) != -1 {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return reflect.Float64, f
		}
	}
	return reflect.String, s
}
