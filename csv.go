package gordb

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
)

func recordToData(records [][]string) [][]Value {
	result := make([][]Value, len(records))
	for i, row := range records {
		result[i] = make([]Value, len(row))
		for j, v := range row {
			result[i][j] = Value(v)
		}
	}
	return result
}

// CSVRelational
func NewCSVRelationalStream(r io.Reader) *Relation {
	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	return &Relation{
		Field: rows[0],
		Data:  recordToData(rows[1:]),
	}
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
