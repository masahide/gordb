package csv

import (
	enc "encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/masahide/gordb/core"
)

const inferenceRowSize = 10000

var ErrSchemaNil = errors.New("attrs is nil.")

func recordToData(attrs *core.Schema, records [][]string) ([][]core.Value, error) {
	if attrs == nil {
		return nil, ErrSchemaNil
	}
	result := make([][]core.Value, len(records))
	for i, row := range records {
		values := make([]core.Value, len(row))
		for j, v := range row {
			_, value := inferenceType(v)
			/*
				if kind != attrs[j].Kind {
					return nil, fmt.Errorf("Type is different. line:%d,col:%d value:%v(type:%s), want:%s", i+2, j+1, v, kind, attrs[j].Kind)
				}
			*/
			if attrs.Attrs[j].Kind == reflect.String {
				values[j] = v
				continue
			}
			values[j] = value
		}
		result[i] = values
	}
	return result, nil
}

func LoadCsv(filename string) (*core.Relation, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	original, err := NewCSVRelationalStream(f)
	if err != nil {
		return nil, err
	}
	original.Name = path.Base(filename)
	original.Name = strings.TrimRight(original.Name, path.Ext(original.Name))
	original.CreateIndex()
	return original, err
}

// CSVRelational
func NewCSVRelationalStream(r io.ReadSeeker) (*core.Relation, error) {
	attrs, err := typeInference(r)
	if err != nil {
		return nil, err
	}
	reader := enc.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	data, err := recordToData(attrs, rows[1:])
	if err != nil {
		return nil, err
	}
	return &core.Relation{
		Attrs: attrs,
		Data:  data,
	}, nil
}

func typeInference(r io.ReadSeeker) (*core.Schema, error) {
	reader := enc.NewReader(r)
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
	s := core.NewSchema()
	attrs := make([]core.Attr, len(record))
	for i, attr := range record {
		attrs[i].Name = attr
		s.Index[attr] = i
	}
	for _, record := range records[1:] {
		for i, attr := range record {
			kind, _ := inferenceType(attr)
			if attrs[i].Kind == reflect.Invalid {
				attrs[i].Kind = kind
				continue
			}
			if attrs[i].Kind == reflect.Int64 && kind == reflect.Float64 {
				attrs[i].Kind = kind
				continue
			}
			if attrs[i].Kind == reflect.String && kind != reflect.String {
				//return attrs, fmt.Errorf("inference type detect error. Kind:%s->%s", attrs[i].Kind, kind)
				attrs[i].Kind = kind // Promotion to string
				continue
			}

		}
	}
	s.Attrs = attrs
	return s, nil
}

func inferenceType(s string) (reflect.Kind, interface{}) {
	if s == "" {
		return reflect.Invalid, ""
	}
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
