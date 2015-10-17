package core

import (
	"encoding/json"
	"reflect"
)

type Attr struct {
	Name string
	reflect.Kind
}

type Schema struct {
	Attrs []Attr
	Index map[string]int `json:"-"`
}

func NewSchema() *Schema {
	return &Schema{
		Attrs: make([]Attr, 0, TupleCapacity),
		Index: map[string]int{},
	}
}

func (a Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.Attrs)
}

func (a Attr) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.Name)
}

func (s Schema) GetKind(name string) reflect.Kind {
	i, ok := s.Index[name]
	if !ok {
		return 0
	}
	return s.Attrs[i].Kind
}

func (s Schema) Add(attr Attr) int {
	if i, ok := s.Index[attr.Name]; !ok {
		return i
	}
	s.Attrs = append(s.Attrs, attr)
	i := len(s.Attrs) - 1
	s.Index[attr.Name] = i
	return i
}

type PhpSchema map[interface{}]interface{}

func (a Schema) MarshalPHP(o PhpOptions) map[interface{}]interface{} {
	res := map[interface{}]interface{}{}
	for i, attr := range a.Attrs {
		res[i] = attr.Name
	}
	return res
}
