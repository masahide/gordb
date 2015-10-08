package core

import (
	"encoding/json"
	"reflect"
)

type Attr struct {
	Name string
	reflect.Kind
}

type Schema []Attr

func (a Attr) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.Name)
}

func (s Schema) GetKind(name string) reflect.Kind {
	for _, attr := range s {
		if attr.Name == name {
			return attr.Kind
		}
	}
	return 0
}

type PhpSchema map[int]string

func (a Schema) MarshalPHP() PhpSchema {
	res := PhpSchema{}
	for i, attr := range a {
		res[i] = attr.Name
	}
	return res
}
