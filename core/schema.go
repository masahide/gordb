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
