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
