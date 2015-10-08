// go-rdb Tuple
package core

import "encoding/json"

type Tuple struct {
	Attrs Schema
	Data  map[string]Value
}

func NewTuple() *Tuple {
	attrs := make(Schema, 0, TupleCapacity)
	return &Tuple{Attrs: attrs, Data: map[string]Value{}}
}

/*
func (t *Tuple) Cutout(args []string) *Tuple {
	schema := make(Schema, 0, len(args))
	data := map[string]Value{}
	for _, arg := range args {
		for _, attr := range t.Attrs {
			if attr.Name == arg {
				schema = append(schema, attr)
				data[arg] = t.Data[arg]
			}
		}
	}
	newt := NewTuple()
	newt.Attrs = schema
	newt.Data = data

	return newt
}
*/

func (t *Tuple) Set(attr Attr, value Value) {
	if _, ok := t.Data[attr.Name]; !ok {
		t.Attrs = append(t.Attrs, attr)
	}
	t.Data[attr.Name] = value
}
func (t *Tuple) Get(attrName string) Value {
	v, _ := t.Data[attrName]
	return v
}

func (t *Tuple) GetAttr(attrName string) Attr {
	for _, f := range t.Attrs {
		if f.Name == attrName {
			return f
		}
	}
	return Attr{}
}

/*
func (t *Tuple) Len() int {
	return len(t.Data)
}
*/

func (t *Tuple) MarshalJSON() ([]byte, error) {
	res := make([]Value, len(t.Data))
	for i, attr := range t.Attrs {
		res[i] = t.Data[attr.Name]
	}
	return json.Marshal(res)
}

type PhpTuple map[interface{}]interface{}

func (t *Tuple) MarshalPHP() map[interface{}]interface{} {
	res := map[interface{}]interface{}{}
	for i, attr := range t.Attrs {
		res[i] = t.Data[attr.Name]
	}
	return res
}
