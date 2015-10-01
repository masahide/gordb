// go-rdb Tuple
package core

import "encoding/json"

type Tuple struct {
	attrs Schema
	data  map[string]Value
}

func NewTuple() *Tuple {
	attrs := make(Schema, 0, TupleCapacity)
	return &Tuple{attrs: attrs, data: map[string]Value{}}
}
func (t *Tuple) Cutout(args []string) *Tuple {
	schema := make(Schema, 0, len(args))
	data := map[string]Value{}
	for _, arg := range args {
		for _, attr := range t.attrs {
			if attr.Name == arg {
				schema = append(schema, attr)
				data[arg] = t.data[arg]
			}
		}
	}
	newt := NewTuple()
	newt.attrs = schema
	newt.data = data

	return newt
}
func (t *Tuple) Set(attr Attr, value Value) {
	if _, ok := t.data[attr.Name]; !ok {
		t.attrs = append(t.attrs, attr)
	}
	t.data[attr.Name] = value
}
func (t *Tuple) Get(attrName string) Value {
	v, _ := t.data[attrName]
	return v
}

func (t *Tuple) GetAttr(attrName string) Attr {
	for _, f := range t.attrs {
		if f.Name == attrName {
			return f
		}
	}
	return Attr{}
}

func (t *Tuple) Len() int {
	return len(t.data)
}

func (t *Tuple) Attrs() Schema {
	return t.attrs
}

func (t *Tuple) Iterator(cb func(i int, attr Attr, value Value) error) error {
	for i, attr := range t.Attrs() {
		if err := cb(i, attr, t.data[attr.Name]); err != nil {
			return err
		}
	}
	return nil
}

func (t *Tuple) MarshalJSON() ([]byte, error) {
	res := make([]Value, len(t.data))
	for i, attr := range t.Attrs() {
		res[i] = t.data[attr.Name]
	}
	return json.Marshal(res)
}
