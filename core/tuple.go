// go-rdb Tuple
package core

import "encoding/json"

type Tuple struct {
	*Schema
	Data []Value
}

func NewTuple() *Tuple {
	return &Tuple{Schema: NewSchema(), Data: []Value{}}
}

func (t *Tuple) Set(attr Attr, value Value) {
	if i, ok := t.Index[attr.Name]; ok {
		t.Data[i] = value
		return
	}
	t.Index[attr.Name] = len(t.Attrs)
	t.Attrs = append(t.Attrs, attr)
	t.Data = append(t.Data, value)
}
func (t *Tuple) Get(attrName string) Value {
	i, ok := t.Index[attrName]
	if ok {
		return t.Data[i]
	}
	return nil
}

func (t *Tuple) GetAttr(attrName string) Attr {
	i, ok := t.Schema.Index[attrName]
	if !ok {
		return Attr{}
	}
	return t.Attrs[i]
}

/*
func (t *Tuple) Len() int {
	return len(t.Data)
}
*/

func (t *Tuple) MarshalJSON() ([]byte, error) {
	res := make([]Value, len(t.Data))
	for i, attr := range t.Attrs {
		res[i] = t.Data[t.Index[attr.Name]]
	}
	return json.Marshal(res)
}

type PhpTuple map[interface{}]interface{}

func (t *Tuple) MarshalPHP(o PhpOptions) map[interface{}]interface{} {
	res := map[interface{}]interface{}{}
	if o.KvFmt {
		for _, attr := range t.Attrs {
			res[attr.Name] = t.Data[t.Index[attr.Name]]
		}
	} else {
		for i, attr := range t.Attrs {
			res[i] = t.Data[t.Index[attr.Name]]
		}
	}
	return res
}
