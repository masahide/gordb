// go-rdb Tuple
package core

type Tuple struct {
	attrs Schema
	data  map[string]Value
}

func NewTuple() *Tuple {
	return &Tuple{attrs: make(Schema, 0, TupleCapacity), data: map[string]Value{}}
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
