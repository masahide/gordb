// go-rdb Tuple
package gordb

type Tuple struct {
	fields Schema
	data   map[string]Value
}

func NewTuple() *Tuple {
	return &Tuple{fields: make(Schema, 0, TupleCapacity), data: map[string]Value{}}
}
func (t *Tuple) Set(field Field, value Value) {
	if _, ok := t.data[field.Name]; !ok {
		t.fields = append(t.fields, field)
	}
	t.data[field.Name] = value
}
func (t *Tuple) Get(fieldName string) Value {
	v, _ := t.data[fieldName]
	return v
}

func (t *Tuple) GetField(fieldName string) Field {
	for _, f := range t.fields {
		if f.Name == fieldName {
			return f
		}
	}
	return Field{}
}

func (t *Tuple) Len() int {
	return len(t.data)
}

func (t *Tuple) Fields() Schema {
	return t.fields
}

func (t *Tuple) Iterator(cb func(i int, field Field, value Value) error) error {
	for i, field := range t.Fields() {
		if err := cb(i, field, t.data[field.Name]); err != nil {
			return err
		}
	}
	return nil
}
