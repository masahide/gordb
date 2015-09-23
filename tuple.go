// go-rdb Tuple
package gordb

type Tuple struct {
	headers []string
	data    map[string]Value
}

func NewTuple() *Tuple {
	return &Tuple{headers: make([]string, 0, TupleCapacity), data: map[string]Value{}}
}
func (t *Tuple) Set(key string, value Value) {
	if _, ok := t.data[key]; !ok {
		t.headers = append(t.headers, key)
	}
	t.data[key] = value
}
func (t *Tuple) Get(key string) Value {
	v, _ := t.data[key]
	return v
}

func (t *Tuple) Len() int {
	return len(t.data)
}

func (t *Tuple) Headers() []string {
	return t.headers
}

func (t *Tuple) Iterator(cb func(i int, key string, value Value) error) error {
	for i, key := range t.Headers() {
		if err := cb(i, key, t.data[key]); err != nil {
			return err
		}
	}
	return nil
}
