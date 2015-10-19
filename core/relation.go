package core

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
)

type Relation struct {
	index       int
	Name        string    `json:"name,omitempty" `
	Attrs       *Schema   `json:"attrs"`
	Data        [][]Value `json:"data"`
	staticIndex []indexArrays
}

type indexArray struct {
	key Value
	ptr int
}

type indexArrays []indexArray

func (a indexArrays) Len() int {
	return len(a)
}
func (a indexArrays) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a indexArrays) Less(i, j int) bool {
	switch {
	case Compare(a[i].key, a[j].key, LessThan): //a[i].key < a[j].key:
		return true
	case Compare(a[i].key, a[j].key, GreaterThan): // a[i].key > a[j].key:
		return false
	case a[i].ptr < a[j].ptr:
		return true
	case a[i].ptr > a[j].ptr:
		return false
	}
	return false
}

func (r *Relation) HasNext() bool {
	return r.index < len(r.Data)
}
func (r *Relation) Close() {
	r.index = 0
}

func (r *Relation) Next() (*Tuple, error) {
	tuple := &Tuple{
		Schema: r.Attrs,
		Data:   r.Data[r.index],
	}
	r.index++
	return tuple, nil
}

func (r *Relation) Clone() *Relation {
	return &Relation{
		Name:  r.Name,
		Attrs: r.Attrs,
		Data:  r.Data,
	}
}

func (r *Relation) Init(root *Node) error {
	r.index = 0
	rel, err := root.GetRelation(r.Name)
	if err != nil {
		return err
	}
	r.Attrs = rel.Attrs
	r.Data = rel.Data
	r.staticIndex = rel.staticIndex
	return nil
}

/*
type PhpRelation struct {
	Name  string
	Attrs map[int]string
	Data  map[int]PhpTuple
}
*/
type PhpOptions struct {
	KvFmt  bool   `json:"kv,omitempty"`
	MapKey string `json:"map_key,omitempty"`
}
type PhpRelation map[interface{}]interface{}

func (r *Relation) MarshalPHP(o PhpOptions) map[interface{}]interface{} {
	res := PhpRelation{
		"Name":  r.Name,
		"Attrs": r.Attrs.MarshalPHP(o),
		"Data":  map[interface{}]interface{}{},
	}
	switch o.MapKey {
	case "":
		for i, row := range r.Data {
			tuple := Tuple{
				Schema: r.Attrs,
				Data:   row,
			}
			res["Data"].(map[interface{}]interface{})[i] = tuple.MarshalPHP(o)
		}
	default:
		for i, row := range r.Data {
			tuple := Tuple{
				Schema: r.Attrs,
				Data:   row,
			}
			key, ok := tuple.Index[o.MapKey]
			if ok {
				res["Data"].(map[interface{}]interface{})[tuple.Data[key]] = tuple.MarshalPHP(o)
			} else {
				res["Data"].(map[interface{}]interface{})[i] = tuple.MarshalPHP(o)
			}
		}
	}
	return res
}

func (r *Relation) CreateIndex() {
	r.staticIndex = make([]indexArrays, len(r.Attrs.Attrs))
	for i, _ := range r.Attrs.Attrs {
		arry := make(indexArrays, len(r.Data))
		for j, v := range r.Data {
			arry[j] = indexArray{key: v[i], ptr: j}
		}
		sort.Sort(arry)
		r.staticIndex[i] = arry
	}
}

func (r *Relation) findSameValueInDesc(attr string, from int, key Value) int {
	i, ok := r.Attrs.Index[attr]
	if !ok {
		return from
	}
	arry := r.staticIndex[i]
	i = from
	for i > -1 && arry[i].key == key {
		i--
	}
	return i + 1
}

func (r *Relation) findSameValueInAsc(attr string, from int, key Value) int {
	i, ok := r.Attrs.Index[attr]
	if !ok {
		return from
	}
	arry := r.staticIndex[i]
	i = from
	for i < len(arry) && arry[i].key == key {
		i++
	}
	return i - 1
}

func (r *Relation) multiSearch(attr string, key Value, kind reflect.Kind) []int {
	result := []int{}
	i, ok := r.Attrs.Index[attr]
	if !ok {
		return result
	}
	arry := r.staticIndex[i]
	tail := len(arry) - 1

	for head := 0; head <= tail; {
		mid := head + ((tail - head) / 2)
		if ok, _ := GreaterThan(kind, arry[mid].key, kind, key); ok {
			tail = mid - 1
		} else if ok, _ := LessThan(kind, arry[mid].key, kind, key); ok {
			head = mid + 1
		} else {
			from := r.findSameValueInDesc(attr, mid, key)
			to := r.findSameValueInAsc(attr, mid, key)
			result = make([]int, to-from+1)
			for i := 0; i <= to-from; i++ {
				result[i] = arry[from+i].ptr
			}
			return result
		}
	}
	return result

}

func (r *Relation) searchGreaterThan(attr string, key Value, include bool, kind reflect.Kind) []int {
	result := []int{}
	i, ok := r.Attrs.Index[attr]
	if !ok {
		return result
	}
	arry := r.staticIndex[i]
	tail := len(arry) - 1
	head := 0
	from := head
	if ok, err := LessThan(kind, arry[tail].key, kind, key); ok || err != nil {
		return result
	}
	if ok, _ := GreaterThan(kind, arry[head].key, kind, key); !ok {
		for head <= tail {
			mid := head + ((tail - head) / 2)
			if ok, _ := GreaterThan(kind, arry[mid].key, kind, key); ok {
				tail = mid - 1
			} else if ok, _ := LessThan(kind, arry[mid].key, kind, key); ok {
				head = mid + 1
			} else {
				if include {
					from = r.findSameValueInDesc(attr, mid, key)
					break
				}
				head = mid + 1
			}
			if head > tail {
				if head < len(arry) && head >= 0 {
					from = r.findSameValueInDesc(attr, head, arry[head].key)
					break
				}
			}
		}
	}
	result = make([]int, len(arry)-from)
	for i := from; i < len(arry); i++ {
		result[i-from] = arry[i].ptr
	}

	return result

}
func (r *Relation) searchLessThan(attr string, key Value, include bool, kind reflect.Kind) []int {
	result := []int{}
	i, ok := r.Attrs.Index[attr]
	if !ok {
		return result
	}
	arry := r.staticIndex[i]
	tail := len(arry) - 1
	head := 0
	to := tail
	if ok, err := GreaterThan(kind, arry[head].key, kind, key); ok || err != nil {
		return result
	}
	if ok, _ := LessThan(kind, arry[tail].key, kind, key); !ok {
		for head <= tail {
			mid := head + ((tail - head) / 2)
			if ok, _ := GreaterThan(kind, arry[mid].key, kind, key); ok {
				tail = mid - 1
			} else if ok, _ := LessThan(kind, arry[mid].key, kind, key); ok {
				head = mid + 1
			} else {
				if include {
					to = r.findSameValueInAsc(attr, mid, key)
					break
				}
				tail = mid - 1
			}
			if head > tail {
				if tail < len(arry) && tail >= 0 {
					to = r.findSameValueInAsc(attr, tail, arry[tail].key)
					break
				}
			}
		}
	}
	result = make([]int, to+1)
	for i := 0; i <= to; i++ {
		result[i] = arry[i].ptr
	}
	return result
}

func SearchLessThan(r *Relation, attr string, key Value, kind reflect.Kind) []int {
	return r.searchLessThan(attr, key, false, kind)
}
func SearchNotGreaterThan(r *Relation, attr string, key Value, kind reflect.Kind) []int {
	return r.searchLessThan(attr, key, true, kind)
}
func SearchGreaterThan(r *Relation, attr string, key Value, kind reflect.Kind) []int {
	return r.searchGreaterThan(attr, key, false, kind)
}
func SearchNotLessThan(r *Relation, attr string, key Value, kind reflect.Kind) []int {
	return r.searchGreaterThan(attr, key, true, kind)
}
func SearchMulti(r *Relation, attr string, key Value, kind reflect.Kind) []int {
	return r.multiSearch(attr, key, kind)
}

/*
	">":  GreaterThan,
	">=": NotLessThan,
	"<":  LessThan,
	"<=": NotGreaterThan,
	"==": Equal,
	"!=": NotEqual,
*/
type IndexedOperator func(*Relation, string, Value, reflect.Kind) []int

func (ss *IndexedOperator) UnmarshalJSON(data []byte) error {
	// Extract the string from data.
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("IndexedOperator should be a string, got %s", data)
	}

	// The rest is equivalen to Operator.
	got, ok := map[string]IndexedOperator{
		">":  SearchGreaterThan,
		">=": SearchNotLessThan,
		"<":  SearchLessThan,
		"<=": SearchNotGreaterThan,
		"==": SearchMulti,
	}[s]
	if !ok {
		return fmt.Errorf("invalid IndexedOperator %q", s)
	}
	*ss = got
	return nil
}
