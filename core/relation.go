package core

import (
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
			result = make([]int, to-from)
			for i := 0; i <= to-from; i++ {
				result[i] = arry[from+i].ptr
			}
			return result
		}
	}
	return result

}

/*

 */
