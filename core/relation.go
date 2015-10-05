package core

import (
	"reflect"

	"github.com/google/btree"
)

type Relation struct {
	Name  string  `json:"name,omitempty" `
	Attrs Schema  `json:"attrs"`
	Data  []Tuple `json:"data"`

	current int
}

const degree = 32

type Index struct {
	Name string
	Kind reflect.Kind
	*Relation
	Tree *btree.BTree
}

type TupleIndex struct {
	*Index
	*Tuple
	Value
}

func (a TupleIndex) Less(b btree.Item) bool {
	var kind reflect.Kind
	if a.Index != nil {
		kind = a.Kind
	} else {
		kind = b.(TupleIndex).Kind
	}
	ret, _ := LessThan(kind, a.Value, kind, b.(TupleIndex).Value)
	return ret
}

/*
type Sorted struct {
	Name   string
	Kind   reflect.Kind
	Tuples []Tuple
}
func (a *Sorted) Len() int      { return len(a.Tuples) }
func (a *Sorted) Swap(i, j int) { a.Tuples[i], a.Tuples[j] = a.Tuples[j], a.Tuples[i] }
func (a *Sorted) Less(i, j int) bool {
	if a.Kind == 0 {
		a.Kind = a.Tuples[i].Attrs.GetKind(a.Name)
	}
	ret, _ := GreaterThan(a.Kind, a.Tuples[i].Data[a.Name], a.Kind, a.Tuples[j].Data[a.Name])
	return ret
}
*/

func (r *Relation) NewIndex(name string) *Index {
	index := Index{
		Name:     name,
		Kind:     r.Attrs.GetKind(name),
		Relation: r,
		Tree:     btree.New(degree),
	}
	for _, tuple := range r.Data {
		index.Tree.ReplaceOrInsert(TupleIndex{&index, &tuple, tuple.Data[name]})
	}
	return &index
}

func (i *Index) AscendGreaterOrEqual(v Value) []Tuple {
	res := make([]Tuple, 0, i.Tree.Len())
	i.Tree.AscendGreaterOrEqual(TupleIndex{Value: v}, func(item btree.Item) bool {
		res = append(res, *item.(TupleIndex).Tuple)
		return true
	})
	return res
}
func (i *Index) AscendLessThan(v Value) []Tuple {
	res := make([]Tuple, 0, i.Tree.Len())
	i.Tree.AscendLessThan(TupleIndex{Value: v}, func(item btree.Item) bool {
		res = append(res, *item.(TupleIndex).Tuple)
		return true
	})
	return res
}

func (i *Index) Get(v Value) *Tuple {
	res := i.Tree.Get(TupleIndex{Value: v}).(TupleIndex)
	return res.Tuple
}

func (r *Relation) HasNext() bool {
	return r.current < len(r.Data)
}
func (r *Relation) Close() {
	r.current = 0
}

func (r *Relation) Next() (*Tuple, error) {
	tuple := &r.Data[r.current]
	r.current++
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
	r.current = 0
	rel, err := root.GetRelation(r.Name)
	if err != nil {
		return err
	}
	r.Attrs = rel.Attrs
	r.Data = rel.Data
	return nil
}
