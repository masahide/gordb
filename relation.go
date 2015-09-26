package gordb

import (
	"fmt"
	"path"
	"strings"
)

type Relation struct {
	index int
	Name  string `json:"name"`
	Attrs Schema
	Data  [][]Value
}

func (r *Relation) HasNext() bool {
	return r.index < len(r.Data)
}
func (r *Relation) Close() {
	r.index = 0
}

func (r *Relation) Next() *Tuple {
	tuple := NewTuple()
	for i, attr := range r.Attrs {
		tuple.Set(attr, r.Data[r.index][i])
	}
	r.index++
	return tuple
}

func (r *Relation) Copy() *Relation {
	return &Relation{
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

type Relations map[string]Relation
type Nodes map[string]*Node

type Node struct {
	Name string
	Nodes
	Relations
}

func (n *Node) GetRelation(fullPath string) (*Relation, error) {
	fullPath = strings.TrimLeft(fullPath, "/")
	dir := path.Dir(fullPath)
	if dir == "." {
		base := path.Base(fullPath)
		r, ok := n.Relations[base]
		if !ok {
			return nil, fmt.Errorf("dir not found:%s fullPath:%s", base, fullPath)
		}
		return &r, nil
	}

	s := strings.Split(strings.TrimLeft(dir, "/"), "/")
	top := s[0]
	if top == "." || top == "" {
		return nil, fmt.Errorf("unkown top dir :%s fullPath:%s", top, fullPath)
	}
	d, ok := n.Nodes[top]
	if !ok {
		return nil, fmt.Errorf("node not found: :%s fullPath:%s", top, fullPath)
	}
	return d.GetRelation(strings.TrimLeft(fullPath, top))
}
