package core

import (
	"fmt"
	"path"
	"strings"
)

type Node struct {
	Name string
	Nodes
	Relations
}

type Relations map[string]*Relation
type Nodes map[string]*Node

func (n *Node) GetRelation(fullPath string) (*Relation, error) {
	fullPath = strings.TrimLeft(fullPath, "/")
	dir := path.Dir(fullPath)
	if dir == "." {
		base := path.Base(fullPath)
		r, ok := n.Relations[base]
		if !ok {
			return nil, fmt.Errorf("dir not found:%s fullPath:%s", base, fullPath)
		}
		return r, nil
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
