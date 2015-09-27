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
	FullPath Relations
}

type Relations map[string]*Relation
type FullPath map[string]*Relation
type Nodes map[string]*Node

func (n *Node) GetRelation(fPath string) (*Relation, error) {
	fPath = strings.TrimLeft(fPath, "/")
	r, ok := n.FullPath[fPath]
	if !ok {
		return n.SearchRelation(fPath)
	}
	return r, nil

}

func (n *Node) SearchRelation(fPath string) (*Relation, error) {
	fPath = strings.TrimLeft(fPath, "/")
	dir := path.Dir(fPath)
	if dir == "." {
		base := path.Base(fPath)
		r, ok := n.Relations[base]
		if !ok {
			return nil, fmt.Errorf("dir not found:%s fullPath:%s", base, fPath)
		}
		return r, nil
	}

	s := strings.Split(strings.TrimLeft(dir, "/"), "/")
	top := s[0]
	if top == "." || top == "" {
		return nil, fmt.Errorf("unkown top dir :%s fullPath:%s", top, fPath)
	}
	d, ok := n.Nodes[top]
	if !ok {
		return nil, fmt.Errorf("node not found: :%s fullPath:%s", top, fPath)
	}
	return d.SearchRelation(strings.TrimLeft(fPath, top))
}

func NewNode(name string) *Node {
	return &Node{
		Name:      name,
		Nodes:     Nodes{},
		Relations: Relations{},
		FullPath:  Relations{},
	}
}

func (n *Node) SetRelations(dir string, rels Relations) error {
	dir = strings.TrimLeft(dir, "/")
	s := strings.Split(dir, "/")
	for name, r := range rels {
		n.FullPath[path.Join(dir, name)] = r
	}
	if s[0] != "" {
		cdir := s[0]
		cn, ok := n.Nodes[cdir]
		if !ok {
			cn = NewNode(cdir)
			n.Nodes[cdir] = cn
		}
		return cn.SetRelations(path.Join(s[1:]...), rels)
	}
	for name, r := range rels {
		n.Relations[name] = r
	}
	return nil

}

func (n *Node) SetRelation(dir string, rel *Relation) error {
	dir = strings.TrimLeft(dir, "/")
	s := strings.Split(dir, "/")
	name := rel.Name
	n.FullPath[path.Join(dir, name)] = rel
	if s[0] != "" {
		cdir := s[0]
		cn, ok := n.Nodes[cdir]
		if !ok {
			cn = NewNode(cdir)
			n.Nodes[cdir] = cn
		}
		return cn.SetRelation(path.Join(s[1:]...), rel)
	}
	n.Relations[name] = rel
	return nil

}
