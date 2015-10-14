package core

type Relation struct {
	index int
	Name  string  `json:"name,omitempty" `
	Attrs Schema  `json:"attrs"`
	Data  []Tuple `json:"data"`
}

func (r *Relation) HasNext() bool {
	return r.index < len(r.Data)
}
func (r *Relation) Close() {
	r.index = 0
}

func (r *Relation) Next() (*Tuple, error) {
	tuple := &r.Data[r.index]
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
		for i, tuple := range r.Data {
			res["Data"].(map[interface{}]interface{})[i] = tuple.MarshalPHP(o)
		}
	default:
		for i, tuple := range r.Data {
			key, ok := tuple.Data[o.MapKey]
			if ok {
				res["Data"].(map[interface{}]interface{})[key] = tuple.MarshalPHP(o)
			} else {
				res["Data"].(map[interface{}]interface{})[i] = tuple.MarshalPHP(o)
			}
		}
	}
	return res
}
