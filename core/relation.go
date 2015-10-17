package core

import "sort"

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

/*
IndexedCSVRelationalStream.prototype.createIndex = function (dataTypes) {
	var staticIndex = {},
	i,
	j,
	key;

	for (i = 0; i < this.header.length; i++) {
		var arry = [];

		for (j = 1; j < this.data.length; j++) {
			switch (dataTypes[i]) {
			case 'Number':
				key = Number(this.data[j][i]);
				break;
			case 'String':
				key = this.data[j][i];
				break;
			case 'Date':
				key = new Date(this.data[j][i]).getTime();
				break;
			default:
				throw new TypeError('invalid type is specified');
				break;
			}
			arry.push({
				key: key,
				pointer: j
			});
		}

		arry = arry.sort(function (a, b) {
			if (a.key < b.key) return -1;
			if (a.key > b.key) return 1;
			if (a.pointer < b.pointer) return -1;
			if (a.pointer > b.pointer) return 1;
			return 0;
		});

		staticIndex[this.header[i]] = arry;
	}

	this.staticIndex = staticIndex;
};
*/
