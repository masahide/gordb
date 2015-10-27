// go-rdb
package core

const TupleCapacity = 100
const RowCapacity = 5000

func StreamToRelation(s Stream, n *Node) (*Relation, error) {
	return GetRelation(s, make([][]Value, 0, RowCapacity), n)
}

func GetRelation(s Stream, buf [][]Value, n *Node) (*Relation, error) {
	err := s.Init(n)
	if err != nil {
		return nil, err
	}
	result := &Relation{
		Attrs: NewSchema(),
		Data:  buf,
	}
	lastTuple := NewTuple()
	for s.HasNext() {
		tuple, err := s.Next()
		if err != nil {
			return nil, err
		}
		if tuple == nil {
			continue
		}
		lastTuple = tuple
		result.Data = append(result.Data, tuple.Data)
	}
	result.Attrs = lastTuple.Schema
	s.Close()
	return result, nil
}

/*
func makeValues(t *Tuple) []Value {
	m := make([]Value, 0, t.Len())
	for _, col := range t.Attrs {
		m = append(m, t.Get(col.Name))
	}
	return m
}
*/
