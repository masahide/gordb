// go-rdb
package core

const TupleCapacity = 100

func StreamToRelation(s Stream, n *Node) (*Relation, error) {
	err := s.Init(n)
	if err != nil {
		return nil, err
	}
	result := &Relation{
		Attrs: make(Schema, 0, TupleCapacity),
		Data:  make([]Tuple, 0, TupleCapacity),
	}
	lastRow := NewTuple()
	for s.HasNext() {
		row, err := s.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		lastRow = row
		result.Data = append(result.Data, *lastRow)
	}
	result.Attrs = lastRow.Attrs()
	s.Close()
	return result, nil
}

func makeValues(t *Tuple) []Value {
	m := make([]Value, 0, t.Len())
	for _, col := range t.Attrs() {
		m = append(m, t.Get(col.Name))
	}
	return m
}
