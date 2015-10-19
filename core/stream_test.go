package core

import "testing"

func TestGetStream(t *testing.T) {
	s := &Stream{}

	res := s.getStream()
	if res != nil {
		t.Error(res)
	}

	s = &Stream{stream: &IndexedSelectionStream{}}
	res = s.getStream()
	if res == nil {
		t.Error("res == nil")
	}

}
func TestInitStream(t *testing.T) {
	s := &Stream{}
	err := s.Init(&Node{})
	if err != ErrUnkownStreamType {
		t.Error("err != ErrUnkownStreamType")
	}
}
