package core

import "testing"

func TestVtoFS(t *testing.T) {
	_, value := VtoFS(Value(int32(4)))
	if value.(float64) != float64(4) {
		t.Error("err int32")
	}
	_, value = VtoFS(Value(float32(4)))
	if value.(float64) != float64(4) {
		t.Error("err float32")
	}
	_, value = VtoFS(Value(uint(4)))
	if value.(float64) != float64(4) {
		t.Error("err uint")
	}
	_, value = VtoFS(Value(uint64(4)))
	if value.(float64) != float64(4) {
		t.Error("err uint64")
	}
	_, value = VtoFS(Value(uint32(4)))
	if value.(float64) != float64(4) {
		t.Error("err uint32")
	}
	_, value = VtoFS(Value(true))
	if value.(float64) != float64(1) {
		t.Error("err bool")
	}

}
