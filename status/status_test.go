package status

import "testing"

func TestGet(t *testing.T) {
	stat := Get()
	//json.NewEncoder(os.Stdout).Encode(stat)
	if stat.Alloc == 0 {
		t.Errorf("stat.Alloc == %v", stat.Alloc)
	}
}
