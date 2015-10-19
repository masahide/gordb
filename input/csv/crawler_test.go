package csv

import (
	"reflect"
	"testing"

	"github.com/masahide/gordb/core"
)

func TestCsvCrawler(t *testing.T) {
	if _, err := Crawler("hoge"); err == nil {
		t.Error(err)
	}
	schema := core.Schema{
		Attrs: []core.Attr{core.Attr{"name", reflect.String}, core.Attr{"age", reflect.Int64}, core.Attr{"job", reflect.String}},
		Index: map[string]int{"name": 0, "age": 1, "job": 2},
	}
	var want = &core.Relation{
		Name:  "rank",
		Attrs: &schema,
		Data: [][]core.Value{
			[]core.Value{"斎藤", int64(30), "エンジニア"},
			[]core.Value{"山田", int64(25), "デザイナー"},
			[]core.Value{"竹内", int64(45), "マネージャー"},
		},
	}
	node, err := Crawler("../../test")
	if err != nil {
		t.Error(err)
	}
	staff2, err := node.GetRelation("/dir1/staff2")
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(staff2.Data, want.Data) {
		t.Errorf("Does not match staff2:%# v, want:%# v", staff2.Data, want.Data)
	}
}

func TestSearchDir(t *testing.T) {
	want := []string{"dir1", "dir2"}
	dirs, err := SearchDir("../../test")
	if err != nil {
		t.Error(err)
	}
	if _, err := SearchDir("../../test/rank2"); err == nil {
		t.Error(err)
	}
	if _, err := SearchDir("hoge"); err == nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(dirs, want) {
		t.Errorf("Does not match dirs:%#v, want:%#v", dirs, want)
	}

}

func TestinferenceType(t *testing.T) {
	if s, err := inferenceType(""); err != nil {
		t.Error(err)
	} else if s != 0 {
		t.Errorf("s !=0 s:%s", s)
	}
	if s, _ := inferenceType("string"); s != reflect.String {
		t.Errorf("s :%s", s)
	}
	if s, _ := inferenceType("1"); s != reflect.Int64 {
		t.Errorf("s :%s", s)
	}
	if s, _ := inferenceType("0.1"); s != reflect.Float64 {
		t.Errorf("s :%s", s)
	}
}
