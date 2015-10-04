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
	schema := core.Schema{core.Attr{"name", reflect.String}, core.Attr{"age", reflect.Int64}, core.Attr{"job", reflect.String}}
	var want = &core.Relation{
		Name:  "rank",
		Attrs: schema,
		Data: []core.Tuple{
			core.Tuple{Attrs: schema, Data: map[string]core.Value{"name": "斎藤", "age": int64(30), "job": "エンジニア"}},
			core.Tuple{Attrs: schema, Data: map[string]core.Value{"name": "山田", "age": int64(25), "job": "デザイナー"}},
			core.Tuple{Attrs: schema, Data: map[string]core.Value{"name": "竹内", "age": int64(45), "job": "マネージャー"}},
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
