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
	var want = &core.Relation{
		Name:  "rank",
		Attrs: core.Schema{core.Attr{"name", reflect.String}, core.Attr{"rank", reflect.Int64}},
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
