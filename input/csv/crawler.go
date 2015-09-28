package csv

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/masahide/gordb/core"
)

const CsvExt = ".csv"

func Crawler(root string) (*core.Node, error) {
	root = strings.TrimRight(root, "/")
	name := path.Base(root)
	node := core.NewNode(name)
	err := filepath.Walk(root, func(objectPath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !f.IsDir() && strings.ToLower(filepath.Ext(objectPath)) == CsvExt {
			rel, err := LoadCsv(objectPath)
			if err != nil {
				return err
			}
			rPath := path.Dir(strings.TrimPrefix(objectPath, root))
			rel.Name = strings.TrimSuffix(path.Base(objectPath), filepath.Ext(objectPath))
			node.SetRelation(rPath, rel)
		}
		return nil
	})
	return node, err
}
