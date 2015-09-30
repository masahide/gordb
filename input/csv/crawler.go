package csv

import (
	"fmt"
	"io/ioutil"
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
				return fmt.Errorf("LoadCsv file:%s err:%s", objectPath, err)
			}
			rPath := path.Dir(strings.TrimPrefix(objectPath, root))
			rel.Name = strings.TrimSuffix(path.Base(objectPath), filepath.Ext(objectPath))
			node.SetRelation(rPath, rel)
		}
		return nil
	})
	return node, err
}

func SearchDir(dir string) ([]string, error) {
	dirs := make([]string, 0, 10)
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("not dir: %s", dir)
	}
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, finfo := range fis {
		if finfo.IsDir() {
			dirs = append(dirs, finfo.Name())
		}
	}
	return dirs, nil
}
