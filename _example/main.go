// example
package main

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/k0kubun/pp"
	"github.com/masahide/gordb/core"
	"github.com/masahide/gordb/input/csv"
)

func main() {

	node, err := csv.Crawler("test")
	if err != nil {
		log.Fatalln(err)
	}
	const jsonStream = `{ 
		"union": {
			"input1": {"selection": {
				"input": { "relation": { "name": "dir1/staff2" } },
				"attr": "age",  "selector": ">=", "arg": 31
			}},
			"input2": {"selection": {
				"input": { "relation": { "name": "dir1/staff2" } },
				"attr": "name", "selector": "==", "arg": "山田"
			}}
		}
	}`
	m := core.Stream{}
	if err := json.NewDecoder(strings.NewReader(jsonStream)).Decode(&m); err != nil {
		log.Fatal(err)
	}
	result, err := core.StreamToRelation(m, node)
	if err != nil {
		log.Fatalln(err)
	}
	pp.Print(result)
}
