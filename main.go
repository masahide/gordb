// example
package main

import (
	"log"

	"github.com/k0kubun/pp"
	"github.com/masahide/gordb/core"
	"github.com/masahide/gordb/input/csv"
)

func main() {

	staff, err := csv.LoadCsv("test/staff.csv")
	if err != nil {
		log.Fatalln(err)
	}
	var testData = &core.Node{
		Name:      "root",
		Relations: core.Relations{"staff": *staff},
	}
	result, err := core.StreamToRelation(core.Stream{Relation: &core.Relation{Name: "staff"}}, testData)
	pp.Print(result)
}
