package main

import (
	"encoding/json"
	"log"
	"path"
	"strings"
	"time"

	"github.com/masahide/gordb/core"
	"github.com/masahide/gordb/daemon"
	"github.com/masahide/gordb/input/csv"
	"golang.org/x/net/context"
)

const (
	Loop       = 1000
	cpuprofile = "mycpu.prof"
	dirname    = "20151001"
	querys1    = `[ {"stream":{ 
		"selection": {
			"input": { "relation": { "name": "csv/action" } },
			"attr": "action_id",  "selector": "==", "arg": 44
		}
	}}]`
	querys2 = `[ {"stream":{
		"selection": {
			"input": { "relation": { "name": "csv/status_ailments" } },
			"attr": "id",  "selector": "==", "arg": 8010020201
		}
	}}]`
)

func main() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	config := daemon.Config{
		Listen:        ":3050",
		ManageListen:  ":9089",
		WorkerLimit:   1,
		WorkerDefault: 1,
		LoadDir:       "prd",
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	d := NewDaemon(config)
	if err := d.Serve(ctx); err != nil {
		log.Fatalln(err)
	}

}

type Daemon struct {
	*daemon.Daemon
}

func NewDaemon(conf daemon.Config) *Daemon {
	return &Daemon{
		Daemon: &daemon.Daemon{
			Config: conf,
			Queue:  make(chan daemon.Request, conf.WorkerLimit),
			MngQ:   make([]chan daemon.ManageRequest, conf.WorkerLimit),
		},
	}
}

func (d *Daemon) Serve(ctx context.Context) error {

	/*
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
	*/
	for i := 0; i < d.WorkerLimit; i++ {
		d.MngQ[i] = make(chan daemon.ManageRequest, 1)
		go d.Worker(ctx, d.MngQ[i])
	}

	names, err := csv.SearchDir(d.LoadDir)
	if err != nil {
		return err
	}
	for _, name := range names {
		dir := path.Join(d.LoadDir, name)
		node, err := csv.Crawler(dir)
		if err != nil {
			return err
		}
		log.Printf("load dir:%s", dir)
		err = d.BroadcastManageReq(daemon.ManageRequest{Cmd: daemon.PostNode, Path: name, Name: name, Node: node})
		if err != nil {
			return err
		}
	}
	d.queryTest(Loop, querys1)
	d.queryTest(1, querys2)
	d.queryTest(1, querys1)
	return nil
}

func (d *Daemon) queryTest(loop int, jsontext string) error {
	startTime := time.Now()

	var querys daemon.Querys
	dec := json.NewDecoder(strings.NewReader(jsontext))
	err := dec.Decode(&querys)
	if err != nil {
		return err
	}
	rs := make([][]*core.Relation, loop)
	elapsendJsonDecode := time.Now().Sub(startTime)
	//pprof.StartCPUProfile(f)
	for i := 0; i < loop; i++ {
		rs[i], err = d.QueryStreams(dirname, querys)
		if err != nil {
			panic(err)
		}
	}
	//pprof.StopCPUProfile()
	elapsendQuery := time.Now().Sub(startTime) - elapsendJsonDecode
	//json.NewEncoder(os.Stdout).Encode(rs)

	elapsedAll := time.Now().Sub(startTime)
	log.Printf("elapsed:%s, json decode:%s, query:%s, json encode:%s,len(rs):%d", elapsedAll, elapsendJsonDecode, elapsendQuery, elapsedAll-elapsendQuery-elapsendJsonDecode, len(rs))
	return nil
}
