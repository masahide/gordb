package main

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/masahide/gordb/core"
	"github.com/masahide/gordb/daemon"
	"github.com/masahide/gordb/input/csv"
	"golang.org/x/net/context"
)

const (
	cpuprofile = "mycpu.prof"
	dirname    = "20151001"
	jsonStream = `[{ 
		"selection": {
			"input": { "relation": { "name": "csv/action" } },
			"attr": "action_id",  "selector": "==", "arg": 44
		}
	}]`
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

	f, err := os.Create(cpuprofile)
	if err != nil {
		log.Fatal(err)
	}
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
		err = d.BroadcastManageReq(daemon.ManageRequest{Cmd: daemon.PutNode, Path: name, Name: name, Node: node})
		if err != nil {
			return err
		}
	}
	startTime := time.Now()

	var streams []core.Stream
	dec := json.NewDecoder(strings.NewReader(jsonStream))
	err = dec.Decode(&streams)
	if err != nil {
		return err
	}
	rs := make([][]*core.Relation, 100)
	elapsendJsonDecode := time.Now().Sub(startTime)
	pprof.StartCPUProfile(f)
	for i := 0; i < 100; i++ {
		rs[i], err = d.QueryStreams(dirname, streams)
		if err != nil {
			return err
		}
	}
	pprof.StopCPUProfile()
	elapsendQuery := time.Now().Sub(startTime) - elapsendJsonDecode
	json.NewEncoder(os.Stdout).Encode(rs)
	elapsedAll := time.Now().Sub(startTime)
	log.Printf("elapsed:%s, json decode:%s, query:%s, json encode:%s", elapsedAll, elapsendJsonDecode, elapsendQuery, elapsedAll-elapsendQuery-elapsendJsonDecode)
	return nil
}
