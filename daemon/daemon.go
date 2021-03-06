package daemon

import (
	"log"
	"net"
	"net/http"
	"path"
	"time"

	"github.com/masahide/gordb/core"
	"github.com/masahide/gordb/input/csv"
	"golang.org/x/net/context"
	"golang.org/x/net/netutil"
)

type Request struct {
	Query core.Stream
	Name  string
	ResCh chan Response
	EndCh chan bool
}

type Response struct {
	Result *core.Relation
	Err    error
}

type Daemon struct {
	Config

	Queue       chan Request
	PoolCounter chan bool
	MaxWorker   chan int
	MngQ        []chan ManageRequest
}

func NewDaemon(conf Config) *Daemon {
	return &Daemon{
		Config: conf,
		Queue:  make(chan Request, conf.WorkerLimit),
		MngQ:   make([]chan ManageRequest, conf.WorkerLimit),
	}
}

func (d *Daemon) Serve(ctx context.Context) error {

	for i := 0; i < d.WorkerLimit; i++ {
		d.MngQ[i] = make(chan ManageRequest, 1)
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
		err = d.BroadcastManageReq(ManageRequest{Cmd: PostNode, Path: name, Name: name, Node: node})
		if err != nil {
			return err
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/query/", d.JsonHandler)
	mux.HandleFunc("/json/", d.JsonHandler)
	mux.HandleFunc("/php/", d.PhpHandler)
	s := &http.Server{
		Addr:           d.Listen,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	listener, err := net.Listen("tcp", d.Listen)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("listen: %s", d.Listen)
	//err = s.ListenAndServe()
	err = s.Serve(netutil.LimitListener(listener, d.ListenLimit))
	if err != nil {
		log.Printf("ListenAndServe err:%s", err)
	}
	return err
}

func (d *Daemon) UtilServe() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", d.ManageHandler)
	s := &http.Server{
		Addr:           d.ManageListen,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Printf("Managelisten: %s", d.ManageListen)
	err := s.ListenAndServe()
	if err != nil {
		log.Printf("ListenAndServe err:%s", err)
	}
}
