package daemon

import (
	"log"
	"net/http"
	"path"
	"time"

	"github.com/masahide/gordb/core"
	"github.com/masahide/gordb/input/csv"
	"golang.org/x/net/context"
)

type Request struct {
	Query core.Stream
	Path  string
	ResCh chan Response
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
		err = d.BroadcastManageReq(ManageRequest{Cmd: PutNode, Path: name, Name: name, Node: node})
		if err != nil {
			return err
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/query", d.Handler) // ハンドラを登録してウェブページを表示させる
	s := &http.Server{
		Addr:           d.Listen,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Printf("listen: %s", d.Listen)
	err = s.ListenAndServe()
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
