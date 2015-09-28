package daemon

import (
	"fmt"
	"log"

	"github.com/masahide/gordb/core"
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
	MaxBuffer   chan int
	MngQ        []chan ManageRequest
}

func (d *Daemon) Worker(ctx context.Context, ManageCh chan ManageRequest) error {
	node := core.NewNode("root")
	for {
		select {
		case req := <-d.Queue:
			res := d.work(req, node)
			req.ResCh <- res
			if res.Err != nil {
				log.Printf("work err: %s", res.Err)
			}
		case req := <-ManageCh:
			res := d.manageWork(req, node)
			req.ResCh <- res
			if res.Err != nil {
				log.Printf("manageWork err: %s", res.Err)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *Daemon) work(req Request, node *core.Node) Response {
	res := Response{}
	n, ok := node.Nodes[req.Path]
	if !ok {
		res.Err = fmt.Errorf("request.Path not found: %s", req.Path)
		return res
	}
	res.Result, res.Err = core.StreamToRelation(req.Query, n)
	return res
}
