package daemon

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/masahide/gordb/core"
	"golang.org/x/net/context"
)

func (d *Daemon) JsonHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	name := r.PostForm.Get("name")
	if name == "" {
		name = strings.TrimRight(path.Base(r.URL.Path), "/")
	}
	defer r.Body.Close()
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var streams []core.Stream
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&streams)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(fmt.Sprintf("json.Decode err:%s", err))
		return
	}
	elapsendJsonDecode := time.Now().Sub(startTime)
	relations, err := d.QueryStreams(name, streams)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(err.Error)
		return
	}
	elapsendQuery := time.Now().Sub(startTime) - elapsendJsonDecode
	json.NewEncoder(w).Encode(relations)
	elapsedAll := time.Now().Sub(startTime)
	log.Printf("elapsed:%s, json decode:%s, query:%s, json encode:%s", elapsedAll, elapsendJsonDecode, elapsendQuery, elapsedAll-elapsendQuery-elapsendJsonDecode)
	return

}

func (d *Daemon) PhpHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	name := r.PostForm.Get("name")
	if name == "" {
		name = strings.TrimRight(path.Base(r.URL.Path), "/")
	}
	defer r.Body.Close()
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var streams []core.Stream
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&streams)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(fmt.Sprintf("json.Decode err:%s", err))
		return
	}
	elapsendJsonDecode := time.Now().Sub(startTime)
	relations, err := d.QueryStreams(name, streams)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(err.Error)
		return
	}
	elapsendQuery := time.Now().Sub(startTime) - elapsendJsonDecode
	json.NewEncoder(w).Encode(relations)
	elapsedAll := time.Now().Sub(startTime)
	log.Printf("elapsed:%s, json decode:%s, query:%s, json encode:%s", elapsedAll, elapsendJsonDecode, elapsendQuery, elapsedAll-elapsendQuery-elapsendJsonDecode)
	return

}

func (d *Daemon) QueryStreams(name string, streams []core.Stream) (res []*core.Relation, err error) {
	result := make([]*core.Relation, len(streams))
	resChs := make([]chan Response, len(streams))
	for i, stream := range streams {
		resChs[i] = make(chan Response, 1)
		d.Queue <- Request{Query: stream, Name: name, ResCh: resChs[i]}
	}
	for i := 0; i < len(streams); i++ {
		res := <-resChs[i]
		if res.Err != nil {
			return nil, res.Err
		}
		result[i] = res.Result
	}
	return result, nil

}

func (d *Daemon) Worker(ctx context.Context, ManageCh chan ManageRequest) {
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
			return
		}
	}
}

func (d *Daemon) work(req Request, node *core.Node) Response {
	res := Response{}
	n, ok := node.Nodes[req.Name]
	if !ok {
		res.Err = fmt.Errorf("request.Name not found: %s", req.Name)
		return res
	}
	res.Result, res.Err = core.StreamToRelation(req.Query, n)
	return res
}
