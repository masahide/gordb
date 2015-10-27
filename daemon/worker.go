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
	"github.com/wulijun/go-php-serialize/phpserialize"
	"golang.org/x/net/context"
)

type Options struct {
	core.PhpOptions
	OrderDesc  bool   `json:"order_by,omitempty"`
	OrderBy    string `json:"order_desc,omitempty"`
	QueryCache bool   `json:"query_cache,omitempty"`
}

type Query struct {
	Options     `json:"options"`
	core.Stream `json:"stream"`
}

type Querys []Query

type Worker struct {
	*Daemon
	DataBuf [][]core.Value
}

func NewWorker(d *Daemon) *Worker {
	return &Worker{
		Daemon:  d,
		DataBuf: make([][]core.Value, 0, core.RowCapacity),
	}

}

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
	var querys Querys
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&querys)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(fmt.Sprintf("json.Decode err:%s", err))
		return
	}
	elapsendJsonDecode := time.Now().Sub(startTime)
	relations, err := d.QueryStreams(name, querys)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(err.Error)
		return
	}
	elapsendQuery := time.Now().Sub(startTime) - elapsendJsonDecode
	json.NewEncoder(w).Encode(relations)
	elapsedAll := time.Now().Sub(startTime)
	if d.LogLevel > 0 {
		log.Printf("elapsed:%s, json decode:%s, query:%s, json encode:%s", elapsedAll, elapsendJsonDecode, elapsendQuery, elapsedAll-elapsendQuery-elapsendJsonDecode)
	}
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
	var querys Querys
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&querys)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s, _ := phpserialize.Encode(fmt.Sprintf("json.Decode err:%s", err))
		fmt.Fprint(w, s)
		return
	}
	elapsendJsonDecode := time.Now().Sub(startTime)
	relations, err := d.QueryStreams(name, querys)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s, e := phpserialize.Encode(err.Error())
		if e != nil {
			log.Printf("Err:%s. (%s)", err.Error(), e)
		}
		fmt.Fprint(w, s)
		return
	}
	elapsendQuery := time.Now().Sub(startTime) - elapsendJsonDecode
	phpArray, err := d.RelationsToPhpArray(relations, querys)
	if err != nil {
		s, e := phpserialize.Encode(err)
		if e != nil {
			log.Printf("Err:%s. (%s)", err, e)
		}
		fmt.Fprint(w, s)
	}
	fmt.Fprint(w, phpArray)
	elapsedAll := time.Now().Sub(startTime)
	log.Printf("elapsed:%s, json decode:%s, query:%s, php encode:%s", elapsedAll, elapsendJsonDecode, elapsendQuery, elapsedAll-elapsendQuery-elapsendJsonDecode)
	return

}

func (d *Daemon) QueryStreams(name string, querys Querys) (res []*core.Relation, err error) {
	result := make([]*core.Relation, len(querys))
	resChs := make([]chan Response, len(querys))
	for i, query := range querys {
		resChs[i] = make(chan Response, 1)
		d.Queue <- Request{Query: query.Stream, Name: name, ResCh: resChs[i]}
	}
	for i := 0; i < len(querys); i++ {
		res := <-resChs[i]
		if res.Err != nil {
			return nil, res.Err
		}
		result[i] = res.Result
	}
	return result, nil

}

func (d *Daemon) RelationsToPhpArray(rs []*core.Relation, querys Querys) (string, error) {
	phpArray := map[interface{}]interface{}{}
	for i, rel := range rs {
		phpArray[i] = rel.MarshalPHP(querys[i].PhpOptions)
	}
	return phpserialize.Encode(phpArray)
}

func (d *Daemon) Worker(ctx context.Context, ManageCh chan ManageRequest) {
	node := core.NewNode("root")
	worker := NewWorker(d)
	for {
		select {
		case req := <-d.Queue:
			worker.DataBuf = worker.DataBuf[0:0]
			res := worker.work(req, node)
			req.ResCh <- res
			if res.Err != nil {
				log.Printf("work err: %s", res.Err)
			}
		case req := <-ManageCh:
			res := worker.manageWork(req, node)
			req.ResCh <- res
			if res.Err != nil {
				log.Printf("manageWork err: %s", res.Err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (d *Worker) work(req Request, node *core.Node) Response {
	res := Response{}
	n, ok := node.Nodes[req.Name]
	if !ok {
		res.Err = fmt.Errorf("request.Name not found: %s", req.Name)
		return res
	}
	res.Result, res.Err = core.GetRelation(req.Query, d.DataBuf, n)
	return res
}
