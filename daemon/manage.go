package daemon

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/masahide/gordb/core"
	"github.com/masahide/gordb/input/csv"
)

type ManageCmd uint

const (
	PutNode ManageCmd = 1 << iota
	DelNode
	GetNodeList
	GetNodes
)

type ManageRequest struct {
	Cmd   ManageCmd
	Name  string
	Path  string
	Node  *core.Node
	ResCh chan ManageResponse
}

type ManageResponse struct {
	Body interface{}
	Err  error
}

func (d *Daemon) ManageHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PostForm.Get("name")
	if name != "" {
		name = strings.TrimRight(path.Base(r.URL.Path), "/")
	}
	switch r.Method {
	case "PUT":
		node, err := csv.Crawler(r.URL.Path)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			err = fmt.Errorf("csv.Crawler err: %s", err)
			fmt.Fprintln(w, err)
			log.Println(err)
			return
		}
		err = d.BroadcastManageReq(ManageRequest{Cmd: PutNode, Path: r.URL.Path, Name: name, Node: node})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintln(w, err)
			return
		}
	case "DELETE":
		err := d.BroadcastManageReq(ManageRequest{Cmd: DelNode, Path: r.URL.Path, Name: name})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintln(w, err)
			return
		}
	case "GET":
		switch r.URL.Path {
		case "/list":
			res := d.sendManageReq(ManageRequest{Cmd: GetNodeList, Path: r.URL.Path, Name: name})
			if res.Err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintln(w, res.Err)
				return
			}
			json.NewEncoder(w).Encode(res.Body)
		}
		return

	}
	return
}

func (d *Daemon) BroadcastManageReq(req ManageRequest) error {
	for _, q := range d.MngQ {
		mr := req
		mr.ResCh = make(chan ManageResponse, 1)
		q <- mr
		res := <-mr.ResCh
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

func (d *Daemon) sendManageReq(req ManageRequest) ManageResponse {
	q := d.MngQ[0]
	mr := req
	mr.ResCh = make(chan ManageResponse, 1)
	q <- mr
	res := <-mr.ResCh
	return res
}

func (d *Daemon) manageWork(req ManageRequest, node *core.Node) ManageResponse {
	res := ManageResponse{}
	switch req.Cmd {
	case PutNode:
		node.Nodes[req.Name] = req.Node
		return res
	case DelNode:
		_, ok := node.Nodes[req.Name]
		if ok {
			delete(node.Nodes, req.Name)
		}
		log.Printf("Name Not found: %s", req.Name)
		return res
	case GetNodeList:
		list := make([]string, 0, len(node.Nodes))
		for name, _ := range node.Nodes {
			list = append(list, name)
		}
		sort.Strings(list)
		res.Body = list
		return res
	}
	return ManageResponse{Err: fmt.Errorf("Unkown ManageRequest:%v", req.Cmd)}
}
