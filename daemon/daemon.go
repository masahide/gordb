package daemon

import (
	"net/http"
	"sync"

	"github.com/masahide/gordb/core"
)

type Request struct {
	Type
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
	mu sync.Mutex

	Queue       chan Request
	httpClient  http.Client
	PoolCounter chan bool

	MaxWorker chan int
	MaxBuffer chan int
}
