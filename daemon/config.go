package daemon

import (
	"net"
	"net/http"
	"time"
)

type Config struct {
	Listen        string
	ManageListen  string
	WorkerLimit   int
	BufferDefault int
	WorkerDefault int
}

var (
	defaultConfig = Config{
		Listen:        ":3050",
		ManageListen:  ":9089",
		WorkerLimit:   5000,
		WorkerDefault: 100,
		BufferDefault: 90000,
	}

	transporter = http.Transport{
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		Dial: (&net.Dialer{
			Timeout: 30 * time.Second,
			//	KeepAlive: 30 * time.Second,
		}).Dial,
		MaxIdleConnsPerHost: 32,
	}
)
