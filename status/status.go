package status

import (
	"runtime"
	"time"
)

type Status struct {
	Time time.Time
	// see: https://golang.org/pkg/runtime/#MemStats
	runtime.MemStats
	NumGoroutine int
}

func Get() Status {
	s := Status{
		Time:         time.Now(),
		NumGoroutine: runtime.NumGoroutine(),
	}
	runtime.ReadMemStats(&s.MemStats)
	return s
}
