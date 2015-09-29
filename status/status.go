package status

import "runtime"

type Status struct {
	// see: https://golang.org/pkg/runtime/#MemStats
	runtime.MemStats
	NumGoroutine int
}

func GetStatus() Status {
	s := Status{}
	runtime.ReadMemStats(&s.MemStats)
	return s
}
