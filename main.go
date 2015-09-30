package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/net/context"

	"github.com/masahide/gordb/daemon"
)

var version = ""

func main() {
	var (
		ShowVersion bool
		ctx         context.Context
		cancel      context.CancelFunc
	)
	ctx, cancel = context.WithCancel(context.Background())
	flag.BoolVar(&ShowVersion, "version", ShowVersion, "show version")
	flag.Parse()
	if ShowVersion {
		fmt.Printf("version: %s\n", version)
		os.Exit(0)
	}
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)
	config, err := daemon.LoadConfig(filepath.Base(os.Args[0]) + ".toml")
	if err != nil {
		log.Fatalln(err)
	}
	daemon := daemon.NewDaemon(config)
	go daemon.UtilServe()
	if err := daemon.Serve(ctx); err != nil {
		log.Fatalln(err)
	}
	cancel()
}
