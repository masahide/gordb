package daemon

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Listen        string
	ManageListen  string
	WorkerLimit   int
	WorkerDefault int
	LoadDir       string
	ListenLimit   int
	LogLevel      int
}

func LoadConfig(filename string) (Config, error) {
	c := Config{}
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return c, fmt.Errorf("LoadConfig err: %s", err)
	}
	log.Printf("load config: \n%s\n", b)
	_, err = toml.Decode(string(b), &c)
	if err != nil {
		return c, fmt.Errorf("LoadConfig err: %s", err)
	}
	//log.Printf("Undecoded keys: %q\n", md.Undecoded())
	return c, nil
}
