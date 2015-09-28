package daemon

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
)
