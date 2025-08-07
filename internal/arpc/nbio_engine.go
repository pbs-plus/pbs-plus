package arpc

import (
	"sync"

	"github.com/lesismal/nbio"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var (
	nbioOnce   sync.Once
	nbioEngine *nbio.Engine
)

// getEngine lazily builds and starts the engine.
func getEngine() *nbio.Engine {
	nbioOnce.Do(func() {
		nbioEngine = nbio.NewEngine(nbio.Config{
			Network:            "tcp",
			Addrs:              []string{}, // client only needs dial, no listen
			MaxWriteBufferSize: 6 * 1024 * 1024,
		})

		nbioEngine.OnClose(func(c *nbio.Conn, err error) {
			if err != nil {
				syslog.L.Error(err).WithMessage("nbio closed").Write()
			}
		})
		if err := nbioEngine.Start(); err != nil {
			panic(err)
		}
	})
	return nbioEngine
}
