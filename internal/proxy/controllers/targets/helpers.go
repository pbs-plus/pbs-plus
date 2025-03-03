//go:build linux

package targets

import (
	"strings"
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

func processTargets(all []types.Target, storeInstance *store.Store, workerCount int) {
	var wg sync.WaitGroup
	tasks := make(chan int, len(all)) // Channel to distribute tasks to workers

	// Start worker goroutines
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range tasks {
				if all[i].IsAgent {
					targetSplit := strings.Split(all[i].Name, " - ")
					arpcSess := storeInstance.GetARPC(targetSplit[0])
					if arpcSess != nil {
						var respBody arpc.MapStringStringMsg
						raw, err := arpcSess.CallMsgWithTimeout(3*time.Second, "ping", nil)
						if err != nil {
							continue
						}
						_, err = respBody.UnmarshalMsg(raw)
						if err == nil {
							all[i].ConnectionStatus = true
							all[i].AgentVersion = respBody["version"]
						}
					}
				}
			}
		}()
	}

	// Send tasks to the workers
	for i := range all {
		tasks <- i
	}
	close(tasks) // Close the task channel to signal workers to stop

	wg.Wait() // Wait for all workers to finish
}
