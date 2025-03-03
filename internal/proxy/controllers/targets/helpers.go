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

func processTargets(all []types.Target, storeInstance *store.Store, maxConcurrency int) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency) // Semaphore to limit concurrency

	for i := range all {
		if all[i].IsAgent {
			wg.Add(1)
			sem <- struct{}{} // Acquire a slot in the semaphore

			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }() // Release the slot in the semaphore

				targetSplit := strings.Split(all[i].Name, " - ")
				arpcSess := storeInstance.GetARPC(targetSplit[0])
				if arpcSess != nil {
					var respBody arpc.MapStringStringMsg
					raw, err := arpcSess.CallMsgWithTimeout(3*time.Second, "ping", nil)
					if err != nil {
						return
					}
					_, err = respBody.UnmarshalMsg(raw)
					if err == nil {
						all[i].ConnectionStatus = true
						all[i].AgentVersion = respBody["version"]
					}
				}
			}(i)
		}
	}

	wg.Wait() // Wait for all goroutines to finish
}
