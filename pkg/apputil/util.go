package apputil

import "time"

func SleepCanClose(sleepTime time.Duration, closeCh chan struct{}) {
	ticker := time.NewTicker(sleepTime)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		return
	case <-closeCh:
		return
	}
}
