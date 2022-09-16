package apputil

import (
	"time"
)

func SleepCanClose(sleepTime time.Duration, closeCh chan struct{}) {
	timer := time.NewTimer(sleepTime)
	defer timer.Stop()
	select {
	case <-timer.C:
		return
	case <-closeCh:
		return
	}
}
