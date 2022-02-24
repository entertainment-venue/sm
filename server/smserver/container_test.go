package smserver

import (
	"github.com/entertainment-venue/sm/pkg/apputil"
	"go.uber.org/zap"
	"testing"
)

func Test_container_Close(t *testing.T) {
	lg, _ := zap.NewDevelopment()
	ctr := smContainer{
		lg:        lg,
		Container: &apputil.Container{},
		stopper:   &apputil.GoroutineStopper{},
	}
	ctr.Close()
	ctr.Close()
}
