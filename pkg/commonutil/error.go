package commonutil

import "errors"

var (
	ErrClosing  = errors.New("closing")
	ErrExist    = errors.New("exist")
	ErrNotExist = errors.New("not exist")
)
