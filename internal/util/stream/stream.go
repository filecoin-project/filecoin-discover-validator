package stream

import (
	"fmt"
	"os"
	"strconv"

	"github.com/mattn/go-isatty"
)

func IsTTY(maybeFile interface{}) bool {
	// not a file => not a TTY
	if f, isFile := maybeFile.(*os.File); isFile {
		return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
	}
	return false
}

// FileHandleOptimizations is populated by individual OS-specific init()s
var ReadOptimizations, WriteOptimizations []FileHandleOptimization

type FileHandleOptimization struct {
	Name   string
	Action func(
		handle *os.File,
		stat os.FileInfo,
	) error
}

// This is a surprisingly cheap and reliable way to emulate a part of unsafe.*
// Use this for various optimization syscalls (in platform-specific util's)
// Saves us from pulling unsafe proper, which is known to make folks go 😱🙀🤮
func _addressofref(val interface{}) uintptr {
	a, _ := strconv.ParseUint(fmt.Sprintf("%p", val), 0, 64)
	return uintptr(a)
}
