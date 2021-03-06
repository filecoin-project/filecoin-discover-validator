// +build profile

package profiler

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"
)

func init() {
	StartStop = setupProfiling
}

var profileOutDir string

func setupProfiling() (profilingStopper func()) {

	if profileOutDir == "" {
		log.Fatalf(`The destination 'profileOutDir' is set to an empty string: you need to build with -ldflags="-X '...profileOutDir=...'"`)
	}

	pathPrefix := profileOutDir + "/" + time.Now().Format("2006-01-02_15-04-05.000")
	var toFlush []*os.File

	cpuProfFh := openProfHandle("cpu", pathPrefix, &toFlush)
	allocsProfFh := openProfHandle("allocs", pathPrefix, &toFlush)

	runtime.GC() // recommended by https://golang.org/pkg/runtime/pprof/#hdr-Profiling_a_Go_program
	runtime.GC() // recommended harder by @warpfork and @kubuxu :cryingbear:

	if err := pprof.StartCPUProfile(cpuProfFh); err != nil {
		log.Fatalf(
			"Unable to start CPU profiling: %s",
			err,
		)
	}

	// this function is a closer, no aborts on errors
	return func() {

		pprof.StopCPUProfile()
		runtime.GC() // recommended by https://golang.org/pkg/runtime/pprof/#hdr-Profiling_a_Go_program
		runtime.GC() // recommended harder by @warpfork and @kubuxu :cryingbear:

		if err := pprof.Lookup("allocs").WriteTo(allocsProfFh, 0); err != nil {
			log.Printf(
				"Error writing out 'allocs' profile: %s",
				err,
			)
		}

		for _, fh := range toFlush {
			if err := fh.Close(); err != nil {
				log.Printf(
					"Closing %s failed: %s",
					fh.Name(),
					err,
				)
			}
		}
	}
}

func openProfHandle(profName string, pathPrefix string, toFlush *[]*os.File) (fh *os.File) {
	filename := fmt.Sprintf("%s_%s.prof", pathPrefix, profName)

	var err error

	if fh, err = os.OpenFile(
		filename,
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0640,
	); err != nil {
		log.Fatalf(
			"Unable to open '%s' profile output: %s",
			profName,
			err,
		)
	}

	// if we managed to open it - we also symlink it right away
	os.Symlink(
		filepath.Base(fh.Name()),
		fh.Name()+".templnk",
	)
	os.Rename(
		fh.Name()+".templnk",
		fmt.Sprintf(
			"%s/latest_%s.prof",
			filepath.Dir(fh.Name()),
			profName,
		),
	)

	*toFlush = append(*toFlush, fh)

	return fh
}
