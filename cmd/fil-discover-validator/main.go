package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cheggaaa/pb/v3"
	"github.com/filecoin-project/filecoin-discover-validator/internal/dagger"
	"github.com/filecoin-project/filecoin-discover-validator/internal/dagger/util/argparser"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/multiformats/go-multibase"
	"github.com/pborman/getopt/v2"
	"github.com/pborman/options"
	"github.com/segmentio/ksuid"
	"golang.org/x/sys/unix"
)

type config struct {
	optSet *getopt.Set
	Mount  string `getopt:"-m --mountpoint  The mountpoint of a Filecoin Discover hard drive you want to validate"`
	Help   bool   `getopt:"-h --help        Display help"`
}

type stats struct {
	DriveIdentifier    string
	ValidationStart    time.Time
	ValidationFinish   time.Time
	SoftFailures       int
	HardFailures       int
	Flawless           int
	CarfilesPerDataset map[string]int
	Carfiles           map[string]*carInfo
}

type DumboChecker struct {
	stats
	cfg       config
	drivePath string
}

type carInfo struct {
	FullPath  string
	DatasetID uint8
	ByteSize  int64

	ByteSizeValidated  bool
	CarHeaderValidated bool `json:",omitempty"`
	CommpValidated     bool `json:",omitempty"`

	SoftFails []string
	HardFails []string

	key [16]byte
}

var manifestID = "UU" + ksuid.New().String()

func fatalf(fmt string, args ...interface{}) {
	log.Fatalf(fmt+`
	

Please contact support by emailing discover-support@protocol.ai

	In your message please include:
	- The exact text appearing above
	- The serial number of the drive
	- The original order number for this hard drive
	- The code: `+manifestID+"\n\n", args...)

	os.Exit(1)
}

func main() {

	loadDatasetDescriptions()

	if runtime.GOOS != "linux" {
		log.Fatal("Unable to continue: this program is designed exclusively for the Linux OS")
	}

	dc := NewFromArgs(os.Args)

	dc.ValidationStart = time.Now()

	dc.resolveMountpoint()

	log.Printf("Processing Filecoin Discover drive %s", dc.DriveIdentifier)
	log.Printf("Gathering about 7,000 filenames from %s...", dc.drivePath)

	bar := pb.Full.Start(0).SetRefreshRate(5 * time.Second)

	nameExtract := regexp.MustCompile(`/(bafyr[a-z0-9A-Z]+)\.car$`)

	if err := filepath.Walk(
		dc.drivePath,
		func(path string, fi os.FileInfo, err error) error {

			if fi.Name() == "lost+found" {
				return filepath.SkipDir
			} else if err != nil {
				return err
			}

			if !fi.Mode().IsRegular() {
				return nil
			}

			f := nameExtract.FindStringSubmatch(path)
			if len(f) == 0 {
				return nil
			}

			cid, err := cid.Parse(f[1])
			if err != nil {
				fatalf("Undecodeable CID '%s': %s", f[1], err)
			}

			ci := carInfo{
				ByteSize:  fi.Size(),
				FullPath:  path[len(dc.drivePath)+1:],
				SoftFails: make([]string, 0),
				HardFails: make([]string, 0),
			}
			copy(ci.key[:], cid.Bytes()[len(cid.Bytes())-16:])

			known, exists := knownCars[ci.key]
			if !exists {
				dc.CarfilesPerDataset["UNKNOWN"] = dc.CarfilesPerDataset["UNKNOWN"] + 1
				ci.HardFails = append(ci.HardFails, "payload not found in the Filecoin Discover set")
			} else {
				ci.DatasetID = known.datasetID
				dc.CarfilesPerDataset[dataSets[known.datasetID]] = dc.CarfilesPerDataset[dataSets[known.datasetID]] + 1
				if int64(known.expectedSize) == ci.ByteSize {
					ci.ByteSizeValidated = true
				} else {
					ci.SoftFails = append(
						ci.HardFails,
						fmt.Sprintf("car file size does not match expected dynamo value %d", known.expectedSize),
					)
				}
			}

			dc.Carfiles[cid.String()] = &ci
			bar.Increment()
			return nil
		},
	); err != nil {
		fatalf("Error encountered while collecting list of available car files: %s", err)
	}

	bar.Finish()

	log.Printf("Found total of %d car files", len(dc.Carfiles))

	for _, k := range MapKeysList(dc.CarfilesPerDataset) {
		log.Printf("\t%d\tbelong to dataset\t%s\n", dc.CarfilesPerDataset[k], k)
	}

	bar = pb.Full.Start(len(dc.Carfiles)).SetRefreshRate(5 * time.Second)

	commpQueue := make(chan string, 20000)
	spotCheckQueue := make(chan string, 20000)
	var wg sync.WaitGroup

	log.Printf("Validating contents...")

	for key, carInfo := range dc.Carfiles {
		wg.Add(1)
		// always commP
		if len(carInfo.SoftFails) > 0 {
			commpQueue <- key
		} else {
			spotCheckQueue <- key
		}
	}

	go func() {
		for {

			var key string
			var isOpen bool

			// We manage to panic some of our tooling - catch things here
			defer func() {
				if r := recover(); r != nil {
					fatalf("unexpected panic() while processing '%s': %s", key, r)
				}
			}()

			select {
			case key, isOpen = <-commpQueue:
			default:
				key, isOpen = <-spotCheckQueue
			}

			if !isOpen {
				return
			}

			dc.Carfiles[key].CommpValidated = dc.validateCommP(key)
			bar.Increment()
			wg.Done()
		}
	}()

	wCount := 2
	for wCount > 0 {
		wCount--
		go func() {

			var key string
			var isOpen bool

			// We manage to panic some of our tooling - catch things here
			defer func() {
				if r := recover(); r != nil {
					fatalf("unexpected panic() while processing '%s': %s", key, r)
				}
			}()

			for {
				key, isOpen = <-spotCheckQueue
				if !isOpen {
					return
				}
				dc.Carfiles[key].CarHeaderValidated = dc.validateCarStructure(key)
				bar.Increment()
				wg.Done()
			}
		}()
	}

	wg.Wait()
	close(commpQueue)
	close(spotCheckQueue)
	bar.Finish()

	for _, ci := range dc.Carfiles {
		if len(ci.HardFails) > 0 {
			dc.HardFailures++
		} else {
			dc.Flawless++
		}
		if len(ci.SoftFails) > 0 {
			dc.SoftFailures++
		}
	}

	dc.ValidationFinish = time.Now()

	js, err := json.MarshalIndent(dc.stats, "", "  ")
	if err != nil {
		fatalf("JSON encoding failed: %s", err)
	}

	log.Print("Uploading report...")

	manifestID = ksuid.New().String()

	_, sss, _ := multibase.Decode("mQUtJQTNYQlFTVEJPVjJQTlNQRTU6bmwwTHZQT3d1SlJlbzl1cXI2VndmTTVtbXBSbkdCazlpOHc3VnFMZw")
	ss := strings.Split(string(sss), ":")
	if _, err := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Region:      aws.String("ap-east-1"),
			Credentials: credentials.NewStaticCredentials(ss[0], ss[1], ""),
		},
	))).PutObject(&s3.PutObjectInput{
		Bucket: aws.String("fil-discover-drive-validation"),
		Key:    aws.String("manifests/" + manifestID + ".json"),
		ACL:    aws.String("public-read"),
		Body:   bytes.NewReader(js),
	}); err != nil {
		fatalf("Unable to execute PUT request: %s", err)
	}

	if dc.Flawless > 6900 && dc.CarfilesPerDataset["UNKNOWN"] == 0 {
		log.Printf(`

=== <3 === <3 === <3 === <3 === <3 === <3 === <3 === <3 === <3 === <3 ===

Your Flecoin Discover drive %s has passed validation!

Your Drive Manifest ID is: %s

In order to finalize the association of this drive with your account, please
enter the Manifest ID '%s' in your onboarding dashboard
(use link from the original registration email)

=== <3 === <3 === <3 === <3 === <3 === <3 === <3 === <3 === <3 === <3 ===
`, dc.DriveIdentifier, manifestID, manifestID)
	} else {
		fatalf(`

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  X   X   X   X   X   X   X   X   X   X   X   X   X   X   X   X   X   X  

!!! UNFORTUNATELY DRIVE %s IS NOT WELL !!!

  X   X   X   X   X   X   X   X   X   X   X   X   X   X   X   X   X   X  

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
`, dc.DriveIdentifier)
		os.Exit(1)
	}
}

func (dc *DumboChecker) validateCommP(cidString string) (ok bool) {

	carInfo := dc.Carfiles[cidString]
	carHandle, err := os.Open(dc.drivePath + "/" + carInfo.FullPath)
	defer carHandle.Close()

	if err != nil {
		carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf("unable to open car file for reading: %s", err))
		return
	}

	dgr := dagger.NewFromArgv([]string{"welp", "--collectors=fil-commP"})
	commP, err := dgr.ProcessReader(carHandle)

	if err != nil {
		carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf("commP calculation failed: %s", err))
		return
	}

	known := knownCars[carInfo.key].commP
	if bytes.Equal(commP[len(commP)-16:], known[:]) {
		return true
	}

	carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf(
		"lower commP bytes of car '%x' do not match expected value '%x'",
		commP[len(commP)-16:],
		known,
	))
	return
}

func (dc *DumboChecker) validateCarStructure(cidString string) (ok bool) {
	carInfo := dc.Carfiles[cidString]
	carHandle, err := os.Open(dc.drivePath + "/" + carInfo.FullPath)
	defer carHandle.Close()

	if err != nil {
		carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf("unable to open car file for reading: %s", err))
		return
	}

	cr, err := car.NewCarReader(bufio.NewReaderSize(carHandle, 16<<20))
	if err != nil {
		carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf("car header parsing failed: %s", err))
		return
	}

	if cr.Header.Roots[0].String() != cidString {
		carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf(
			"car header root CID '%s' does not match expected CID '%s'",
			cr.Header.Roots[0].String(),
			cidString,
		))
		return
	}

	blockCount := 0
	for blockCount < 15 {
		_, err := cr.Next()
		if err == io.EOF {
			return
		} else if err != nil {
			carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf(
				"car file invalid around block #%d: %s",
				blockCount,
				err,
			))
			return
		}
		blockCount++
	}

	// make sure we can read some from the end, because we are awesome :(
	endReadSize := 1 << 20

	_, err = carHandle.Seek(carInfo.ByteSize-int64(endReadSize), io.SeekStart)
	if err != nil {
		carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf(
			"unable to seek to the end of the file: %s",
			err,
		))
		return
	}

	br := bufio.NewReaderSize(carHandle, endReadSize)

	_, err = br.Discard(endReadSize)
	if err != nil {
		carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf(
			"reading tail of file failed: %s",
			err,
		))
		return
	}

	_, err = br.Discard(1)
	if err != io.EOF {
		carInfo.HardFails = append(carInfo.HardFails, fmt.Sprintf(
			"expected EOF, but got: %s",
			err,
		))
		return
	}

	return true
}

var sernoExtractor = regexp.MustCompile(`\A/dev/disk/by-id/(.+)-part1\z`)
var stExtractor = regexp.MustCompile(`\A/dev/disk/by-id/(.+ST8000.+)-part1\z`)

func (dc *DumboChecker) resolveMountpoint() {
	abs, err := filepath.Abs(dc.cfg.Mount)
	if err != nil {
		fatalf("Determining absolute name of mountpoint '%s' failed: %s", dc.cfg.Mount, err)
	}

	dc.drivePath = abs

	lstat := new(unix.Stat_t)
	if err := unix.Lstat(abs, lstat); err != nil {
		fatalf("lstat() of mountpoint '%s' failed: %s", abs, err)
	}

	if unix.S_IFDIR != (lstat.Mode & unix.S_IFMT) {
		fatalf("The supplied mountpoint '%s' is not a directory", abs)
	}

	glb := "/dev/disk/by-id/*-part1"
	drives, err := filepath.Glob(glb)
	if err != nil ||
		len(drives) == 0 ||
		(len(drives) == 1 && drives[0] == glb) {
		fatalf("No filecoin discover drives seem to be attached to this machine: %s", err)
	}

	statParent := new(unix.Stat_t)
	if err := unix.Lstat(abs+"/..", statParent); err != nil {
		fatalf("lstat() of mountpoint parent failed: %s", err)
	}

	if _, err := os.Stat(abs + "/lost+found"); err != nil || statParent.Dev == lstat.Dev {
		fatalf("Mountpoint '%s' does not correspond to the root of a mounted Filecoin Discover drive", abs)
	}

	// Try to find the ST8000 name
	for _, d := range drives {
		statDev := new(unix.Stat_t)
		unix.Stat(d, statDev)
		if statDev.Rdev == lstat.Dev {
			serno := stExtractor.FindStringSubmatch(d)
			if len(serno) > 0 {
				dc.DriveIdentifier = serno[1]
				return
			}
		}
	}

	// Just find whatever
	for _, d := range drives {
		statDev := new(unix.Stat_t)
		unix.Stat(d, statDev)
		if statDev.Rdev == lstat.Dev {
			serno := sernoExtractor.FindStringSubmatch(d)
			if len(serno) > 0 {
				dc.DriveIdentifier = serno[1]
				return
			}
		}
	}

	fatalf("Mountpoint '%s' does not correspond to a mounted drive on this system...", abs)
}

func NewFromArgs(argv []string) (dc *DumboChecker) {

	dc = &DumboChecker{
		cfg: config{
			optSet: getopt.New(),
		},
		stats: stats{
			CarfilesPerDataset: make(map[string]int, 8),
			Carfiles:           make(map[string]*carInfo, 8000),
		},
	}

	cfg := &dc.cfg

	if err := options.RegisterSet("", cfg, cfg.optSet); err != nil {
		log.Fatalf("option set registration failed: %s", err)
	}
	cfg.optSet.SetParameters("")

	argParseErrors := argparser.Parse(argv, cfg.optSet)

	if len(cfg.Mount) == 0 {
		argParseErrors = append(argParseErrors, "The path of the Filecoin Discover drive mountpoint must be supplied")
	}

	if cfg.Help || len(argParseErrors) > 0 {
		cfg.usageAndExit(argParseErrors)
	}

	return
}

func (cfg *config) usageAndExit(errorStrings []string) {

	if len(errorStrings) > 0 {
		fmt.Fprint(os.Stderr, "\nFatal error parsing arguments:\n\n")
	}

	cfg.optSet.PrintUsage(os.Stderr)

	if len(errorStrings) > 0 {
		sort.Strings(errorStrings)
		fmt.Fprintf(
			os.Stderr,
			"\nFatal error parsing arguments:\n\t%s\n\n",
			strings.Join(errorStrings, "\n\t"),
		)
		os.Exit(2)
	}

	os.Exit(0)
}

func MapKeysList(m interface{}) []string {
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		log.Panicf("input type not a map: %v", v)
	}
	avail := make([]string, 0, v.Len())
	for _, k := range v.MapKeys() {
		avail = append(avail, k.String())
	}
	sort.Strings(avail)
	return avail
}
