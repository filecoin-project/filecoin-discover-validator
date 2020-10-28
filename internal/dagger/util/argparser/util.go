package argparser

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"strconv"

	"github.com/pborman/getopt/v2"
	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
)

// ugly as sin due to lack of lookaheads :/
var indenter, nonOptIndenter, dashStripper *regexp.Regexp

func SubHelp(description string, optSet *getopt.Set) (sh []string) {

	if indenter == nil {
		indenter = regexp.MustCompile(`(?m)^([^\n])`)
		nonOptIndenter = regexp.MustCompile(`(?m)^\s{0,12}([^\s\n\-])`)
		dashStripper = regexp.MustCompile(`(?m)^(\s*)\-\-`)
	}

	sh = append(
		sh,
		string(indenter.ReplaceAll(
			[]byte(description),
			[]byte(`  $1`),
		)),
	)

	if optSet == nil {
		return sh
	}

	b := bytes.NewBuffer(make([]byte, 0, 1024))
	optSet.PrintOptions(b)

	sh = append(sh, "  ------------\n   SubOptions")
	sh = append(sh,
		string(dashStripper.ReplaceAll(
			nonOptIndenter.ReplaceAll(
				b.Bytes(),
				[]byte(`              $1`),
			),
			[]byte(`$1  `),
		)),
	)

	return sh
}

var maxPlaceholder *regexp.Regexp

func Parse(args []string, optSet *getopt.Set) (argErrs []string) {

	if maxPlaceholder == nil {
		maxPlaceholder = regexp.MustCompile(`\bMaxPayload\b`)
	}

	if err := optSet.Getopt(args, nil); err != nil {
		argErrs = append(argErrs, err.Error())
	}

	unexpectedArgs := optSet.Args()
	if len(unexpectedArgs) != 0 {
		argErrs = append(argErrs, fmt.Sprintf(
			"unexpected free-form parameter(s): %s...",
			unexpectedArgs[0],
		))
	}

	// going through the limits when we are already in error is too confusing
	if len(argErrs) > 0 {
		return
	}

	optSet.VisitAll(func(o getopt.Option) {
		if spec := []byte(reflect.ValueOf(o).Elem().FieldByName("name").String()); len(spec) > 0 {

			max := int((^uint(0)) >> 1)
			min := -max - 1

			if spec[0] == '[' && spec[len(spec)-1] == ']' {
				spec = maxPlaceholder.ReplaceAll(spec, []byte(fmt.Sprintf("%d", constants.MaxLeafPayloadSize)))

				if _, err := fmt.Sscanf(string(spec), "[%d:]", &min); err != nil {
					if _, err := fmt.Sscanf(string(spec), "[%d:%d]", &min, &max); err != nil {
						argErrs = append(argErrs, fmt.Sprintf("Failed parsing '%s' as '[%%d:%%d]' - %s", spec, err))
						return
					}
				}
			} else {
				// not a spec we recognize
				return
			}

			if !o.Seen() {
				argErrs = append(argErrs, fmt.Sprintf("a value for %s must be specified", o.LongName()))
				return
			}

			actual, err := strconv.ParseInt(o.Value().String(), 10, 64)
			if err != nil {
				argErrs = append(argErrs, err.Error())
				return
			}

			if actual < int64(min) || actual > int64(max) {
				argErrs = append(argErrs, fmt.Sprintf(
					"value '%d' supplied for %s out of range [%d:%d]",
					actual,
					o.LongName(),
					min, max,
				))
			}
		}
	})

	return
}
