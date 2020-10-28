package text

import (
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

func Commify(inVal int) []byte {
	return Commify64(int64(inVal))
}

func Commify64(inVal int64) []byte {
	inStr := strconv.FormatInt(inVal, 10)

	outStr := make([]byte, 0, 20)
	i := 1

	if inVal < 0 {
		outStr = append(outStr, '-')
		i++
	}

	for i <= len(inStr) {
		outStr = append(outStr, inStr[i-1])

		if i < len(inStr) &&
			((len(inStr)-i)%3) == 0 {
			outStr = append(outStr, ',')
		}

		i++
	}

	return outStr
}

func AvailableMapKeysList(m interface{}) []string {
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

func AvailableMapKeys(m interface{}) string {
	avail := AvailableMapKeysList(m)
	for i := range avail {
		avail[i] = ("'" + avail[i] + "'")
	}
	return strings.Join(avail, ", ")
}
