package main

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "failed running onebilly challenge: %v", err)
		os.Exit(1)
	}
}

func run(_ []string) error {
	fmt.Printf("Starting...\n")

	ppfd, err := os.Create("./var/cpu.pprof")
	if err != nil {
		return fmt.Errorf("could not open pprof file: %w", err)
	}

	defer ppfd.Close()

	pprof.StartCPUProfile(ppfd)
	defer pprof.StopCPUProfile()

	fd, err := os.Open("./measurements.txt")
	if err != nil {
		return fmt.Errorf("could not open measurements file: %w", err)
	}

	defer fd.Close()

	s := bufio.NewScanner(fd)
	buf := make([]byte, 256*1024*1024)
	s.Buffer(buf, 1024)

	measurements := make(map[string]*Station, 1024)

	i := 0
	for s.Scan() {
		l := s.Bytes()
		sep := bytes.IndexByte(l, ';')

		station := string(l[:sep])
		rawMeasure := l[sep+1:]

		measure := fastFloatParse(rawMeasure)

		s, ok := measurements[station]
		if !ok {
			s = &Station{
				Name:     station,
				Measures: make([]int32, 0, 8192),
				Max:      measure,
				Min:      measure,
				Sum:      measure,
			}
			measurements[station] = s
		} else {
			if measure > s.Max {
				s.Max = measure
			}
			if measure < s.Min {
				s.Min = measure
			}
			s.Sum += measure
		}

		s.Measures = append(s.Measures, measure)
		if i%1_000_000 == 0 {
			fmt.Printf("at %d.\n", i)
		}
		i++
	}

	mslice := slices.Collect(maps.Values(measurements))
	stationCount := len(mslice)
	slices.SortFunc(mslice, func(a, b *Station) int {
		return strings.Compare(a.Name, b.Name)
	})

	fmt.Printf("{")
	for i, station := range mslice {
		station.Avg = (float32(station.Sum) / float32(10)) / float32(len(station.Measures))

		fmt.Printf("%s=%.1f/%.1f/%.1f", station.Name, float32(station.Min)/10, station.Avg, float32(station.Max)/10)

		i++
		if i < stationCount {
			fmt.Printf(", ")
		}
	}
	fmt.Print("}\n")

	return nil
}

type Station struct {
	Name     string
	Measures []int32
	Avg      float32
	Min      int32
	Max      int32
	Sum      int32
}

func tenToThePowerOf(n uint32) uint32 {
	var r uint32 = 1
	for range n {
		r *= 10
	}

	return r
}

func fastFloatParse(b []byte) int32 {
	var sign int32 = 1
	if len(b) > 0 && b[0] == '-' {
		sign = -1

		b = b[1:]
	}

	var measure int32 = 0
	var power uint32 = 0
	for i := int32(len(b) - 1); i >= 0; i-- {
		b := b[i]
		if b >= '0' && b <= '9' {
			measure += (int32(b) - int32('0')) * int32(tenToThePowerOf(power))
			power++
		}
	}

	measure *= sign
	return measure
}
