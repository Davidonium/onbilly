package main

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
	"os"
	"runtime/pprof"
	"slices"
	"strconv"
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

	s := bufio.NewScanner(bufio.NewReaderSize(fd, 256*1024*1024))

	measurements := make(map[string]*Station, 1024)

	i := 0
	for s.Scan() {
		l := s.Bytes()
		sep := bytes.IndexByte(l, ';')

		station := string(l[:sep])
		rawMeasure := string(l[sep+1:])
		parsed, err := strconv.ParseFloat(rawMeasure, 32)
		if err != nil {
			return fmt.Errorf("could not parse %s to float: %w", rawMeasure, err)
		}
		measure := float32(parsed)

		s, ok := measurements[station]
		if !ok {
			s = &Station{
				Name:     station,
				Measures: make([]float32, 0, 8192),
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
		station.Avg = station.Sum / float32(len(station.Measures))

		fmt.Printf("%s=%.1f/%.1f/%.1f", station.Name, station.Min, station.Avg, station.Max)

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
	Measures []float32
	Avg      float32
	Min      float32
	Max      float32
	Sum      float32
}
