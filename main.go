package main

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
	"os"
	"runtime/pprof"
	"slices"
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
	// 256 MiB buffer
	buf := make([]byte, 256*1024*1024)
	s.Buffer(buf, 1024)

	measurements := make(map[uint64]*Station, 1024)

	i := 0
	for s.Scan() {
		l := s.Bytes()
		sep := bytes.IndexByte(l, ';')

		station := l[:sep]
		rawMeasure := l[sep+1:]

		b := rawMeasure
		var sign int32 = 1
		if len(b) > 0 && b[0] == '-' {
			sign = -1

			b = b[1:]
		}

		var measure int32 = 0
		var power uint32 = 0
		for i := int32(len(b) - 1); i >= 0; i-- {
			b := b[i]
			// avoid '.'
			if b >= '0' && b <= '9' {
				// subtracting the ascii value of 0 to any digit transforms it to its value
				measure += int32(b - '0') * int32(tenToThePowerOf(power))
				power++
			}
		}

		measure *= sign

		h := hash(station)
		s, ok := measurements[h]
		if !ok {
			stationCopy := make([]byte, len(station))
			copy(stationCopy, station)
			s = &Station{
				Name:     stationCopy,
				Measures: make([]int32, 0, 20 * 1024),
				Max:      measure,
				Min:      measure,
				Sum:      measure,
			}
			measurements[h] = s
		} else {
			if measure > s.Max {
				s.Max = measure
			}
			if measure < s.Min {
				s.Min = measure
			}
			s.Sum += measure
			s.Count++
		}

		s.Measures = append(s.Measures, measure)
		// if i%1_000_000 == 0 {
		// 	fmt.Printf("at %d.\n", i)
		// }
		i++
	}

	mslice := slices.Collect(maps.Values(measurements))
	stationCount := len(mslice)
	slices.SortFunc(mslice, func(a, b *Station) int {
		return bytes.Compare(a.Name, b.Name)
	})

	fmt.Printf("{")
	for i, station := range mslice {
		station.Avg = (float32(station.Sum) / 10) / float32(station.Count)

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
	Name     []byte
	Measures []int32
	Avg      float32
	Min      int32
	Max      int32
	Sum      int32
	Count    uint32
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

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

func hash(b []byte) uint64 {
	var h uint64 = offset64
	for _, c := range b {
		h *= prime64
		h ^= uint64(c)
	}
	return h
}
