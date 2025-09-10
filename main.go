package main

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
)

const (
	chunkSize = 1000
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

	chunkPool := &sync.Pool{
		New: func() any {
			return &Chunk{
				Lines: make([][]byte, 0, chunkSize),
			}
		},
	}

	chunkCh := make(chan *Chunk, runtime.NumCPU())
	resultsCh := make(chan map[uint64]*Station)
	finalCh := make(chan map[uint64]*Station)

	var wg sync.WaitGroup
	for cpun := range runtime.NumCPU() {
		wg.Add(1)
		go func(chunkCh <-chan *Chunk, resultsCh chan<- map[uint64]*Station) {
			defer wg.Done()
			measurements := make(map[uint64]*Station, 128)
			i := 0
			for ch := range chunkCh {
				for _, l := range ch.Lines {
					if i%1_000_000 == 0 {
						fmt.Printf("cpu#%d - i=%d\n", cpun, i)
					}
					i++

					sepidx := bytes.IndexByte(l, ';')

					stationNameBytes := l[:sepidx]
					rawMeasure := l[sepidx+1:]

					h := hash(stationNameBytes)
					measure := fastFloatParse(rawMeasure)

					s, ok := measurements[h]
					if !ok {
						s = &Station{
							Name:  stationNameBytes,
							Max:   measure,
							Min:   measure,
							Sum:   measure,
							Count: 1,
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
				}

				ch.Lines = ch.Lines[:0]
				chunkPool.Put(ch)
			}
			fmt.Println("writing measurements")
			resultsCh <- measurements
			fmt.Println("measurements written")

		}(chunkCh, resultsCh)
	}

	go func() {
		combined := make(map[uint64]*Station, 512)
		for r := range resultsCh {
			for k, s := range r {
				if cs, ok := combined[k]; ok {
					cs.Sum += s.Sum
					cs.Count += s.Count
					if s.Min < cs.Min {
						cs.Min = s.Min
					}
					if s.Max > cs.Max {
						cs.Max = s.Max
					}
				} else {
					combined[k] = s
				}
			}
		}
		finalCh <- combined
	}()

	s := bufio.NewScanner(fd)
	// 256 MiB buffer
	bufCap := 256 * 1024 * 1024
	// set the max token size same as the buffer capacity to avoid reallocations
	buf := make([]byte, bufCap)
	s.Buffer(buf, bufCap)


	for {
		chunk := chunkPool.Get().(*Chunk)
		chunk.Lines = chunk.Lines[:0]
		for len(chunk.Lines) < chunkSize && s.Scan() {
			l := s.Bytes()
			line := make([]byte, len(l))
			copy(line, l)
			chunk.Lines = append(chunk.Lines, line)
		}

		// if no lines were scanned, that means the input has been totally consumed
		if len(chunk.Lines) == 0 {
			chunk.Lines = chunk.Lines[:0]
			chunkPool.Put(chunk)
			break
		}

		chunkCh <- chunk
	}

	close(chunkCh)

	fmt.Println("waiting for goroutines to finish")
	wg.Wait()
	close(resultsCh)
	combined := <-finalCh

	mslice := slices.Collect(maps.Values(combined))
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
	Name  []byte
	Avg   float32
	Min   int32
	Max   int32
	Sum   int32
	Count uint32
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
			measure += int32(b-'0') * int32(tenToThePowerOf(power))
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

type Chunk struct {
	Lines [][]byte
}

type Line struct {
	Bytes []byte
}
