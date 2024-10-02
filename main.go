package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
)

var (
	workers = runtime.NumCPU()
)

type city struct {
	count int32
	total int32
	min   int32
	max   int32
	name  []byte
}

type readPos struct {
	start int64
	end   int64
}

const (
	offset32 = 2166136261
	prime32  = 16777619
	mapSize  = 1 << 16
	fileName = "measurements.txt"
)

func main() {

	cityResChan := make(chan city, 1000)
	posChan := make(chan *readPos, 5)

	go getReadPositions(posChan)

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for pos := range posChan {
		go processChunk(pos.start, pos.end, cityResChan, wg)
	}

	go func() {
		wg.Wait()
		close(cityResChan)
	}()

	resultsMap := map[string]city{}

	for city := range cityResChan {
		c, ok := resultsMap[string(city.name)]
		if !ok {
			resultsMap[string(city.name)] = city
			continue
		}
		c.count += city.count
		c.total += city.total
		if c.max < city.max {
			c.max = city.max
		}
		if c.min > city.min {
			c.min = city.min
		}
		resultsMap[string(city.name)] = c
	}

	results := make([]city, 0, 500)

	for _, c := range resultsMap {
		results = append(results, c)
	}

	slices.SortFunc(results, func(a, b city) int {
		return bytes.Compare(a.name, b.name)
	})

	outputFile, _ := os.Create("output.txt")
	defer outputFile.Close()

	buffer := bytes.NewBuffer(make([]byte, 0, 200))
	for _, city := range results {
		buffer.Write(city.name)
		buffer.WriteByte('=')
		buffer.WriteString(strconv.Itoa(int(city.min) / 10))
		buffer.WriteByte('.')
		remainder := city.min % 10
		if remainder < 0 {
			remainder = -remainder
		}
		buffer.WriteByte(byte(remainder) + '0')
		buffer.WriteByte('/')
		fmt.Fprintf(buffer, "%.1f", float64(city.total)/float64(city.count)/float64(10))
		buffer.WriteByte('/')
		buffer.WriteString(strconv.Itoa(int(city.max) / 10))
		buffer.WriteByte('.')
		remainder = city.max % 10
		if remainder < 0 {
			remainder = -remainder
		}
		buffer.WriteByte(byte(remainder) + '0')
		buffer.WriteByte('\n')
		outputFile.Write(buffer.Bytes())
		buffer.Reset()
	}
}

func getReadPositions(res chan *readPos) {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	s, _ := file.Stat()
	chunkSize := s.Size() / int64(workers)

	start := int64(0)
	buf := make([]byte, 100)

	for start < s.Size() {
		file.Seek(start+chunkSize, 0)
		n, _ := file.Read(buf)
		if n != 0 {
			n = bytes.IndexByte(buf[:n], '\n') + 1
		}
		res <- &readPos{start: start, end: chunkSize + int64(n)}
		start += chunkSize + int64(n)
	}

	close(res)
}

func processChunk(offset, size int64, res chan city, wg *sync.WaitGroup) {
	defer wg.Done()
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	file.Seek(offset, 0)
	rdr := io.LimitReader(file, size)

	buf := make([]byte, 1024*1024*8)

	cityMap := make([]city, mapSize)

	start := 0

	for {
		n, err := rdr.Read(buf[start:])
		if err != nil {
			break
		}

		chunk := buf[:start+n]

		idx := bytes.LastIndexByte(chunk, '\n')

		if idx == -1 {
			break
		}

		remaining := chunk[idx+1:]

		chunk = chunk[:idx+1]

		for {
			var name, rawTemp []byte
			// fnv
			hash := uint32(offset32)
			idx := 0
			for {
				if idx > len(chunk) {
					break
				}

				if len(chunk) == 0 {
					break
				}

				b := chunk[idx]
				if b == ';' {
					name = chunk[:idx]
					rawTemp = chunk[idx+1:]
					break
				}
				hash ^= uint32(b)
				hash *= prime32
				idx++
			}

			if idx == len(chunk) {
				break
			}

			var neg bool
			var i int

			if rawTemp[i] == '-' {
				neg = true
				i++
			}
			temp := int32(rawTemp[i] - '0')
			i++
			if rawTemp[i] != '.' {
				temp = temp*10 + int32(rawTemp[i]-'0')
				i++
			}
			i++
			temp = temp*10 + int32(rawTemp[i]-'0')

			i += 2
			if neg {
				temp = -temp
			}

			chunk = rawTemp[i:]

			mapIdx := hash % (mapSize - 1)

			for {
				if cityMap[mapIdx].name == nil {
					cityName := make([]byte, len(name))
					copy(cityName, name)
					c := &cityMap[mapIdx]
					c.min = temp
					c.max = temp
					c.count = 1
					c.total = temp
					c.name = cityName
					break
				}

				if bytes.Equal(cityMap[mapIdx].name, name) {
					c := &cityMap[mapIdx]
					if temp < c.min {
						c.min = temp
					}
					if temp > c.max {
						c.max = temp
					}
					c.total += temp
					c.count++
					break
				}

				mapIdx++
				if mapIdx == mapSize {
					mapIdx = 0
				}
			}
		}

		start = copy(buf, remaining)
	}

	for i := range cityMap {
		if cityMap[i].name == nil {
			continue
		}
		res <- cityMap[i]
	}
}
