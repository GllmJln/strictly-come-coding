package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"sync"
)

type city struct {
	count int32
	total int32
	min   int32
	max   int32
	name  []byte
}

const (
	offset32 = 2166136261
	prime32  = 16777619
	mapSize  = 1 << 15
)

func main() {
	fileName := "measurements.txt"

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	chunks := 1

	s, _ := file.Stat()
	size := s.Size() / int64(chunks)

	resChan := make(chan city, 1000)

	wg := &sync.WaitGroup{}

	wg.Add(chunks)

	for i := 0; i < chunks; i++ {
		go processChunk(file, size*int64(i), size, resChan, wg)
	}

	go func() {
		wg.Wait()
		close(resChan)
	}()

	resultsMap := map[string]city{}

	for city := range resChan {
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
			c.min = city.max
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

func processChunk(file *os.File, offset, size int64, res chan city, wg *sync.WaitGroup) {
	defer wg.Done()

	file.Seek(offset, 0)
	rdr := io.LimitReader(file, size)

	buf := make([]byte, 1024*1024*8)

	cityMap := make([]city, mapSize)

	start := 0

	var done bool
	for !done {
		n, err := rdr.Read(buf[start:])
		if err != nil && err != io.EOF {
			return
		}

		chunk := buf[:start+n]

		idx := bytes.LastIndexByte(chunk, '\n')

		// read next new line
		if idx == -1 {
			buf = make([]byte, 100)
			file.Read(buf)
			idx = bytes.LastIndexByte(buf, '\n')
			chunk = buf
			done = true
		}

		remaining := chunk[idx+1:]

		// fist line picked up by previous routine, unless there isn't one
		if start == 0 && offset != 0 {
			continue
		}

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
