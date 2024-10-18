package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
)

const WORKER_COUNT = 1000

type measurementDetails struct {
	min   float64
	max   float64
	count int
	total float64
}

var wg sync.WaitGroup
var detailsMap = make(map[string]measurementDetails)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func processData(data []string, mutex *sync.Mutex) {
	cityName := data[0]
	reading, err := strconv.ParseFloat(data[1], 64)
	if err != nil {
		panic(fmt.Sprintf("Error converting value %s with error %v", data[1], err))
	}

	mutex.Lock()
	defer mutex.Unlock()
	value, ok := detailsMap[cityName]
	if !ok {
		// City details doesn't exist yet. Create new one.
		measurement := measurementDetails{
			min:   reading,
			max:   reading,
			count: 1,
			total: reading,
		}
		detailsMap[cityName] = measurement
		return
	}

	existingReading := value
	existingReading.count += 1
	if reading < existingReading.min {
		existingReading.min = reading
	}

	if reading > existingReading.max {
		existingReading.max = reading
	}

	existingReading.total += reading
}

func roundToTwoDecimals(value float64) float64 {
	return math.Round(value*100) / 100
}

func worker(cityChannel <-chan string, i int, mutex *sync.Mutex) {
	for {
		select {
		case data, ok := <-cityChannel:
			if !ok {
				wg.Done()
				return
			}
			temperatureDetails := strings.Split(data, ";")
			processData(temperatureDetails, mutex)
		}
	}
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// ... rest of the program ...

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	filePath := os.Getenv("MEASUREMENT_FILE_PATH")
	if filePath == "" {
		panic("MEASUREMENT_FILE_PATH environment variable not found.")
	}
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Something went wrong while opening file.")
		panic(err)
	}
	defer file.Close()

	cityDetailsChan := make(chan string, 100)
	var mutex sync.Mutex
	scanner := bufio.NewScanner(file)

	// Start profiling
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// Start workers
	for i := 0; i < WORKER_COUNT; i++ {
		wg.Add(1)
		go worker(cityDetailsChan, i, &mutex)
	}

	// Read data.
	go func() {
		for scanner.Scan() {
			cityDetailsChan <- scanner.Text()
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}
		defer close(cityDetailsChan)
	}()

	wg.Wait()

	for key, value := range detailsMap {
		measurementDetail := value
		mean := measurementDetail.total / measurementDetail.total
		fmt.Printf("%s=%.2f/%.2f/%.2f,", key, measurementDetail.min, mean, measurementDetail.max)
	}
}
