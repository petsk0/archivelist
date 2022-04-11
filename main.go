package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const numFields = 9
const startNumber = 20
const startInfo = 50
const endline = "\n"

type info struct {
	duration int
	quality  string
}

func main() {
	var inPath string
	var labelPath string
	var year int

	flag.StringVar(&inPath, "i", "", "path to input file")
	flag.StringVar(&labelPath, "l", "", "path to label file")
	flag.IntVar(&year, "y", time.Now().Year()-1, "year of archive list")
	flag.Parse()

	file, err := os.Open(labelPath)
	if err != nil {
		log.Fatal(err)
	}
	labelInfo := make(map[string]info)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) > 3 || len(fields) < 2 {
			log.Fatal("wrong label file format! should be <label duration quality \\n>")
		}
		if len(fields[0]) >= startNumber {
			log.Fatalf("name of label %s is too long! (max. %d characters)\n", fields[0], startNumber)
		}
		label := fields[0]
		dur, err := strconv.Atoi(fields[1])
		if err != nil {
			log.Fatal(err)
		}
		quality := ""
		if len(fields) == 3 {
			quality = fields[2]
		}
		_, ok := labelInfo[label]
		if ok {
			log.Fatalf("label %s declared more than once!\n", label)
		}
		labelInfo[label] = info{dur, quality}
	}
	file.Close()

	file, err = os.Open(inPath)
	if err != nil {
		log.Fatal(err)
	}
	records := make(map[string][]int)
	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		rawnum := fields[1]
		label := fields[4]
		_, ok := labelInfo[label]
		num, err := strconv.Atoi(rawnum[strings.IndexByte(rawnum, '/')+1:])
		if err != nil {
			log.Fatal(err)
		}
		_, ok = records[label]
		if !ok {
			records[label] = make([]int, 0, 128)
		}
		records[label] = append(records[label], num)
	}
	file.Close()

	var sb strings.Builder
	var wg sync.WaitGroup
	for k, v := range records {
		info := labelInfo[k]
		sb.WriteString(k)
		sb.WriteString(strings.Repeat(" ", startNumber-len(k)))
		sb.WriteString("OU-BS-OSZP-")
		sb.WriteString(strconv.Itoa(year))
		sb.WriteString("/")
		head := sb.String()
		sb.Reset()
		if info.quality != "" {
			sb.WriteString(strings.Repeat(" ", startInfo-(2*startNumber+2)))
			sb.WriteString(info.quality)
			sb.WriteString(" - ")
		} else {
			sb.WriteString(strings.Repeat(" ", startInfo-(2*startNumber)))
		}
		sb.WriteString(strconv.Itoa(info.duration))
		sb.WriteString(endline)
		tail := sb.String()
		sb.Reset()
		wg.Add(1)
		go writeList(&wg, k, v, head, tail)
	}
	wg.Wait()
}

func writeList(wg *sync.WaitGroup, label string, list []int, head string, tail string) {
	defer wg.Done()
	sort.Sort(sort.Reverse(sort.IntSlice(list)))
	file, err := os.Create(label + ".txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, num := range list {
		fmt.Fprintf(writer, "%s%06d%s", head, num, tail)
	}
	writer.Flush()
}
