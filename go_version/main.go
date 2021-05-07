package main

import (
	"fmt"
	"github.com/buger/jsonparser"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)

type entry struct {
	id            string
	author_id     string
	author_name   string
	author_gender *string
	timestamp     string
	reactions     int64
	url           string
	comment       *string
}

func (e *entry) format() string {
	cond := func(s *string) string {
		if s != nil {
			return *s
		} else {
			return ""
		}
	}
	s := fmt.Sprintf("%s,%s,%s,%s,%v,%v,%s,%s",
		e.id, e.author_id, e.author_name, cond(e.author_gender), e.timestamp, e.reactions, e.url, cond(e.comment))
	return s
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func optional_string(values *[]byte, fields ...string) (*string, error) {
	val, err := jsonparser.GetString(*values, fields...)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

func format_date(values *[]byte, fields ...string) string {
	value, err := jsonparser.GetInt(*values, fields...)
	check(err)
	t := time.Unix(value, 0)
	return t.Format(time.RFC3339)
}

func parse_json(filename string, output *[]*entry, m *sync.Mutex, done chan<- bool, jobs chan int) {
	results := make([]*entry, 0)
	data, err := ioutil.ReadFile(filename)
	<-jobs
	check(err)
	value, _, _, err := jsonparser.Get(data, "data", "feedback", "display_comments", "edges")
	if err != nil {
		fmt.Println("Skipping ", filename)
	}
	jsonparser.ArrayEach(value, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		var e *entry = new(entry)
		node, _, _, err := jsonparser.Get(value, "node")
		check(err)
		e.id, _ = jsonparser.GetString(node, "id")
		e.author_id, err = jsonparser.GetString(node, "author", "id")
		e.author_name, _ = jsonparser.GetString(node, "author", "name")
		e.author_gender, _ = optional_string(&node, "author", "gender")
		e.timestamp = format_date(&node, "created_time")
		e.reactions, _ = jsonparser.GetInt(node, "feedback", "reactors", "count")
		e.url, _ = jsonparser.GetString(node, "url")
		e.comment, _ = optional_string(&node, "body", "text")

		results = append(results, e)
	})

	m.Lock()
	*output = append(*output, results...)
	m.Unlock()
	done <- true
}

func main() {
	args := os.Args;
	if len(args) < 2 {
		fmt.Println("Not enough arguments")
		return
	}
	datapath := args[1]

	// Output will contain all of our entries, each job will 
	// write into it using the mutex to avoid data races.
	output := make([]*entry, 0)
	m := &sync.Mutex{}

	files, err := ioutil.ReadDir(datapath)
	check(err)

	// Since in Go, goroutines are pseudo infinite, we can spin as many
	// workers as we want.
	workers := make(chan bool, len(files))

	// This makes sure we don't do more than 4 in parallel because the current OS
	// has a limit for how many files you can open.
	jobs := make(chan int, 4)

	start := time.Now()

	for i := 0; i < len(files); i++ {
		jobs <- i
		go parse_json(fmt.Sprintf("%s/%s", datapath, files[i].Name()), &output, m, workers, jobs)
	}

	for i := 0; i < len(files); i++ {
		<-workers
	}

	f, err := os.Create("output.csv")
	check(err)
	sort.Slice(output, func(i, j int) bool {
		return output[i].id < output[j].id
	})

	fmt.Println("Dumping the csv")
	f.Write([]byte("id,author_id,author_name,author_gender,timestamp,reactions,url,comment\n"))
	for _, e := range output {
		f.Write([]byte(fmt.Sprintf("%s\n", e.format())))
	}
	defer f.Close()

	end := time.Now()
	fmt.Printf("Speed: %.2f\n", (952.0 / end.Sub(start).Seconds()))
}
