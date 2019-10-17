package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	Byte     int = 1
	KiloByte     = 1000 * Byte
	MegaByte     = 1000 * KiloByte
)

type blob struct {
	hash string
	size int
	path string
}

func main() {
	var minSize int
	flag.IntVar(&minSize, "size", 100*KiloByte,
		"the minimum size for a blob to be printed")
	flag.Parse()

	var wg sync.WaitGroup

	revList := exec.Command("git", "rev-list", "--objects", "--all")
	catFile := exec.Command("git", "cat-file",
		"--batch-check=%(objecttype) %(objectname) %(objectsize) %(rest)")

	r, w := io.Pipe()
	revList.Stdout = w
	catFile.Stdin = r

	rd, err := catFile.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(rd)

	go func() {
		wg.Add(1)
		defer wg.Done()

		blobs := make([]blob, 0)
		for scanner.Scan() {
			t := scanner.Text()
			if strings.HasPrefix(t, "blob") {
				parts := strings.SplitN(t, " ", 4)

				size, err := strconv.Atoi(parts[2])
				if err != nil {
					log.Fatal(err)
				}

				blob := blob{hash: parts[1], size: size, path: parts[3]}
				blobs = append(blobs, blob)
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		sort.Slice(blobs, func(i, j int) bool {
			return blobs[i].size > blobs[j].size
		})

		for _, blob := range blobs {
			if blob.size < minSize {
				break
			}

			fmt.Printf("%s %d %s\n", blob.hash, blob.size, blob.path)
		}
	}()

	revList.Start()
	catFile.Start()

	if err := revList.Wait(); err != nil {
		log.Fatal(err)
	}

	w.Close()

	if err := catFile.Wait(); err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
