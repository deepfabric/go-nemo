package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	gonemo "github.com/deepfabric/go-nemo"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var pLength = flag.Int("length", 10, "length for the hash table member key")
var pkNum = flag.Int("n", 1000, "max count for the hash table name")
var pCnt = flag.Int("count", 10000, "query time")
var pThread = flag.Int("p", 1, "parallel thread")

var alphabet = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

func genString(length int) []byte {
	str := make([]byte, length)
	for i := 0; i < len(str); i++ {
		str[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return str
}

func opThread(iSumTime []int64, thread int, done chan int, n *gonemo.NEMO) {
	iSumTime[thread] = 0
	for i := 0; i < *pCnt; i++ {
		HKey := append([]byte("hash_key:"), []byte(strconv.Itoa(rand.Intn(*pkNum)))...)
		field := genString(*pLength)
		value := genString(*pLength)
		t1 := time.Now().UnixNano()
		_, err := n.HSet(HKey, field, value)
		t2 := time.Now().UnixNano()
		iSumTime[thread] += t2 - t1
		if err != nil {
			fmt.Println("HSet Err!")
		}
	}
	fmt.Print("Thread done:")
	fmt.Println(thread)
	done <- 1
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	opts := gonemo.NewOptions()
	n := gonemo.OpenNemo(opts, "./tmp/")
	rand.Seed(time.Now().Unix())

	go func() {
		log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
	}()

	threads := *pThread
	SumTime := make([]int64, threads)

	done := make(chan int)

	for thread := 0; thread < threads; thread++ {
		go opThread(SumTime, thread, done, n)
		fmt.Print("Thread:")
		fmt.Println(thread)
	}

	for thread := 0; thread < threads; thread++ {
		<-done
		fmt.Println("main thread recieve from chan")
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
	n.Close()
	var sum int64 = 0
	for i, _ := range SumTime {
		sum += SumTime[i]
	}
	fmt.Print("total time:")
	fmt.Println(sum)
	fmt.Print(float32(sum) / float32(*pCnt))
	fmt.Println(" per ops")
	fmt.Print("QPS: ")
	fmt.Println(float32(*pCnt) / float32(sum) * 1000000000)
}
