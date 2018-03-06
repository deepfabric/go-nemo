package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	gonemo "github.com/deepfabric/go-nemo"
	"github.com/golang/glog"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var pKeyLen = flag.Int("k", 10, "length for key")
var pValLen = flag.Int("v", 20, "length for value")
var pValRandom = flag.Bool("r", false, "random value")
var pCnt = flag.Int("c", 10000, "query count")
var pThread = flag.Int("p", 1, "parallel thread")
var pBatchSize = flag.Int("b", 8, "batch write size")

var alphabet = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

func genString(length int, r *rand.Rand) []byte {
	str := make([]byte, length)
	for i := 0; i < len(str); i++ {
		str[i] = alphabet[r.Intn(len(alphabet))]
	}
	return str
}

func opThread(iSumTime []int64, thread int, done chan int, n *gonemo.NEMO) {
	iSumTime[thread] = 0
	r := rand.New(rand.NewSource(time.Now().Unix()))
	loops := (*pCnt) / (*pBatchSize)
	Keys := make([][]byte, *pBatchSize)
	Values := make([][]byte, *pBatchSize)

	statisticThreshold := 10000
	writeCount := 0
	var sumTime int64

	for i := 0; i < loops; i++ {
		for j := 0; j < *pBatchSize; j++ {
			Keys[j] = append([]byte(strconv.Itoa(thread*1000000+i)), genString(*pKeyLen, r)...)
			if *pValRandom == true {
				Values[j] = genString(*pValLen, r)
			} else {
				Values[j] = append([]byte(strconv.Itoa(i)), make([]byte, *pValLen)...)
			}
		}
		t1 := time.Now().UnixNano()
		err := n.MSet(Keys, Values)
		t2 := time.Now().UnixNano()

		latency := t2 - t1
		sumTime += latency
		writeCount += *pBatchSize

		iSumTime[thread] += latency
		if writeCount >= statisticThreshold {
			glog.V(2).Infof("per %v key sum time %v ms\n", statisticThreshold, sumTime/1000000)
			glog.Flush()
			writeCount = 0
			sumTime = 0
		}

		latencyMs := int(latency / 1000000)
		if latencyMs > 1 {
			glog.V(4).Infof("big latency[%v]ms\n", latencyMs)
			glog.Flush()
		}

		if err != nil {
			fmt.Println("Set Err!")
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

	_, err := exec.Command("bash", "-c", "rm -rf /tmp/kv_batch_bench/*").Output()
	if err != nil {
		fmt.Println(err)
		fmt.Println("delete nemo path error,exit!")
	}

	opts := gonemo.NewDefaultOptions()
	n := gonemo.OpenNemo(opts, "/tmp/kv_batch_bench")

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
	fmt.Println(float64(*pCnt*threads*threads) / float64(sum) * 1000000000)
}
