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

var alphabet = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

func genString(length int) []byte {
	str := make([]byte, length)
	for i := 0; i < len(str); i++ {
		str[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return str
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

	length := *pLength
	kNum := *pkNum
	cnt := *pCnt
	var SumTime int64
	opts := gonemo.NewOptions()
	n := gonemo.OpenNemo(opts, "./tmp/")
	rand.Seed(time.Now().Unix())

	go func() {
		log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
	}()

	for i := 0; i < cnt; i++ {
		HKey := append([]byte("hash_key:"), []byte(strconv.Itoa(rand.Intn(kNum)))...)
		field := genString(length)
		value := genString(length)
		t1 := time.Now().UnixNano()
		_, err := n.HSet(HKey, field, value)
		t2 := time.Now().UnixNano()
		SumTime += t2 - t1
		if err != nil {
			fmt.Println("HSet Err!")
		}
	}

	s := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		s[i] = genString(1234)
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
	fmt.Print("total time:")
	fmt.Println(SumTime)
	fmt.Print(float32(SumTime) / float32(cnt))
	fmt.Println(" per ops")
	fmt.Print("QPS: ")
	fmt.Println(float32(cnt) / float32(SumTime) * 1000000000)
}
