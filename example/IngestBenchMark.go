package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	gonemo "github.com/deepfabric/go-nemo"
)

var srcPath = flag.String("source-path", "/tmp/datagen/", "source nemo db path the test dump from")
var bakPath = flag.String("backup-path", "/tmp/datagen-bak/", "sst files dump path")
var targetPath = flag.String("target-path", "/tmp/ingest/", "target nemo db path will be ingested")
var keyRange = flag.String("key-range", "0:9", "dumped key range")

var optFile = flag.String("nemo-option", "./option.json", "option file for nemo")

func main() {

	flag.Parse()

	str := time.Now().Format("2006-01-02T15:04:05")
	str = strings.Replace(str, "-", "", -1)
	str = strings.Replace(str, ":", "", -1)
	logFilename := "ingest-benchmark-" + str + ".log"
	logFileHandle, err := os.Create(logFilename)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	logger := log.New(logFileHandle, "ingest benchmark", log.LstdFlags)

	keys := strings.Split(*keyRange, ":")
	if len(keys) != 2 {
		fmt.Printf("input argument key-range[%s] is wrong!\n", *keyRange)
		return
	}

	startKey := []byte(keys[0])
	endKey := []byte(keys[1])

	fmt.Printf("source nemo db path[%s]\n", *srcPath)
	fmt.Printf("dump path[%s]\n", *bakPath)
	fmt.Printf("ingest target nemo db path[%s]\n", *targetPath)
	fmt.Printf("dump key range start key[%s],end key[%s]\n", keys[0], keys[1])
	logger.Printf("source nemo db path[%s]\n", *srcPath)
	logger.Printf("dump path[%s]\n", *bakPath)
	logger.Printf("ingest target nemo db path[%s]\n", *targetPath)
	logger.Printf("dump key range start key[%s],end key[%s]\n", keys[0], keys[1])

	opts, jsonConf := gonemo.NewOptions(*optFile)
	if opts == nil {
		fmt.Println("nemo options init failed")
		return
	}

	logger.Printf("nemo conf:\n")
	logger.Printf("CreateIfMissing:	%t\n", jsonConf.CreateIfMissing)
	logger.Printf("WriteBufferSize:	%dMegaByte\n", jsonConf.WriteBufferSize)
	logger.Printf("MaxOpenFiles:	%d\n", jsonConf.MaxOpenFiles)
	logger.Printf("UseBloomfilter:	%t\n", jsonConf.UseBloomfilter)
	logger.Printf("WriteThreads:	%d\n", jsonConf.WriteThreads)
	logger.Printf("TargetFileSizeBase:	%dMegaByte\n", jsonConf.TargetFileSizeBase)
	logger.Printf("Compression:	%t\n", jsonConf.Compression)
	logger.Printf("MaxBackgroundFlushes:	%d\n", jsonConf.MaxBackgroundFlushes)
	logger.Printf("MaxBackgroundCompactions:	%d\n", jsonConf.MaxBackgroundCompactions)
	logger.Printf("MaxBytesForLevelMultiplier:	%d\n", jsonConf.MaxBytesForLevelMultiplier)

	n := gonemo.OpenNemo(opts, *srcPath)

	t1 := time.Now().UnixNano()
	err = n.RawScanSaveRange(*bakPath, startKey, endKey, true)
	t2 := time.Now().UnixNano()
	fmt.Printf("dump sst file spent time %d milli-seconds\n", (t2-t1)/1000000)
	logger.Printf("dump sst file spent time %d milli-seconds\n", (t2-t1)/1000000)
	n.Close()

	n = gonemo.OpenNemo(opts, *targetPath)

	t1 = time.Now().UnixNano()
	err = n.IngestFile(*bakPath)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to IngestFile")
	}
	t2 = time.Now().UnixNano()
	fmt.Printf("Ingest sst File spent time %d milli-seconds\n", (t2-t1)/1000000)
	logger.Printf("Ingest sst File spent time %d milli-seconds\n", (t2-t1)/1000000)
}
