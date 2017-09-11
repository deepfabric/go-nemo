package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	gonemo "github.com/deepfabric/go-nemo"
	datagen "github.com/deepfabric/go-nemo/datagen"
)

var optFile = flag.String("nemo-option", "./option.json", "option file for nemo")

func main() {

	flag.Parse()

	str := time.Now().Format("2006-01-02T15:04:05")
	str = strings.Replace(str, "-", "", -1)
	str = strings.Replace(str, ":", "", -1)
	logFilename := "gendata-benchmark-" + str + ".log"
	logFileHandle, err := os.Create(logFilename)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	logger := log.New(logFileHandle, "", log.LstdFlags)

	cfg, err := datagen.LoadConfig(*datagen.CfgFile)
	if err == nil {
		fmt.Printf("generat data type hash:\n")
		fmt.Printf("key_length:	%d\n", cfg.Hash.KeyLen)
		fmt.Printf("field_length:	%d\n", cfg.Hash.FieldLen)
		fmt.Printf("value_length:	%d\n", cfg.Hash.ValueLen)
		fmt.Printf("table_count:	%d\n", cfg.Hash.TableCount)
		fmt.Printf("entry_count:	%d\n", cfg.Hash.EntryCount)
		fmt.Printf("thread count:	%d\n", cfg.Hash.ThreadNum)

		logger.Printf("generat data type hash:\n")
		logger.Printf("key_length:	%d\n", cfg.Hash.KeyLen)
		logger.Printf("field_length:	%d\n", cfg.Hash.FieldLen)
		logger.Printf("table_count:	%d\n", cfg.Hash.TableCount)
		logger.Printf("value_length:	%d\n", cfg.Hash.ValueLen)
		logger.Printf("entry_count:	%d\n", cfg.Hash.EntryCount)
		logger.Printf("thread count:	%d\n", cfg.Hash.ThreadNum)
	} else {
		return
	}

	opts, jsonConf := gonemo.NewOptions(*optFile)
	if opts == nil {
		fmt.Println("nemo options init failed")
		return
	}

	logger.Printf("nemo conf:\n")
	logger.Printf("CreateIfMissing:	%t\n", jsonConf.Db.CreateIfMissing)
	logger.Printf("WriteBufferSize:	%dMegaByte\n", jsonConf.Db.WriteBufferSize)
	logger.Printf("MaxOpenFiles:	%d\n", jsonConf.Db.MaxOpenFiles)
	logger.Printf("UseBloomfilter:	%t\n", jsonConf.Db.UseBloomfilter)
	logger.Printf("WriteThreads:	%d\n", jsonConf.Db.WriteThreads)
	logger.Printf("TargetFileSizeBase:	%dMegaByte\n", jsonConf.Db.TargetFileSizeBase)
	logger.Printf("TargetFileSizeMultiplier: %d\n", jsonConf.Db.TargetFileSizeMultiplier)
	logger.Printf("Compression:	%t\n", jsonConf.Db.Compression)
	logger.Printf("MaxBackgroundFlushes:	%d\n", jsonConf.Db.MaxBackgroundFlushes)
	logger.Printf("MaxBackgroundCompactions:	%d\n", jsonConf.Db.MaxBackgroundCompactions)
	logger.Printf("MaxBytesForLevelMultiplier:	%d\n", jsonConf.Db.MaxBytesForLevelMultiplier)

	t1 := time.Now().UnixNano()
	n := gonemo.OpenNemo(opts, cfg.NemoPath)

	done := make(chan int)
	threads := cfg.Hash.ThreadNum
	for t := 0; t < threads; t++ {
		go datagen.HashInsert(t, done, n, cfg)
	}

	for t := 0; t < threads; t++ {
		<-done
	}

	t2 := time.Now().UnixNano()
	fmt.Println("Hash Insert done!")
	fmt.Printf("spend [%d] milli-seconds to insert data\n", (t2-t1)/1000000)
	logger.Printf("spend [%d] milli-seconds to insert data\n", (t2-t1)/1000000)
}
