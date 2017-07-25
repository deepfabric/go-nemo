package main

import (
	"flag"
	"fmt"

	gonemo "github.com/deepfabric/go-nemo"
	datagen "github.com/deepfabric/go-nemo/datagen"
)

func main() {
	flag.Parse()
	cfg, err := datagen.LoadConfig(*datagen.CfgFile)
	if err == nil {
		fmt.Println(*cfg)
	} else {
		return
	}

	opts := gonemo.NewOptions()
	n := gonemo.OpenNemo(opts, cfg.NemoPath)

	done := make(chan int)
	threads := cfg.Hash.ThreadNum
	for t := 0; t < threads; t++ {
		go datagen.HashInsert(t, done, n, cfg)
	}

	for t := 0; t < threads; t++ {
		<-done
	}
	fmt.Println("Hash Insert done!")
}
