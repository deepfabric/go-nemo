package datagen

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	gonemo "github.com/deepfabric/go-nemo"
)

// HashInsert insert entries into hash table according to configuration
func HashInsert(thread int, done chan int, n *gonemo.NEMO, cfg *Config) {
	r := rand.New(rand.NewSource(time.Now().Unix() + int64(thread)))
	for i := 0; i < cfg.Hash.TableCount; i++ {
		HKey := genBytes(cfg.Hash.KeyLen, r)
		prefix := []byte(fmt.Sprintf("%d%d", thread, i))
		HKey = append(prefix, HKey...)
		for j := 0; j < cfg.Hash.EntryCount; j++ {
			field := genBytes(cfg.Hash.FieldLen, r)
			field = append([]byte(strconv.Itoa(j)), field...)
			value := genBytes(cfg.Hash.ValueLen, r)
			_, err := n.HSet(HKey, field, value)
			if err != nil {
				fmt.Printf("HSet Err! %s", err.Error())
			}
		}
		fmt.Printf("Thread %d insert hash table %s done\n", thread, HKey)
	}
	fmt.Printf("HashInsert Thread %d done\n", thread)
	done <- 1
}
