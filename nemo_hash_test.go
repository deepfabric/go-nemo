package gonemo

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var alphabet = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

func genString(length int) []byte {
	str := make([]byte, length)
	for i := 0; i < len(str); i++ {
		str[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return str
}

func BenchmarkHashSet(b *testing.B) {

	var length = 10
	var num = 1000
	opts := NewDefaultOptions()
	n := OpenNemo(opts, "/tmp/go-benchmark/go-nemo/")
	rand.Seed(time.Now().Unix())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		HKey := append([]byte("hash_key:"), []byte(strconv.Itoa(rand.Intn(num)))...)
		field := genString(length)
		value := genString(length)
		b.StartTimer()
		_, err := n.HSet(HKey, field, value)
		if err != nil {
			b.Errorf("HSet error:%s\n", err.Error())
		}
	}
	b.StopTimer()
	n.Close()
}
