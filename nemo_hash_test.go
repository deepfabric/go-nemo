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

	var lenght int = 10
	var kNum int = 1000
	opts := NewOptions()
	n := OpenNemo(opts, "/tmp/go-benchmark/go-nemo/")
	rand.Seed(time.Now().Unix())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		HKey := append([]byte("hash_key:"), []byte(strconv.Itoa(rand.Intn(kNum)))...)
		field := genString(lenght)
		value := genString(lenght)
		b.StartTimer()
		_, err := n.HSet(HKey, field, value)
		if err != nil {
			b.Errorf("HSet error:%s\n", err.Error())
		}
	}
	b.StopTimer()
	n.Close()
}
