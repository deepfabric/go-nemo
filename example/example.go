package main

// #cgo LDFLAGS: -lstdc++ -lsnappy
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all -lrt
// #include <stdlib.h>
import "C"
import (
	"fmt"

	gonemo "github.com/deepfabric/go-nemo"
)

func main() {
	opts := gonemo.NewDefaultOptions()
	n := gonemo.OpenNemo(opts, "/tmp/rocksdb")
	key := []byte("Hello")
	field := []byte("Hello")
	value := []byte("World")

	keys := [][]byte{{'n', '1'}, {'n', '2'}}
	vals := [][]byte{{'T', 'o', 'm'}, {'C', 'a', 't'}}
	fields := [][]byte{{'n', '1'}, {'n', '2'}}

	//Set
	err := n.Set(key, value, 1000)
	if err == nil {
		fmt.Print("success to set!")
		fmt.Println("key:" + string(key))
		fmt.Println("value:" + string(value))
	} else {
		fmt.Println(err)
	}

	//Get0
	res_value0, err := n.Get0(key)
	defer res_value0.Free()
	if err == nil {
		fmt.Print("success to Get0 value:")
		res := res_value0.Data()
		fmt.Println("value:" + string(res))
		if Equal(res, value) {
			fmt.Println("Get0 value correct!")
		}
	} else {
		fmt.Println(err)
	}
	//Get
	res_value, err := n.Get(key)
	if err == nil {
		fmt.Print("success to Get value:")
		fmt.Println("value:" + string(res_value))
		if Equal(res_value, value) {
			fmt.Println("Get value correct!")
		}
	} else {
		fmt.Println(err)
	}

	//Mset
	err = n.MSet(keys, vals)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to Mset")
	}
	//MGet
	res_vals, errs := n.MGet(keys)
	fmt.Println("MGet result:")
	for i, err := range errs {
		fmt.Println(err)
		fmt.Println("key" + string(keys[i]))
		fmt.Println("value" + string(res_vals[i]))
		if Equal(vals[i], res_vals[i]) {
			fmt.Printf("get value[%d] correct\n", i)
		} else {
			fmt.Printf("get value[%d] wrong\n", i)
		}
	}
	//Keys
	res_keys, err := n.Keys([]byte("*"))
	if err == nil {
		for i, _ := range res_keys {
			fmt.Println("keys res:" + string(res_keys[i]))
		}
	} else {
		fmt.Println(err)
	}

	Hkey := []byte("H1")
	//HSet

	err = n.HSet(Hkey, field, value)
	if err == nil {
		fmt.Print("success to HSet!")
		fmt.Println("key:" + string("H1"))
		fmt.Println("field:" + string(field))
		fmt.Println("value:" + string(value))
	} else {
		fmt.Println(err)
	}
	//HGet
	res_value, err = n.HGet(Hkey, field)
	if err == nil {
		fmt.Print("success to HGet value:")
		fmt.Println("key:" + string("H1"))
		fmt.Println("field:" + string(field))
		fmt.Println("value:" + string(res_value))
		if Equal(res_value, value) {
			fmt.Println("HGet value correct!")
		}
	} else {
		fmt.Println(err)
	}

	//HMset
	err = n.HMSet(Hkey, fields, vals)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to HMset")
	}
	//HMGet
	res_vals, errs = n.HMGet(Hkey, fields)
	fmt.Println("HMGet result:")
	for i, err := range errs {
		fmt.Println(err)
		fmt.Println("field" + string(fields[i]))
		fmt.Println("value" + string(res_vals[i]))
		if Equal(vals[i], res_vals[i]) {
			fmt.Printf("get value[%d] correct\n", i)
		} else {
			fmt.Printf("get value[%d] wrong\n", i)
		}
	}

	//List Push
	len, err := n.LPush([]byte("List1"), []byte("world"))
	if err == nil {
		fmt.Print("success to LPush!,list len:")
		fmt.Println(len)
	} else {
		fmt.Println(err)
	}
	len, err = n.LPush([]byte("List1"), []byte("hello"))
	if err == nil {
		fmt.Print("success to LPush!,list len:")
		fmt.Println(len)
	} else {
		fmt.Println(err)
	}
	//List Pop
	res_value, err = n.LPop([]byte("List1"))
	if err == nil {
		fmt.Println("success to LPop!")
		if !Equal(res_value, []byte("hello")) {
			fmt.Println("LPop wrong value")
			fmt.Println(res_value)
		}
	} else {
		fmt.Println(err)
	}

	h1 := n.GetMetaHandle()
	err = n.PutWithHandle(h1, key, value)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to PutWithHandle")
	}
	value, err = n.GetWithHandle(h1, key)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to GetWithHandle")
		fmt.Println("key:" + string(key))
		fmt.Println("value:" + string(value))
	}

	wb := gonemo.NewWriteBatch()
	wb.WriteBatchPut([]byte("BK1"), []byte("V1"))
	wb.WriteBatchPut([]byte("BK2"), []byte("V2"))
	wb.WriteBatchDel([]byte("BK2"), []byte("V2"))
	err = n.BatchWrite(h1, wb)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to BatchWrite")
	}

	it := n.NewVolumeIterator([]byte("A"), []byte("x"), 100)
	for ; it.Valid(); it.Next() {
		fmt.Println("volume iterator key:" + string(it.Key()))
		fmt.Println(it.Value())
	}
	it.Free()

	kit := n.KScanWithHandle(h1, []byte("A"), []byte("x"), 100)
	for ; kit.Valid(); it.Next() {
		fmt.Println("kviterator key:" + string(kit.Key()))
		fmt.Println("kviterator value:" + string(kit.Value()))
	}
	kit.Free()

	n.RangeDel([]byte("A"), []byte("x"), 100)
	n.RangeDelWithHandle(h1, []byte("A"), []byte("x"), 100)

	/*
		snapshots, err := n.BGSaveGetSnapshot()
		if err == nil {
			fmt.Println("Get Snapshots success!")
		} else {
			fmt.Println(err)
			return
		}

		err = n.BGSave(snapshots, "/tmp/backup2/")
		if err == nil {
			fmt.Println("BGSave backup2 success!")
		} else {
			fmt.Println(err)
			return
		}

		kv_snapshot, err := n.BGSaveGetSpecifySnapshot(gonemo.HASH_DB)
		if err == nil {
			fmt.Println("Get Snapshot hashdb success!")
		} else {
			fmt.Println(err)
			return
		}

		err = n.BGSaveSpecify(kv_snapshot)
		if err == nil {
			fmt.Println("BGSaveSpecify hashdb success!")
		} else {
			fmt.Println(err)
			return
		}
	*/

	n.Close()

}

func Equal(slice1 []byte, slice2 []byte) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	for i := range slice1 {
		if slice1[i] != slice2[i] {
			return false
		}
	}
	return true
}
