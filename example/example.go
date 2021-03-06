package main

// #cgo LDFLAGS: -lstdc++ -lsnappy -ljemalloc
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all -lrt -lpthread
// #include <stdlib.h>
import "C"
import (
	"bytes"
	"fmt"
	"time"

	gonemo "github.com/deepfabric/go-nemo"
)

func main() {
	//opts := gonemo.NewDefaultOptions()
	opts, _ := gonemo.NewOptions("option.json")
	n := gonemo.OpenNemo(opts, "/tmp/go-nemo/")
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
	// time.Sleep(3600 * time.Second)
	//Get
	ResValue, err := n.Get(key)
	if err == nil {
		fmt.Print("success to Get value:")
		fmt.Println("value:" + string(ResValue))
		if equal(ResValue, value) {
			fmt.Println("Get value correct!")
		}
	} else {
		fmt.Println(err)
	}
	//GetUnSafe
	ResValue, cppStr, err := n.GetUnSafe(key)
	if err == nil {
		fmt.Print("success to Get value:")
		fmt.Println("value:" + string(ResValue))
		if equal(ResValue, value) {
			fmt.Println("Get value correct!")
		}
	} else {
		fmt.Println(err)
	}
	gonemo.FreeCppStr(cppStr)
	//Mset
	err = n.MSet(keys, vals)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to Mset")
	}
	//MGet
	ResVals, errs := n.MGet(keys)
	fmt.Println("MGet result:")
	for i, err := range errs {
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("key " + string(keys[i]))
			fmt.Println("value " + string(ResVals[i]))
			if equal(vals[i], ResVals[i]) {
				fmt.Printf("get value[%d] correct\n", i)
			} else {
				fmt.Printf("get value[%d] wrong\n", i)
			}
		}
	}
	//MGetUnSafe
	ResVals, vp, errs := n.MGetUnSafe(keys)
	fmt.Println("MGetUnSafe result:")
	for i, err := range errs {
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("key " + string(keys[i]))
			fmt.Println("value " + string(ResVals[i]))
			if equal(vals[i], ResVals[i]) {
				fmt.Printf("get value[%d] correct\n", i)
			} else {
				fmt.Printf("get value[%d] wrong\n", i)
			}
		}
	}
	gonemo.FreeCppSSVector(vp)
	//Keys
	ResKeys, err := n.Keys([]byte("*"))
	if err == nil {
		for i := range ResKeys {
			fmt.Println("keys res:" + string(ResKeys[i]))
		}
	} else {
		fmt.Println(err)
	}
	//HSet
	Hkey := []byte("H1")
	HSetRes, err := n.HSet(Hkey, field, value)
	if err == nil {
		fmt.Print("success to HSet!")
		fmt.Println("key:" + string("H1"))
		fmt.Println("field:" + string(field))
		fmt.Println("value:" + string(value))
		fmt.Print("HSetRes:")
		fmt.Println(HSetRes)
	} else {
		fmt.Println(err)
	}

	Hkey = []byte("H2")
	HSetRes, err = n.HSet(Hkey, field, value)
	if err == nil {
		fmt.Print("success to HSet!")
		fmt.Println("key:" + string("H2"))
		fmt.Println("field:" + string(field))
		fmt.Println("value:" + string(value))
		fmt.Print("HSetRes:")
		fmt.Println(HSetRes)
	} else {
		fmt.Println(err)
	}

	//HGet
	ResValue, err = n.HGet(Hkey, field)
	if err == nil {
		fmt.Print("success to HGet value:")
		fmt.Println("key:" + string("H1"))
		fmt.Println("field:" + string(field))
		fmt.Println("value:" + string(ResValue))
		if equal(ResValue, value) {
			fmt.Println("HGet value correct!")
		}
	} else {
		fmt.Println(err)
	}

	resIndexInfo, indexRes, err := n.HGetIndexInfo([]byte("HIndexInfoNotExists"))
	if err != nil {
		fmt.Println(err)
	} else {
		if indexRes == -1 {
			fmt.Println("success to get a non-exists index key")
		} else {
			fmt.Println("fail to get a non-exists key")
		}
	}
	indexInfo := []byte("indexInfo")
	err = n.HSetIndexInfo(Hkey, indexInfo)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to HSetIndexInfo")
	}
	resIndexInfo, indexRes, err = n.HGetIndexInfo(Hkey)
	if err != nil {
		fmt.Println(err)
	} else {
		if indexRes == 0 {
			if equal(indexInfo, resIndexInfo) {
				fmt.Println("success to HGetIndexInfo")
			} else {
				fmt.Printf("get index info[%s] wrong\n", string(resIndexInfo))
			}
		} else {
			fmt.Println("fail to get a Index key")
			fmt.Printf("get index info[%s] wrong\n", string(resIndexInfo))
		}
	}
	//HMset
	_, err = n.HMSet(Hkey, fields, vals)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to HMset")
	}
	//HMGet
	ResVals, errs = n.HMGet(Hkey, fields)
	fmt.Println("HMGet result:")
	for i, err := range errs {
		fmt.Println(err)
		fmt.Println("field:" + string(fields[i]))
		fmt.Println("value:" + string(ResVals[i]))
		if equal(vals[i], ResVals[i]) {
			fmt.Printf("get value[%d] correct\n", i)
		} else {
			fmt.Printf("get value[%d] wrong\n", i)
		}
	}
	//HMGetUnSafe
	ResVals, vp, errs = n.HMGetUnSafe(Hkey, fields)
	fmt.Println("HMGet result:")
	for i, err := range errs {
		fmt.Println(err)
		fmt.Println("field:" + string(fields[i]))
		fmt.Println("value:" + string(ResVals[i]))
		if equal(vals[i], ResVals[i]) {
			fmt.Printf("get value[%d] correct\n", i)
		} else {
			fmt.Printf("get value[%d] wrong\n", i)
		}
	}
	gonemo.FreeCppSSVector(vp)

	fmt.Println("hash scan")
	field = []byte("f1")
	value = []byte("1")
	HSetRes, err = n.HSet(Hkey, field, value)
	if err != nil {
		fmt.Println(err)
	}
	field = []byte("f2")
	value = []byte("2")
	HSetRes, err = n.HSet(Hkey, field, value)
	if err != nil {
		fmt.Println(err)
	}
	field = []byte("f3")
	value = []byte("3")
	HSetRes, err = n.HSet(Hkey, field, value)
	if err != nil {
		fmt.Println(err)
	}
	hit := n.HScan(Hkey, []byte("f1"), []byte("f4"), true)
	fmt.Println("hash iterator key: " + string(hit.Key()))
	for ; hit.Valid(); hit.Next() {
		if !bytes.Equal([]byte("f1"), hit.Field()) {
			fmt.Println("hash iterator field: " + string(hit.Field()))
			fmt.Println("hash iterator value: " + string(hit.Value()))
		}
	}
	hit.Free()

	fmt.Println("hmeta scan skip nil index")
	hmit := n.HmeataScan([]byte("A"), []byte("x"), true, true)
	for ; hmit.Valid(); hmit.Next() {
		fmt.Println("hmeta iterator key: " + string(hmit.Key()))
		k := hmit.PooledKey()
		fmt.Println("hmeta iterator pooled key: " + string(k))
		gonemo.MemPool.Free(k)
		fmt.Println("hmeta iterator indexInfo: " + string(hmit.IndexInfo()))
	}
	hmit.Free()
	fmt.Println("hmeta scan, do not skip nil index")
	hmit = n.HmeataScan([]byte("A"), []byte("x"), true, false)
	for ; hmit.Valid(); hmit.Next() {
		fmt.Println("hmeta iterator key: " + string(hmit.Key()))
		k := hmit.PooledKey()
		fmt.Println("hmeta iterator pooled key: " + string(k))
		gonemo.MemPool.Free(k)
	}
	hmit.Free()

	//List Push
	length, err := n.LPush([]byte("List1"), []byte("world"))
	if err == nil {
		fmt.Print("success to LPush!,list length:")
		fmt.Println(length)
	} else {
		fmt.Println(err)
	}
	length, err = n.LPush([]byte("List1"), []byte("hello"))
	if err == nil {
		fmt.Print("success to LPush!,list length:")
		fmt.Println(length)
	} else {
		fmt.Println(err)
	}
	//List Pop
	ResValue, err = n.LPop([]byte("List1"))
	if err == nil {
		fmt.Println("success to LPop!")
		if !equal(ResValue, []byte("hello")) {
			fmt.Println("LPop wrong value")
			fmt.Println(ResValue)
		}
	} else {
		fmt.Println(err)
	}
	//handle
	h1 := n.GetMetaHandle()
	err = n.PutWithHandle(h1, key, value, false)
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
	value, cppStr, err = n.GetWithHandleUnSafe(h1, key)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to GetWithHandle")
		fmt.Println("key:" + string(key))
		fmt.Println("value:" + string(value))
	}
	gonemo.FreeCppStr(cppStr)

	err = n.DeleteWithHandle(h1, key, false)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to DeleteWithHandle")
	}
	wb := gonemo.NewWriteBatch()
	wb.WriteBatchPut([]byte("BK1"), []byte("V1"))
	wb.WriteBatchPut([]byte("BK2"), []byte("V2"))
	wb.WriteBatchDel([]byte("BK2"))
	wb.WriteBatchPut([]byte("BK3"), []byte("V3"))
	wb.WriteBatchPut([]byte("BK4"), []byte("V4"))
	err = n.BatchWrite(h1, wb, false)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to BatchWrite")
	}

	h2 := n.GetKvHandle()
	wb = gonemo.NewWriteBatch()
	wb.WriteBatchDel(key)
	wb.WriteBatchPut([]byte("KVBatch"), []byte("test"))
	err = n.BatchWrite(h2, wb, true)
	if err != nil {
		fmt.Println(err)
	} else {
		ResValue, err = n.Get([]byte("KVBatch"))
		if err != nil {
			fmt.Println(err)
		}
		if equal(ResValue, []byte("test")) {
			fmt.Println("Success to KV BatchWrite")
		} else {
			fmt.Println("Faile to KV BatchWrite")
			fmt.Printf("get key Hello with wrong value: [%s]\n", string(ResValue))
		}
	}

	ttlKeyStrs := []string{"n1", "n2", "n3", "n4", "n4"}
	ttlValStrs := []string{"v1", "v2", "v3", "v4", "v4"}
	l := len(ttlKeyStrs)
	ttlKeys := make([][]byte, l)
	ttlVals := make([][]byte, l)
	for i := 0; i < l; i++ {
		ttlKeys[i] = []byte(ttlKeyStrs[i])
		ttlVals[i] = []byte(ttlValStrs[i])
	}
	ops := []int32{0, 0, 0, 0, 1}
	ttls := []int32{0, 1, 10, 0, 0}
	err = n.BatchWriteTTL(ttlKeys, ttlVals, ops, ttls, true)
	fmt.Print("BatchWriteTTL ")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("batch write ok")
	}
	time.Sleep(time.Duration(2) * time.Second)

	for i := 0; i < l; i++ {
		fmt.Println("BatchWriteTTL read case ", string(ttlKeys[i]), string(ttlVals[i]), ops[i], ttls[i])
		ResValue, err = n.Get(ttlKeys[i])
		if err != nil {
			fmt.Println("BatchWriteTTL Get err ", err)
		} else {
			fmt.Println("BatchWriteTTL res: ", string(ResValue))
		}
		kvh := n.GetKvHandle()
		ResValue, err = n.GetWithHandle(kvh, ttlKeys[i])
		if err != nil {
			fmt.Println("BatchWriteTTL GetWithHandle err ", err)
		} else {
			fmt.Println("BatchWriteTTL res: ", string(ResValue))
		}
	}

	ops2 := []int32{0, -1}
	ttls2 := []int32{0, 0}
	err = n.BatchWriteTTL(keys, vals, ops2, ttls2, true)
	fmt.Print("BatchWriteTTL ")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("batch write ok")
	}
	ttls2 = []int32{-1, 0}
	err = n.BatchWriteTTL(keys, vals, ops2, ttls2, true)
	fmt.Print("BatchWriteTTL ")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("batch write ok")
	}

	kit := n.KScanWithHandle(h1, []byte("A"), []byte("x"), true)
	for ; kit.Valid(); kit.Next() {
		fmt.Println("raw iterator ephemeral key:" + string(kit.Key()))
		fmt.Println("raw iterator ephemeral val:" + string(kit.Value()))
	}
	kit.Free()

	kit = n.KScanWithHandle(h1, []byte("A"), []byte("x"), true)
	for ; kit.Valid(); kit.Next() {
		k := kit.PooledKey()
		fmt.Println("raw iterator pooled key:" + string(k))
		gonemo.MemPool.Free(k)
		v := kit.PooledValue()
		fmt.Println("raw iterator pooled val:" + string(v))
		gonemo.MemPool.Free(v)
	}
	kit.Free()

	nKey, nVal, err := n.SeekWithHandle(h1, []byte("A"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to SeekWithHandle start: A")
		fmt.Println("next key:" + string(nKey))
		fmt.Println("next value:" + string(nVal))
	}
	nKey, nVal, err = n.SeekWithHandle(h1, []byte("x"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to SeekWithHandle start: x")
		fmt.Println("next key:" + string(nKey))
		fmt.Println("next value:" + string(nVal))
	}
	vit := n.NewVolumeIterator([]byte("A"), []byte("x"))
	for ; vit.Valid(); vit.Next() {
		fmt.Println("volume iterator key:" + string(vit.Key()))
		fmt.Println(vit.Value())
	}
	vit.Free()
	vit = n.NewVolumeIterator([]byte("A"), []byte("x"))
	if vit.TargetScan(1) {
		fmt.Println("find the targe key: ")
		fmt.Println(string(vit.TargetKey()))
	} else {
		fmt.Println("can't find the targe key, current total volume: ")
		fmt.Println(vit.TotalVolume())
	}
	vit.Free()
	vit = n.NewVolumeIterator([]byte("A"), []byte("x"))
	if vit.TargetScan(100) {
		fmt.Println("find the targe key: ")
		fmt.Println(string(vit.TargetKey()))
	} else {
		fmt.Println("can't find the targe key, current total volume: ")
		fmt.Println(vit.TotalVolume())
	}
	vit.Free()
	/*
		kit := n.KScan([]byte("A"), []byte("x"), 100)
		for ; kit.Valid(); kit.Next() {
			fmt.Println("kviterator key:" + string(kit.Key()))
			fmt.Println("kviterator value:" + string(kit.Value()))
		}
		kit.Free()
	*/
	err = n.RawScanSaveRange("/tmp/go-nemo-bak/", []byte("A"), []byte("zz"), true)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to RawScanSaveRange")
	}
	err = n.RangeDel([]byte("A"), []byte("zz"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to RangeDel")
	}
	err = n.RangeDelWithHandle(h1, []byte("A"), []byte("zz"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to RangeDelWithHandle")
	}
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
	// time.Sleep(3600 * time.Second)
	n.Close()

	n = gonemo.OpenNemo(opts, "/tmp/go-nemo/")

	err = n.IngestFile("/tmp/go-nemo-bak/")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success to IngestFile")
	}

	vit = n.NewVolumeIterator([]byte("A"), []byte("x"))
	for ; vit.Valid(); vit.Next() {
		fmt.Println("volume iterator key:" + string(vit.Key()))
		fmt.Println(vit.Value())
	}
	vit.Free()

	h1 = n.GetMetaHandle()
	kit = n.KScanWithHandle(h1, []byte("BK1"), []byte("BK4"), true)
	for ; kit.Valid(); kit.Next() {
		fmt.Println("meta iterator key:" + string(kit.Key()))
		fmt.Println("meta iterator val:" + string(kit.Value()))
	}
	kit.Free()

	/*
		kit = n.KScan([]byte("A"), []byte("x"), 100)
		for ; kit.Valid(); kit.Next() {
			fmt.Println("kviterator key:" + string(kit.Key()))
			fmt.Println("kviterator value:" + string(kit.Value()))
		}
		kit.Free()
	*/
}

func equal(slice1 []byte, slice2 []byte) bool {
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
