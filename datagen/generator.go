package datagen

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
)

var alphabet = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

func genBytes(length int, r *rand.Rand) []byte {
	bytes := make([]byte, length)
	for i := 0; i < len(bytes); i++ {
		bytes[i] = alphabet[r.Intn(len(alphabet))]
	}
	return bytes
}

// Config json structure for datagen configuration
type Config struct {
	Hash     hash   `json:"hash"`
	NemoPath string `json:"nemo-path"`
}

type hash struct {
	KeyLen     int `json:"key_length"`
	FieldLen   int `json:"field_length"`
	ValueLen   int `json:"value_length"`
	TableCount int `json:"table_count"`
	EntryCount int `json:"entry_count"`
	ThreadNum  int `json:"thread"`
}

// CfgFile configuration file name parse from input argument
var CfgFile = flag.String("cfg", "../datagen/gen.json", "config json file for data generator")

// LoadConfig parse json file 'cfgFileName', return a config object pointer
func LoadConfig(cfgFileName string) (*Config, error) {
	bytes, err := ioutil.ReadFile(cfgFileName)
	if err != nil {
		fmt.Printf("Read config File[%s] err: %s\n", cfgFileName, err.Error())
		return nil, err
	}
	var cfg Config
	err = json.Unmarshal(bytes, &cfg)
	if err != nil {
		fmt.Printf("Parse jsonfile[%s] err: %s\n", cfgFileName, err.Error())
		return nil, err
	}
	return &cfg, nil
}
