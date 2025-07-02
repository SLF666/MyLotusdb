package main

import (
	"github.com/lotusdblabs/lotusdb/v2"
)

func main() {
	options := lotusdb.DefaultOptions
	options.DirPath = "E:\\tmp"

	// open a database
	db, err := lotusdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// put a key
	err = db.Put([]byte("name"), []byte("lotusdb"))
	if err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println("val = ", string(val))

	// delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
