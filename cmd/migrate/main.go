// Command migrate copies a stopped Pebble v1 database to a new Pebble v2 directory.
package main

import (
	"flag"
	"fmt"
	legacy "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"
	"log"
	"os"
	"path/filepath"
)

func migrate(source, destination string) error {
	if source == "" || destination == "" {
		return fmt.Errorf("both -source and -destination are required")
	}
	src, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	dst, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	if src == dst {
		return fmt.Errorf("destination must differ from source")
	}
	if _, err = os.Stat(dst); !os.IsNotExist(err) {
		return fmt.Errorf("destination must not exist")
	}
	old, err := legacy.Open(src, &legacy.Options{ReadOnly: true, ErrorIfNotExists: true})
	if err != nil {
		return err
	}
	defer old.Close()
	db, err := pebble.Open(dst, &pebble.Options{ErrorIfExists: true})
	if err != nil {
		return err
	}
	defer db.Close()
	iter, err := old.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()
	batch := db.NewBatch()
	defer batch.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err = batch.Set(iter.Key(), iter.Value(), nil); err != nil {
			return err
		}
		if batch.Len() >= 4<<20 {
			if err = batch.Commit(pebble.Sync); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if err = iter.Error(); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}
func main() {
	source := flag.String("source", "", "stopped legacy database (read only)")
	destination := flag.String("destination", "", "new destination directory (must not exist)")
	flag.Parse()
	if err := migrate(*source, *destination); err != nil {
		log.Fatal(err)
	}
	log.Print("database copied; source unchanged")
}
