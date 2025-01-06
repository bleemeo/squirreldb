// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"log"
	"os"

	"github.com/pilosa/pilosa/v2/roaring"
)

// small tools to show number present in a postings.
// To procude a .hex file:
// Run in cqlsh:
// * select bitset from squirreldb.index_postings where shard = -1 and name = '__global__all|metrics__';
// * In shell, run: cut -b 3- | fold -s -w 80 > filename.hex
// * Copy/paste output from cqlsh to shell with cut+fold

var (
	filename = flag.String("filename", "", ".hex file to read")
	force    = flag.Bool("force", false, "force operation")
)

func loadBitmap(filename string) (*roaring.Bitmap, error) {
	tmp := roaring.NewBTreeBitmap()

	bufferHex, err := os.ReadFile(filename)
	if err != nil {
		return tmp, err
	}

	// remove all new-line
	bufferHex = bytes.ReplaceAll(bufferHex, []byte("\n"), nil)

	buffer := make([]byte, hex.DecodedLen(len(bufferHex)))

	_, err = hex.Decode(buffer, bufferHex)
	if err != nil {
		return tmp, err
	}

	err = tmp.UnmarshalBinary(buffer)
	if err != nil {
		return tmp, err
	}

	return tmp, nil
}

func main() {
	flag.Parse()

	tmp, err := loadBitmap(*filename)
	if err != nil {
		log.Fatal(err)
	}

	count := tmp.Count()

	log.Printf("The bitmap contains %d number", count)
	log.Printf("number of hole: %d = max (%d) - count (%d)", tmp.Max()-count, tmp.Max(), count)

	if tmp.Count() > 1e9 && !*force {
		log.Printf("bitmap is too big to be processed. Use -force to process anyway (may cause OOM)")

		return
	}

	slice := tmp.Slice()
	if uint64(len(slice)) != count {
		log.Printf("slice had %d value, want %d", len(slice), count)
	}

	startRange := uint64(0)
	endRange := uint64(0)
	numberHole := uint64(0)

	for i, v := range slice {
		switch {
		case i == 0:
			startRange = v
			endRange = v
			numberHole += v - 1
		case v == endRange+1:
			endRange = v
		case v <= endRange:
			log.Fatalf("number aren't sorted ! %d <= %d", v, endRange)
		default:
			log.Printf("range from %d to %d (free before this range: %d)", startRange, endRange, numberHole)
			numberHole += v - endRange - 1
			startRange = v
			endRange = v
		}
	}

	log.Printf("range from %d to %d (free before this range: %d)", startRange, endRange, numberHole)
	log.Printf("number of hole: %d = max (%d) - count (%d)", tmp.Max()-count, tmp.Max(), count)
}
