package main

import (
	"fmt"
	"os"

	"github.com/segmentio/parquet-go"
)

func main() {
	f, err := os.OpenFile("tmp.parquet", os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := parquet.NewReader(f)
	fmt.Printf("%v\n", r.Schema().Columns())
	var count int64
	for {

		var record = make(map[string]interface{})
		err := r.Read(&record)
		if err != nil {
			break
		}
		for k, v := range record {
			fmt.Printf("  %s : %v\n", k, v)
		}
		//counties = append(counties, v)
		genSpace(3)
		count++
	}
}
func genSpace(n int) {
	for i := 0; i < n; i++ {
		println("")
	}
}
