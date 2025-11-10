package projectoptimizer

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/apache/arrow/go/v15/parquet/schema"
)

// ArrowTest demonstrates reading parquet with Apache Arrow
// This is the recommended approach for the execution engine
func ArrowTest(f *os.File) {
	// Method 1: Using pqarrow (HIGH-LEVEL - RECOMMENDED)
	readWithPqArrow(f)

	// Method 2: Using low-level file API
	// readWithLowLevel(f)
}

// readWithPqArrow uses the high-level pqarrow API (easiest and most common)
func readWithPqArrow(f *os.File) {
	allocator := memory.NewGoAllocator()

	// First create low-level file reader
	fileReader, err := file.NewParquetReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer fileReader.Close()

	// Create Arrow-integrated reader from file reader
	arrowReader, err := pqarrow.NewFileReader(
		fileReader,
		pqarrow.ArrowReadProperties{Parallel: true, BatchSize: 1024 * 8},
		allocator,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Read entire file as Arrow Table
	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer table.Release()

	// Print metadata
	fmt.Printf("Schema: %s\n", table.Schema())
	fmt.Printf("NumRows: %d\n", table.NumRows())
	fmt.Printf("NumCols: %d\n", table.NumCols())

	// Iterate through columns
	for i := 0; i < int(table.NumCols()); i++ {
		col := table.Column(i)
		fmt.Printf("\nColumn %d: %s (type: %s)\n", i, col.Name(), col.DataType())

		// Get first chunk and print first 5 values
		if col.Data().Len() > 0 {
			chunk := col.Data().Chunk(0)
			printFirstValues(chunk, 5)
		}
	}
}

// printFirstValues prints first N values from an Arrow array
func printFirstValues(arr arrow.Array, n int) {
	limit := minInt(n, arr.Len())

	switch a := arr.(type) {
	case *array.String:
		for i := 0; i < limit; i++ {
			if a.IsNull(i) {
				fmt.Printf("  [%d]: NULL\n", i)
			} else {
				fmt.Printf("  [%d]: %s\n", i, a.Value(i))
			}
		}
	case *array.Float64:
		for i := 0; i < limit; i++ {
			if a.IsNull(i) {
				fmt.Printf("  [%d]: NULL\n", i)
			} else {
				fmt.Printf("  [%d]: %.2f\n", i, a.Value(i))
			}
		}
	case *array.Int64:
		for i := 0; i < limit; i++ {
			if a.IsNull(i) {
				fmt.Printf("  [%d]: NULL\n", i)
			} else {
				fmt.Printf("  [%d]: %d\n", i, a.Value(i))
			}
		}
	case *array.Int32:
		for i := 0; i < limit; i++ {
			if a.IsNull(i) {
				fmt.Printf("  [%d]: NULL\n", i)
			} else {
				fmt.Printf("  [%d]: %d\n", i, a.Value(i))
			}
		}
	default:
		fmt.Printf("  Type %T not handled in example\n", arr)
	}
}

// readWithLowLevel demonstrates low-level API (for advanced use cases)
func readWithLowLevel(f *os.File) {
	reader, err := file.NewParquetReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	// Get metadata
	meta := reader.MetaData()
	fmt.Printf("NumRows: %d\n", meta.NumRows)
	fmt.Printf("NumRowGroups: %d\n", reader.NumRowGroups())

	schema := meta.Schema
	fmt.Printf("NumColumns: %d\n", schema.NumColumns())

	// Read first row group, first column
	if reader.NumRowGroups() > 0 {
		rowGroup := reader.RowGroup(0)

		if schema.NumColumns() > 0 {
			col, err := rowGroup.Column(0)
			if err != nil {
				log.Fatal(err)
			}

			readColumnValues(col, schema.Column(0))
		}
	}
}

// readColumnValues reads values from a column chunk based on physical type
func readColumnValues(col file.ColumnChunkReader, schemaCol *schema.Column) {
	batchSize := int64(10) // Read 10 values

	switch schemaCol.PhysicalType() {
	case parquet.Types.Int64:
		int64Col := col.(*file.Int64ColumnChunkReader)
		values := make([]int64, batchSize)
		defLevels := make([]int16, batchSize)
		repLevels := make([]int16, batchSize)

		_, valuesRead, err := int64Col.ReadBatch(batchSize, values, defLevels, repLevels)
		if err != nil {
			log.Println(err)
			return
		}

		fmt.Printf("Column %s (Int64):\n", schemaCol.Name())
		for i := 0; i < valuesRead; i++ {
			fmt.Printf("  %d\n", values[i])
		}

	case parquet.Types.Double:
		float64Col := col.(*file.Float64ColumnChunkReader)
		values := make([]float64, batchSize)
		defLevels := make([]int16, batchSize)
		repLevels := make([]int16, batchSize)

		_, valuesRead, err := float64Col.ReadBatch(batchSize, values, defLevels, repLevels)
		if err != nil {
			log.Println(err)
			return
		}

		fmt.Printf("Column %s (Float64):\n", schemaCol.Name())
		for i := 0; i < valuesRead; i++ {
			fmt.Printf("  %.2f\n", values[i])
		}

	case parquet.Types.ByteArray:
		byteArrayCol := col.(*file.ByteArrayColumnChunkReader)
		values := make([]parquet.ByteArray, batchSize)
		defLevels := make([]int16, batchSize)
		repLevels := make([]int16, batchSize)

		_, valuesRead, err := byteArrayCol.ReadBatch(batchSize, values, defLevels, repLevels)
		if err != nil {
			log.Println(err)
			return
		}

		fmt.Printf("Column %s (String):\n", schemaCol.Name())
		for i := 0; i < valuesRead; i++ {
			fmt.Printf("  %s\n", string(values[i]))
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func ReadRecordBatch(f *os.File, columns []string) {
	allocator := memory.NewGoAllocator()

	// First create low-level file reader
	fileReader, err := file.NewParquetReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer fileReader.Close()

	// Create Arrow-integrated reader from file reader
	arrowReader, err := pqarrow.NewFileReader(
		fileReader,
		pqarrow.ArrowReadProperties{Parallel: true, BatchSize: 5},
		allocator,
	)
	if err != nil {
		log.Fatal(err)
	}
	var colidx []int
	s, _ := arrowReader.Schema()
	for _, colName := range columns {
		idx := s.FieldIndices(colName)
		if len(idx) == 0 {
			log.Fatalf("column %s not found in schema", colName)
		}
		colidx = append(colidx, idx[0])
	}
	fmt.Printf("cols idx : %v\n", colidx)
	rdr, err := arrowReader.GetRecordReader(context.TODO(), colidx, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rdr schema : %v", rdr.Schema())
	return
	defer rdr.Release()
	idx := 0
	for rdr.Next() && idx < 3 {
		rec := rdr.Record()
		cols := rec.NumCols()
		for i := 0; i < int(cols); i++ {
			col := rec.Column(i)
			fmt.Printf("col (%d) : %v\n", i, col)
		}

		idx++
	}

}
