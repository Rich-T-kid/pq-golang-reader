package projectoptimizer

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
)

type AggregationExec interface {
	Schema() *parquetSchema
	Aggr() (any, error)
}

// implement sum,avg,count only

type SumExec struct {
	childInput Operator
	schema     *parquetSchema
	columnName string
	result     float64
}

func NewSumExec(input Operator, columnName string) (*SumExec, error) {
	field, err := input.Schema().ColumnInfo(columnName)
	if err != nil {
		return nil, fmt.Errorf("column %q not found: %w", columnName, err)
	}

	if !isNumericType(field.PqType) {
		return nil, fmt.Errorf("column %q has unsupported type %v for sum operation (must be numeric)", columnName, field.PqType)
	}

	// Build output schema with single result column
	schema := &parquetSchema{
		Fields: []structField{
			{Name: fmt.Sprintf("sum_%s", columnName), PqType: parquet.DoubleType},
		},
	}

	return &SumExec{
		childInput: input,
		schema:     schema,
		columnName: columnName,
	}, nil
}

func (s *SumExec) Schema() *parquetSchema {
	return s.schema
}

// need to change the interfaces, shouldnt return a record batch should return a single value
func (s *SumExec) Next(numRecords int) (RecordBatch, error) {
	return RecordBatch{}, nil
}

func isNumericType(t parquet.Type) bool {
	switch t {
	case parquet.Int32Type, parquet.Int64Type, parquet.FloatType, parquet.DoubleType:
		return true
	default:
		return false
	}
}

type AvgExec struct {
	childInput Operator
	columnName string
	columnType string
	columnIdx  int
	result     float64
}

func NewAvgExec(input Operator, columnName, columnType string, columnIdx int) *AvgExec {
	return &AvgExec{
		childInput: input,
		columnName: columnName,
		columnType: columnType,
		columnIdx:  columnIdx,
	}
}

type CountExec struct {
	childInput Operator
	columnName string
	columnType string
	columnIdx  int
	tot        uint64
}

func NewCountExec(input Operator, columnName, columnType string, columnIdx int) *CountExec {
	return &CountExec{
		childInput: input,
		columnName: columnName,
		columnType: columnType,
		columnIdx:  columnIdx,
	}
}
