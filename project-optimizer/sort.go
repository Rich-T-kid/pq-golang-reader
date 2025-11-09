package projectoptimizer

import "reflect"

// deal with sorting large data sets that wont fit in memory

type SortExec struct {
	columnName string
	columnType reflect.Type
	columnIdx  int
}

func NewSortExec(columnName string, columnType reflect.Type, columnIdx int) *SortExec {
	return &SortExec{
		columnName: columnName,
		columnType: columnType,
		columnIdx:  columnIdx,
	}
}
