package projectoptimizer

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

type FilterPredicate func(reflect.Value) bool
type structField struct {
	Name   string
	PqType parquet.Type
}
type parquetSchema struct {
	Fields []structField
}

type RecordBatch struct {
	Schema  parquetSchema
	Columns [][]any
}

type Display interface {
	Show() string
	ShowSchema() string
}
type Operator interface {
	Next(n uint) ([]RecordBatch, error) // read in n RecordBatches     |      return EOF when done
	Schema() *parquetSchema
}

// read from a file for source data
type Leaf struct {
	r    *parquet.Reader
	Type reflect.Type
}
type ProjectExec struct {
	childInput Operator // child operator
	schema     *parquetSchema
	filter     []FilterPredicate
	Type       reflect.Type
	columns    []string
	leaf       *Leaf
}

func NewProjectExec(schema *parquetSchema, columns []string, input Operator, filter []FilterPredicate) *ProjectExec {
	return &ProjectExec{
		columns:    columns,
		schema:     schema,
		filter:     filter,
		childInput: input,
		leaf:       nil,
	}
}
func NewProjectExecLeaf(source *os.File, columns []string, filter []FilterPredicate) *ProjectExec {
	schema, Typestruct, reader := initPrunedReader(source, columns...)
	return &ProjectExec{
		childInput: nil,
		columns:    columns,
		schema:     schema,
		filter:     filter,
		Type:       Typestruct,
		leaf:       &Leaf{r: reader},
	}
}

func (p *ProjectExec) Next(n uint64) (RecordBatch, error) {
	var batches = RecordBatch{
		Schema:  *p.schema,
		Columns: make([][]any, len(p.schema.Fields)),
	}
	var curSize, retries uint64
	for curSize < n {
		entry := reflect.New(p.Type).Interface()
		if err := p.leaf.r.Read(entry); err != nil {
			if err == io.EOF {
				return batches, err
			} else {
				retries++
				if retries > 3 {
					break
				}
				continue // for now continue but dont get stuck in infident loop
			}

		}
		v := reflect.ValueOf(entry).Elem()
		include := true
		for _, pred := range p.filter {
			if !pred(v) {
				include = false
				break
			}
		}
		if !include {
			continue
		}
		for i := 0; i < v.NumField(); i++ {
			_ = v.Type().Field(i)
			value := v.Field(i).Interface()
			batches.Columns[i] = append(batches.Columns[i], value)
		}
		curSize++
	}
	return batches, nil
}

func (p *ProjectExec) Schema() *parquetSchema {
	return p.schema
}
func (p *ProjectExec) isLeaf() bool {
	return p.leaf != nil
}

func initPrunedReader(f *os.File, columns ...string) (*parquetSchema, reflect.Type, *parquet.Reader) {
	// Parse original schema
	freshReader := parquet.NewReader(f)
	schema := freshReader.Schema()
	parsedSchema, _ := parseSchema(schema)
	freshReader.Close()

	// Prune to requested columns
	parsedSchema.pruneFields(columns...)

	// Generate struct for reading pruned columns
	prunedStruct, structType := genStructWithFields(parsedSchema.Fields...)

	// Create new reader with pruned schema
	reader := parquet.NewReader(f, parquet.SchemaOf(prunedStruct))

	return parsedSchema, structType, reader
}

// iterate through row groups
// TODO:  should return a Record interface instead of parquet.Row, for now this is fine
func IterRowGroupsWithPrune(f *os.File, columns ...string) *RecordBatch {

	v, structType, reader := initPrunedReader(f, columns...)

	size := reader.NumRows()
	fmt.Printf("Number of Rows: %d\n", size)
	rb := RecordBatch{
		Schema:  *v,
		Columns: make([][]any, len(v.Fields)),
	}
	i := 0
	rows := 0
	for i < 50 {
		entry := reflect.New(structType).Interface()
		if err := reader.Read(entry); err != nil {
			fmt.Printf("error reading rows: %v\n", err)
			break
		}
		v := reflect.ValueOf(entry).Elem()
		for i := 0; i < v.NumField(); i++ {
			_ = v.Type().Field(i)
			value := v.Field(i).Interface()
			rb.Columns[i] = append(rb.Columns[i], value)
		}
		rows++
		i++
		// Further processing can be done here
	}
	fmt.Printf("========================================\n")
	fmt.Printf("               read %d rows               \n", len(rb.Columns))
	fmt.Printf("========================================\n")
	return &rb
}
func IterRowGroupsWithPruneFilter(f *os.File, columns []string, pred FilterPredicate) *RecordBatch {
	v, structType, reader := initPrunedReader(f, columns...)

	size := reader.NumRows()
	fmt.Printf("Number of Rows: %d\n", size)
	rb := RecordBatch{
		Schema:  *v,
		Columns: make([][]any, len(v.Fields)),
	}
	i := 0
	rows := 0
	for i < 50 {
		entry := reflect.New(structType).Interface()
		if err := reader.Read(entry); err != nil {
			fmt.Printf("error reading rows: %v\n", err)
			break
		}
		v := reflect.ValueOf(entry).Elem()
		if pred(v) {
			for i := 0; i < v.NumField(); i++ {
				_ = v.Type().Field(i)
				value := v.Field(i).Interface()
				rb.Columns[i] = append(rb.Columns[i], value)
			}
		}
		rows++
		i++
		// Further processing can be done here
	}
	fmt.Printf("========================================\n")
	fmt.Printf("               read %d rows               \n", len(rb.Columns))
	fmt.Printf("========================================\n")

	return &rb
}

func parseSchema(schema *parquet.Schema) (*parquetSchema, error) {
	var fields []structField
	size := len(schema.Columns())
	size2 := len(schema.Fields())
	if size != size2 {
		return nil, fmt.Errorf("mismatched schema columns and fields size: %d vs %d", size, size2)
	}
	for i := 0; i < size; i++ {
		field := schema.Fields()[i]
		fields = append(fields, structField{
			Name:   field.Name(),
			PqType: field.Type(),
		})
	}
	return &parquetSchema{Fields: fields}, nil
}
func (p *parquetSchema) existIn(fieldName string) bool {
	for _, field := range p.Fields {
		if field.Name == fieldName {
			return true
		}
	}
	return false
}

// pass in fields u want to prune
// case insensitive
func (p *parquetSchema) pruneFields(fieldNames ...string) {
	var wantedFields []structField
	for _, name := range fieldNames {
		for _, field := range p.Fields {
			if strings.EqualFold(name, field.Name) {
				wantedFields = append(wantedFields, field)
			}
		}
	}
	p.Fields = wantedFields
}

// projection prune/push down

// filter by max/min values

// read values within a range

// do projection and predicate push down together

// do projection and predicate push down together & write output to a new parquet file

// provide a general way to write intermediary data to parquet files and return a pointer to theese files to upper operators

// provide a general way to read intermediary parquet files created by upper operators

/*
Create custom structs based on field names @ run time.
This is for generating structs dynamically.
used to prune parquet files for projection push down
*/
func genStructWithFields(fields ...structField) (any, reflect.Type) {
	var res []reflect.StructField
	for _, field := range fields {
		res = append(res, reflect.StructField{
			Name: capitalize(field.Name),
			Type: reflect.TypeOf(ZeroValueForParquetType(field.PqType)),
			Tag:  reflect.StructTag(`parquet:"` + field.Name + `"`),
		})
	}
	typ := reflect.StructOf(res)
	return reflect.New(typ).Interface(), typ
}

func capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// ZeroValueForParquetType returns the zero/default Go value for a Parquet type.
func ZeroValueForParquetType(pqType parquet.Type) any {
	switch pqType {
	case parquet.BooleanType:
		return false
	case parquet.Int32Type:
		return int32(0)
	case parquet.Int64Type:
		return int64(0)
	case parquet.FloatType:
		return float32(0)
	case parquet.DoubleType:
		return float64(0)
	case parquet.ByteArrayType:
		return "" // or []byte{} if you prefer raw bytes
	default:
		return ""
	}
}

func compareValues(a, b any) bool {
	// Type switch based on actual data type
	switch v := a.(type) {
	case int:
		return v < b.(int)
	case int32:
		return v < b.(int32)
	case int64:
		return v < b.(int64)
	case float64:
		return v < b.(float64)
	case string:
		return v < b.(string)
	default:
		panic("unsupported type for comparison")
	}
}

func (r RecordBatch) Show() string {
	if len(r.Columns) == 0 {
		return "Empty RecordBatch"
	}

	numRows := 0
	if len(r.Columns) > 0 {
		numRows = len(r.Columns[0])
	}

	if numRows == 0 {
		return "RecordBatch with 0 rows"
	}

	// Calculate column widths
	colWidths := make([]int, len(r.Schema.Fields))
	for i, field := range r.Schema.Fields {
		// Start with header width
		colWidths[i] = len(field.Name)

		// Check data widths (sample first 100 rows for performance)
		sampleSize := min(numRows, 100)
		for rowIdx := 0; rowIdx < sampleSize; rowIdx++ {
			val := formatValue(r.Columns[i][rowIdx])
			if len(val) > colWidths[i] {
				colWidths[i] = len(val)
			}
		}

		// Cap maximum width at 50 characters
		if colWidths[i] > 50 {
			colWidths[i] = 50
		}

		// Minimum width of 3
		if colWidths[i] < 3 {
			colWidths[i] = 3
		}
	}

	var sb strings.Builder

	// Write summary
	sb.WriteString(fmt.Sprintf("RecordBatch: %d rows × %d columns\n", numRows, len(r.Schema.Fields)))
	sb.WriteString("\n")

	// Write top border
	sb.WriteString("┌")
	for i, width := range colWidths {
		sb.WriteString(strings.Repeat("─", width+2))
		if i < len(colWidths)-1 {
			sb.WriteString("┬")
		}
	}
	sb.WriteString("┐\n")

	// Write header
	sb.WriteString("│")
	for i, field := range r.Schema.Fields {
		sb.WriteString(" ")
		sb.WriteString(padRight(field.Name, colWidths[i]))
		sb.WriteString(" │")
	}
	sb.WriteString("\n")

	// Write separator
	sb.WriteString("├")
	for i, width := range colWidths {
		sb.WriteString(strings.Repeat("─", width+2))
		if i < len(colWidths)-1 {
			sb.WriteString("┼")
		}
	}
	sb.WriteString("┤\n")

	// Write data rows (limit display to first 20 rows)
	displayRows := min(numRows, 20)
	for rowIdx := 0; rowIdx < displayRows; rowIdx++ {
		sb.WriteString("│")
		for colIdx := 0; colIdx < len(r.Columns); colIdx++ {
			sb.WriteString(" ")
			val := formatValue(r.Columns[colIdx][rowIdx])
			sb.WriteString(padRight(truncate(val, colWidths[colIdx]), colWidths[colIdx]))
			sb.WriteString(" │")
		}
		sb.WriteString("\n")
	}

	// If there are more rows, show ellipsis
	if numRows > displayRows {
		sb.WriteString("│")
		for _, width := range colWidths {
			sb.WriteString(" ")
			sb.WriteString(padRight("...", width))
			sb.WriteString(" │")
		}
		sb.WriteString("\n")
		sb.WriteString(fmt.Sprintf("│ ... %d more rows\n", numRows-displayRows))
	}

	// Write bottom border
	sb.WriteString("└")
	for i, width := range colWidths {
		sb.WriteString(strings.Repeat("─", width+2))
		if i < len(colWidths)-1 {
			sb.WriteString("┴")
		}
	}
	sb.WriteString("┘\n")

	return sb.String()
}

func (r RecordBatch) ShowSchema() string {
	return r.Schema.ShowSchema()
}

func formatValue(val any) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%.2f", v)
	case float64:
		return fmt.Sprintf("%.2f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func (s parquetSchema) ShowSchema() string {
	var sb strings.Builder
	sb.WriteString("Schema:\n")
	for i, field := range s.Fields {
		sb.WriteString(fmt.Sprintf("  %d. %s: %T\n", i, field.Name, field.PqType))
	}
	return sb.String()
}
