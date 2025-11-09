package projectoptimizer

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// mock helper instead of opening a real parquet file
func generateData() *os.File {
	f, _ := os.CreateTemp("", "fake.parquet")
	return f
}

func TestPruneFields(t *testing.T) {
	p := &parquetSchema{
		Fields: []structField{
			{Name: "country", PqType: parquet.ByteArrayType},
			{Name: "lat", PqType: parquet.DoubleType},
			{Name: "lon", PqType: parquet.DoubleType},
			{Name: "temp", PqType: parquet.FloatType},
		},
	}

	p.KeepFields("country", "lat")

	if len(p.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(p.Fields))
	}

	names := []string{p.Fields[0].Name, p.Fields[1].Name}
	for _, want := range []string{"country", "lat"} {
		found := false
		for _, got := range names {
			if strings.EqualFold(want, got) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected field %q in pruned list", want)
		}
	}
}

func TestGenStructWithFields(t *testing.T) {
	fields := []structField{
		{Name: "country", PqType: parquet.ByteArrayType},
		{Name: "lat", PqType: parquet.DoubleType},
		{Name: "lon", PqType: parquet.DoubleType},
	}
	s, _ := genStructWithFields(fields...)

	typ := reflect.TypeOf(s).Elem()

	if typ.NumField() != len(fields) {
		t.Fatalf("expected %d fields, got %d", len(fields), typ.NumField())
	}

	for i, f := range fields {
		sf := typ.Field(i)
		if !strings.EqualFold(sf.Tag.Get("parquet"), f.Name) {
			t.Errorf("expected tag %q, got %q", f.Name, sf.Tag.Get("parquet"))
		}
	}
}

// generateDataFilter creates a temporary parquet file for filter tests.
// For the purposes of unit tests we just need a file handle; the real reader
// will attempt to open it when tests are run in integration. This helper keeps
// the same shape as existing generateData.
func generateDataFilter() *os.File {
	pwd, _ := os.Getwd()
	fmt.Printf("current pwd: %s", pwd)
	f, err := os.OpenFile("../data/history.parquet", os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	return f
}

// readRecords converts a RecordBatch into a slice of maps (one per row).
func readRecords(rb *RecordBatch) []map[string]any {
	if rb == nil {
		return nil
	}
	if len(rb.Columns) == 0 {
		return nil
	}
	// determine number of rows from first column
	nrows := len(rb.Columns[0])
	out := make([]map[string]any, nrows)
	for i := 0; i < nrows; i++ {
		m := make(map[string]any)
		for ci, field := range rb.Schema.Fields {
			var v any
			if i < len(rb.Columns[ci]) {
				v = rb.Columns[ci][i]
			} else {
				v = nil
			}
			m[field.Name] = v
		}
		out[i] = m
	}
	return out
}

func TestIterRowGroupsWithPruneFilter(t *testing.T) {
	f := generateDataFilter()
	print(f.Name())

	r1 := IterRowGroupsWithPruneFilter(f, []string{"country", "lat", "lon", "date", "temp_mean_c_approx"}, func(v reflect.Value) bool {
		country := v.FieldByName("Country").String()
		return country != "Angola"
	})

	recs2 := readRecords(r1)

	for _, r := range recs2 {
		if rv, ok := r["country"]; ok {
			if s, ok := rv.(string); ok && s == "Angola" {
				t.Errorf("filtered results contain Angola")
			}
		}
	}
}

func TestIterRowGroupsWithMultiplePredicates(t *testing.T) {
	f := generateDataFilter()
	// do not remove the fixture file used by other tests; just close
	defer f.Close()

	columns := []string{"country", "lat", "temp_mean_c_approx"}

	tests := []struct {
		name   string
		pred   func(reflect.Value) bool
		verify func(t *testing.T, recs []map[string]any)
	}{
		{
			name: "exclude-angola",
			pred: func(v reflect.Value) bool {
				fv := v.FieldByName("Country")
				if fv.Kind() == reflect.String {
					return fv.String() != "Angola"
				}
				if s, ok := fv.Interface().(string); ok {
					return s != "Angola"
				}
				return true
			},
			verify: func(t *testing.T, recs []map[string]any) {
				for _, r := range recs {
					if rv, ok := r["country"]; ok {
						if s, ok := rv.(string); ok && s == "Angola" {
							t.Errorf("filtered results contain Angola")
						}
					}
				}
			},
		},
		{
			name: "lat-positive",
			pred: func(v reflect.Value) bool {
				fv := v.FieldByName("Lat")
				switch fv.Kind() {
				case reflect.Float32, reflect.Float64:
					return fv.Float() > 0
				case reflect.Int, reflect.Int32, reflect.Int64:
					return fv.Int() > 0
				default:
					if f, ok := fv.Interface().(float64); ok {
						return f > 0
					}
					return false
				}
			},
			verify: func(t *testing.T, recs []map[string]any) {
				for _, r := range recs {
					if rv, ok := r["lat"]; ok {
						switch v := rv.(type) {
						case float64:
							if v <= 0 {
								t.Errorf("lat <= 0: %v", v)
							}
						case float32:
							if float64(v) <= 0 {
								t.Errorf("lat <= 0: %v", v)
							}
						case int, int32, int64:
							if reflect.ValueOf(v).Int() <= 0 {
								t.Errorf("lat <= 0: %v", v)
							}
						default:
							t.Logf("skipping unknown lat type: %T", v)
						}
					}
				}
			},
		},
		{
			name: "temp-high",
			pred: func(v reflect.Value) bool {
				fv := v.FieldByName("Temp_mean_c_approx")
				switch fv.Kind() {
				case reflect.Float32, reflect.Float64:
					return fv.Float() >= 30
				default:
					if f, ok := fv.Interface().(float64); ok {
						return f >= 30
					}
					return false
				}
			},
			verify: func(t *testing.T, recs []map[string]any) {
				for _, r := range recs {
					if rv, ok := r["temp_mean_c_approx"]; ok {
						switch v := rv.(type) {
						case float64:
							if v < 30 {
								t.Errorf("temp < 30: %v", v)
							}
						case float32:
							if float64(v) < 30 {
								t.Errorf("temp < 30: %v", v)
							}
						default:
							t.Logf("skipping unknown temp type: %T", v)
						}
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rb := IterRowGroupsWithPruneFilter(f, columns, tc.pred)
			recs := readRecords(rb)
			tc.verify(t, recs)
		})
	}
}

func TestProjectExecLeafProjection(t *testing.T) {
	// open the real fixture via helper
	f := generateDataFilter()
	defer f.Close()

	// create a leaf node that reads four columns
	leaf := NewProjectExecLeaf(f, []string{"lat", "lon", "country", "capital"}, nil)

	// ensure leaf schema has four fields
	if len(leaf.Schema().Fields) != 4 {
		t.Fatalf("expected leaf schema to have 4 fields, got %d", len(leaf.Schema().Fields))
	}

	// clone and keep only lat and country
	tmp := leaf.Schema().Clone()
	tmp.KeepFields("lat", "country")

	// create an upper-level project using the cloned/pruned schema
	proj := NewProjectExec(tmp, leaf, nil)

	// call Next once and validate returned batch schema and columns
	batch, err := proj.Next(3)
	if err != nil {
		// allow io.EOF (no rows) but fail on other errors
		if err != io.EOF {
			t.Fatalf("unexpected error from Next: %v", err)
		}
	}

	if len(batch.Schema.Fields) != 2 {
		t.Fatalf("expected projected schema to have 2 fields, got %d", len(batch.Schema.Fields))
	}

	// check field names are lat and country (case-insensitive)
	names := []string{batch.Schema.Fields[0].Name, batch.Schema.Fields[1].Name}
	want := []string{"lat", "country"}
	for _, w := range want {
		found := false
		for _, n := range names {
			if strings.EqualFold(w, n) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected projected field %q in result schema, got %v", w, names)
		}
	}

	// ensure returned columns length matches schema
	if len(batch.Columns) != len(batch.Schema.Fields) {
		t.Errorf("columns length (%d) does not match schema fields (%d)", len(batch.Columns), len(batch.Schema.Fields))
	}
}
