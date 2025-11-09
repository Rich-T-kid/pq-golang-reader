# little doc to refrence for coding

```bash
type PartialRecord struct {
	Data             string  `parquet:"data"`
	Country          string  `parquet:"country"`
	Capital          string  `parquet:"capital"`
	Lat              float64 `parquet:"lat"`
	Long             float64 `parquet:"lon"`
	WindSpeed        float64 `parquet:"windspeed_10m_max_kmh"`
	DaylightDuration float64 `parquet:"daylight_duration_s;"`
}
```

## projection limit 
```bash
	reader := parquet.NewReader(f, parquet.SchemaOf(&PartialRecord{}))
```
* only reads the columns that are mentioned in the struct

## generate custom structs
```bash
func genStructWithFields(fields ...string) any {
	var res []reflect.StructField
	for _, field := range fields {
		res = append(res, reflect.StructField{
			Name: capitalize(field),
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`parquet:"` + field + `"`),
		})
	}
	typ := reflect.StructOf(res)
	return reflect.New(typ).Interface()
}
```
* and then use these run-time structs for projection pruning 
```bash 
reader := parquet.NewReader(f, parquet.SchemaOf(genStructWithFields("country", "lat", "lon")))
```