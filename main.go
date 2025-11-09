package main

import (
	"fmt"
	"os"
	projectoptimizer "parqlite/project-optimizer"
	"reflect"
)

func handleErr(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	f, err := os.Open("data/history.parquet")
	handleErr(err)
	r1 := projectoptimizer.IterRowGroupsWithPrune(f, "country", "lat", "lon", "date", "temp_mean_c_approx")
	r2 := projectoptimizer.IterRowGroupsWithPruneFilter(f, []string{"country", "lat", "lon", "date", "temp_mean_c_approx"}, func(v reflect.Value) bool {
		lon := v.FieldByName("Lon").Float()
		return lon > 19
	})
	readRecords(r1)
	fmt.Printf("\n\n")
	readRecords(r2)

}

func readRecords(r *projectoptimizer.RecordBatch) {
	fmt.Printf("record Schmea %v\n", r.Schema)
	for i, col := range r.Columns {
		recordSchema := r.Schema.Fields[i]
		name := recordSchema.Name
		Type := recordSchema.PqType
		fmt.Printf("column: %s | type: %v | values : %v\n", name, Type, col)
	}

}

/*
scheam message schema {
        optional binary date (STRING);
        optional binary country (STRING);
        optional binary country_alpha2 (STRING);
        optional binary capital (STRING);
        optional double lat;
        optional double lon;
        optional double temp_min_c;
        optional double temp_max_c;
        optional double temp_mean_c_approx;
        optional double app_temp_min_c;
        optional double app_temp_max_c;
        optional double precip_mm;
        optional double rain_mm;
        optional double snow_mm;
        optional double windspeed_10m_max_kmh;
        optional double windgusts_10m_max_kmh;
        optional double wind_dir_dom_deg;
        optional double sunshine_duration_s;
        optional double daylight_duration_s;
        optional double shortwave_radiation_MJ_m2;

*/
