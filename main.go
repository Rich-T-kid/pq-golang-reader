package main

import (
	"fmt"
	"os"
	projectoptimizer "parqlite/project-optimizer"
)

func handleErr(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	f, err := os.Open("data/history.parquet")
	handleErr(err)
	projectoptimizer.ReadRecordBatch(f, []string{"lat", "lon"})
	/*
	   	projectNodeLeaf := projectoptimizer.NewProjectExecLeaf(f, []string{"lat", "lon", "country", "capital"}, []projectoptimizer.FilterPredicate{
	   		func(v reflect.Value) bool {
	   			lat := v.FieldByName("Lat").Float()
	   			return lat == -12.06
	   		},
	   	})

	   tmpCopy := projectNodeLeaf.Schema().Clone()
	   tmpCopy.KeepFields("lat", "country")
	   proj := projectoptimizer.NewProjectExec(tmpCopy, projectNodeLeaf, nil)
	   sumExec, err := projectoptimizer.NewSumExec(proj, "lat")
	   handleErr(err)
	   fmt.Printf("schema: %v", sumExec.Schema().ShowSchema())
	*/
}

func DisplayRecords(displayer projectoptimizer.Display) {
	fmt.Fprintf(os.Stdout, "%s", displayer.Show())
	fmt.Println()
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
