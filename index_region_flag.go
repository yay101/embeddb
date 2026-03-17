package embeddb

import "os"

func useExperimentalRegionIndex() bool {
	v := os.Getenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX")
	if v == "0" || v == "false" {
		return false
	}
	return true
}
