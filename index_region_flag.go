package embeddb

import "os"

func useExperimentalRegionIndex() bool {
	return os.Getenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX") == "1"
}
