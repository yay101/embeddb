package embeddb

import (
	"fmt"
	"os"
	"testing"
)

func TestAdaptiveCache(t *testing.T) {
	os.Remove("/tmp/test_adaptive_cache.db")
	defer os.Remove("/tmp/test_adaptive_cache.db")

	db, err := Open("/tmp/test_adaptive_cache.db", OpenOptions{
		CachePages: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[BenchUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		tbl.Insert(&BenchUser{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@test.com", i),
			Age:   i % 100,
		})
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 1000; j++ {
			tbl.Get(uint32(j + 1))
		}
	}

	stats := db.Stats()
	cacheStats, ok := stats.CacheStats["primary"]
	if !ok {
		t.Fatal("no primary cache stats")
	}

	t.Logf("Cache: size=%d filled=%d hits=%d misses=%d hitRate=%.2f",
		cacheStats.Size, cacheStats.Filled, cacheStats.Hits, cacheStats.Misses, cacheStats.HitRate)

	if cacheStats.HitRate < 0.5 {
		t.Errorf("expected hit rate > 0.5, got %.2f", cacheStats.HitRate)
	}
}

func TestAdaptiveCacheGrowth(t *testing.T) {
	os.Remove("/tmp/test_adaptive_growth.db")
	defer os.Remove("/tmp/test_adaptive_growth.db")

	db, err := Open("/tmp/test_adaptive_growth.db", OpenOptions{
		CachePages: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[BenchUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2000; i++ {
		tbl.Insert(&BenchUser{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@test.com", i),
			Age:   i % 100,
		})
	}

	initialStats := db.Stats()
	initialSize := initialStats.CacheStats["primary"].Size
	t.Logf("Initial cache size: %d", initialSize)

	for i := 0; i < 100; i++ {
		for j := 0; j < 2000; j++ {
			tbl.Get(uint32(j + 1))
		}
	}

	finalStats := db.Stats()
	finalSize := finalStats.CacheStats["primary"].Size
	finalHitRate := finalStats.CacheStats["primary"].HitRate

	t.Logf("Cache: %d -> %d (filled=%d, hitRate=%.2f)", initialSize, finalSize, finalStats.CacheStats["primary"].Filled, finalHitRate)
}
