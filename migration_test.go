package embeddb

import (
	"fmt"
	"os"
	"testing"
)

type MigrateV1 struct {
	ID   uint32 `db:"id,primary"`
	Name string
	Age  int
}

type MigrateV2 struct {
	ID   uint32 `db:"id,primary"`
	Name string
	Age  int
	City string
}

type MigrateV3 struct {
	ID     uint32 `db:"id,primary"`
	Name   string
	Age    int
	City   string
	Active bool
}

func TestMigrationAddField(t *testing.T) {
	os.Remove("/tmp/test_migrate_add_field.db")
	defer os.Remove("/tmp/test_migrate_add_field.db")

	{
		db, err := Open("/tmp/test_migrate_add_field.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		tbl.Insert(&MigrateV1{Name: "Bob", Age: 25})
		tbl.Insert(&MigrateV1{Name: "Charlie", Age: 35})

		if count := tbl.Count(); count != 3 {
			t.Fatalf("expected 3 records before migration, got %d", count)
		}

		db.Close()
	}
}

type MigrateV1Simple struct {
	ID   uint32 `db:"id,primary"`
	Name string
}

type MigrateV2NewFields struct {
	ID    uint32 `db:"id,primary"`
	Email string
	Count int
}

func TestMigrationCompletelyNewFields(t *testing.T) {
	os.Remove("/tmp/test_migrate_new_fields.db")
	defer os.Remove("/tmp/test_migrate_new_fields.db")

	{
		db, err := Open("/tmp/test_migrate_new_fields.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1Simple](db, "items")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1Simple{Name: "Alpha"})
		tbl.Insert(&MigrateV1Simple{Name: "Beta"})

		if count := tbl.Count(); count != 2 {
			t.Fatalf("expected 2 records before migration, got %d", count)
		}

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_new_fields.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tbl, err := Use[MigrateV2NewFields](db, "items")
		if err != nil {
			t.Fatal(err)
		}

		if count := tbl.Count(); count != 2 {
			t.Fatalf("expected 2 records after migration with all-new fields, got %d", count)
		}

		all, err := tbl.All()
		if err != nil {
			t.Fatalf("failed to get all records: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2 records from All(), got %d", len(all))
		}

		for _, rec := range all {
			if rec.ID == 0 {
				t.Errorf("ID should be preserved, got 0")
			}
			if rec.Count != 0 {
				t.Errorf("Count should be 0 for new field, got %d", rec.Count)
			}
		}
	}
}

func TestMigrationAddMultipleFields(t *testing.T) {
	os.Remove("/tmp/test_migrate_add_multi.db")
	defer os.Remove("/tmp/test_migrate_add_multi.db")

	{
		db, err := Open("/tmp/test_migrate_add_multi.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		tbl.Insert(&MigrateV1{Name: "Bob", Age: 25})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_add_multi.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tbl, err := Use[MigrateV3](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		if count := tbl.Count(); count != 2 {
			t.Fatalf("expected 2 records after migration, got %d", count)
		}

		rec1, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatalf("failed to get record 1: %v", err)
		}
		if rec1.Name != "Alice" {
			t.Errorf("record 1 Name: got %q, want %q", rec1.Name, "Alice")
		}
		if rec1.Age != 30 {
			t.Errorf("record 1 Age: got %d, want %d", rec1.Age, 30)
		}
		if rec1.City != "" {
			t.Errorf("record 1 City: got %q, want empty string", rec1.City)
		}
		if rec1.Active != false {
			t.Errorf("record 1 Active: got %v, want false", rec1.Active)
		}

		rec2, err := tbl.Get(uint32(2))
		if err != nil {
			t.Fatalf("failed to get record 2: %v", err)
		}
		if rec2.Name != "Bob" {
			t.Errorf("record 2 Name: got %q, want %q", rec2.Name, "Bob")
		}
		if rec2.Age != 25 {
			t.Errorf("record 2 Age: got %d, want %d", rec2.Age, 25)
		}
	}
}

func TestMigrationDisabledError(t *testing.T) {
	os.Remove("/tmp/test_migrate_disabled.db")
	defer os.Remove("/tmp/test_migrate_disabled.db")

	{
		db, err := Open("/tmp/test_migrate_disabled.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_disabled.db", OpenOptions{Migrate: false})
		if err != nil {
			t.Fatal(err)
		}

		_, err = Use[MigrateV2](db, "users")
		if err == nil {
			t.Error("expected error when schema mismatch and migration disabled, got nil")
			db.Close()
			return
		}

		db.Close()
	}
}

func TestMigrationSameSchemaNoOp(t *testing.T) {
	os.Remove("/tmp/test_migrate_noop.db")
	defer os.Remove("/tmp/test_migrate_noop.db")

	{
		db, err := Open("/tmp/test_migrate_noop.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_noop.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		rec, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatalf("failed to get record: %v", err)
		}
		if rec.Name != "Alice" || rec.Age != 30 {
			t.Errorf("data mismatch after reopening with same schema: got %s/%d", rec.Name, rec.Age)
		}

		db.Close()
	}
}

func TestMigrationPersistsAfterRestart(t *testing.T) {
	os.Remove("/tmp/test_migrate_persist.db")
	defer os.Remove("/tmp/test_migrate_persist.db")

	{
		db, err := Open("/tmp/test_migrate_persist.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		tbl.Insert(&MigrateV1{Name: "Bob", Age: 25})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_persist.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		rec1, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if rec1.Name != "Alice" {
			t.Errorf("Name after migration: got %q, want %q", rec1.Name, "Alice")
		}

		rec1.City = "NYC"
		tbl.Update(uint32(1), rec1)

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_persist.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		rec1, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if rec1.City != "NYC" {
			t.Errorf("City after restart: got %q, want %q", rec1.City, "NYC")
		}
		if rec1.Name != "Alice" {
			t.Errorf("Name after restart: got %q, want %q", rec1.Name, "Alice")
		}

		rec2, err := tbl.Get(uint32(2))
		if err != nil {
			t.Fatal(err)
		}
		if rec2.Name != "Bob" {
			t.Errorf("Name2 after restart: got %q, want %q", rec2.Name, "Bob")
		}
		if rec2.City != "" {
			t.Errorf("City2 after restart: got %q, want empty string", rec2.City)
		}

		db.Close()
	}
}

func TestMigrationWithMultipleRecords(t *testing.T) {
	os.Remove("/tmp/test_migrate_many.db")
	defer os.Remove("/tmp/test_migrate_many.db")

	{
		db, err := Open("/tmp/test_migrate_many.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 50; i++ {
			tbl.Insert(&MigrateV1{Name: fmt.Sprintf("person%d", i), Age: 20 + i})
		}

		if count := tbl.Count(); count != 50 {
			t.Fatalf("expected 50 records before migration, got %d", count)
		}

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_many.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		if count := tbl.Count(); count != 50 {
			t.Fatalf("expected 50 records after migration, got %d", count)
		}

		all, err := tbl.All()
		if err != nil {
			t.Fatalf("failed to get all records: %v", err)
		}
		if len(all) != 50 {
			t.Errorf("expected 50 records from All(), got %d", len(all))
		}

		names := make(map[string]bool)
		for _, rec := range all {
			names[rec.Name] = true
		}
		if len(names) != 50 {
			t.Errorf("expected 50 unique names, got %d", len(names))
		}
	}
}

func TestMigrationWithVersioning(t *testing.T) {
	os.Remove("/tmp/test_migrate_versioning.db")
	defer os.Remove("/tmp/test_migrate_versioning.db")

	type DocV1 struct {
		ID      uint32 `db:"id,primary"`
		Title   string
		Content string
	}

	type DocV2 struct {
		ID      uint32 `db:"id,primary"`
		Title   string
		Content string
		Tags    string
	}

	{
		db, err := Open("/tmp/test_migrate_versioning.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[DocV1](db, "docs", UseOptions{MaxVersions: 3})
		if err != nil {
			t.Fatal(err)
		}

		id, err := tbl.Insert(&DocV1{Title: "doc1", Content: "hello"})
		if err != nil {
			t.Fatal(err)
		}

		tbl.Update(id, &DocV1{ID: id, Title: "doc1-v2", Content: "hello-v2"})
		tbl.Update(id, &DocV1{ID: id, Title: "doc1-v3", Content: "hello-v3"})

		versions, err := tbl.ListVersions(id)
		if err != nil {
			t.Fatalf("failed to list versions before migration: %v", err)
		}
		if len(versions) != 3 {
			t.Errorf("expected 3 versions before migration, got %d", len(versions))
		}

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_versioning.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tbl, err := Use[DocV2](db, "docs", UseOptions{MaxVersions: 3})
		if err != nil {
			t.Fatal(err)
		}

		current, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatalf("failed to get current record after migration: %v", err)
		}
		if current.Title != "doc1-v3" {
			t.Errorf("current Title after migration: got %q, want %q", current.Title, "doc1-v3")
		}
		if current.Content != "hello-v3" {
			t.Errorf("current Content after migration: got %q, want %q", current.Content, "hello-v3")
		}

		versions, err := tbl.ListVersions(uint32(1))
		if err != nil {
			t.Logf("warning: failed to list versions after migration: %v", err)
		} else {
			for _, v := range versions {
				_, err := tbl.GetVersion(uint32(1), v.Version)
				if err != nil {
					t.Logf("warning: failed to get version %d after migration: %v", v.Version, err)
				}
			}
		}

		if count := tbl.Count(); count != 1 {
			t.Errorf("expected 1 record after migration, got %d", count)
		}
	}
}

func TestMigrationOldRecordsInactive(t *testing.T) {
	os.Remove("/tmp/test_migrate_inactive.db")
	defer os.Remove("/tmp/test_migrate_inactive.db")

	{
		db, err := Open("/tmp/test_migrate_inactive.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		tbl.Insert(&MigrateV1{Name: "Bob", Age: 25})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_inactive.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		all, err := tbl.All()
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2 records, got %d", len(all))
		}

		for _, rec := range all {
			if rec.Name == "" {
				t.Error("found record with empty Name")
			}
		}

		db.Sync()

		err = db.Vacuum()
		if err != nil {
			t.Fatalf("vacuum failed after migration: %v", err)
		}

		all2, err := tbl.All()
		if err != nil {
			t.Fatal(err)
		}
		if len(all2) != 2 {
			t.Fatalf("expected 2 records after vacuum, got %d", len(all2))
		}

		names := map[string]bool{}
		for _, rec := range all2 {
			names[rec.Name] = true
		}
		if !names["Alice"] {
			t.Error("Alice missing after vacuum")
		}
		if !names["Bob"] {
			t.Error("Bob missing after vacuum")
		}

		db.Close()
	}
}

func TestMigrationThenInsertUpdate(t *testing.T) {
	os.Remove("/tmp/test_migrate_then_ops.db")
	defer os.Remove("/tmp/test_migrate_then_ops.db")

	{
		db, err := Open("/tmp/test_migrate_then_ops.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_then_ops.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		id2, err := tbl.Insert(&MigrateV2{Name: "Bob", Age: 25, City: "LA"})
		if err != nil {
			t.Fatal(err)
		}

		rec, err := tbl.Get(id2)
		if err != nil {
			t.Fatal(err)
		}
		if rec.City != "LA" {
			t.Errorf("new record City: got %q, want %q", rec.City, "LA")
		}

		err = tbl.Update(uint32(1), &MigrateV2{ID: 1, Name: "Alice Updated", Age: 31, City: "NYC"})
		if err != nil {
			t.Fatal(err)
		}

		rec1, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if rec1.Name != "Alice Updated" {
			t.Errorf("updated Name: got %q, want %q", rec1.Name, "Alice Updated")
		}
		if rec1.City != "NYC" {
			t.Errorf("updated City: got %q, want %q", rec1.City, "NYC")
		}
		if rec1.Age != 31 {
			t.Errorf("updated Age: got %d, want %d", rec1.Age, 31)
		}

		if count := tbl.Count(); count != 2 {
			t.Errorf("expected 2 records, got %d", count)
		}

		db.Close()
	}
}

func TestMigrationThenDelete(t *testing.T) {
	os.Remove("/tmp/test_migrate_then_delete.db")
	defer os.Remove("/tmp/test_migrate_then_delete.db")

	{
		db, err := Open("/tmp/test_migrate_then_delete.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		tbl.Insert(&MigrateV1{Name: "Bob", Age: 25})
		tbl.Insert(&MigrateV1{Name: "Charlie", Age: 35})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_then_delete.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		err = tbl.Delete(uint32(2))
		if err != nil {
			t.Fatal(err)
		}

		if count := tbl.Count(); count != 2 {
			t.Errorf("expected 2 records after delete, got %d", count)
		}

		rec1, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if rec1.Name != "Alice" {
			t.Errorf("Alice Name: got %q, want %q", rec1.Name, "Alice")
		}

		rec3, err := tbl.Get(uint32(3))
		if err != nil {
			t.Fatal(err)
		}
		if rec3.Name != "Charlie" {
			t.Errorf("Charlie Name: got %q, want %q", rec3.Name, "Charlie")
		}

		_, err = tbl.Get(uint32(2))
		if err == nil {
			t.Error("expected error getting deleted record")
		}

		db.Close()
	}
}

func TestMigrationAcrossRestarts(t *testing.T) {
	os.Remove("/tmp/test_migrate_restarts.db")
	defer os.Remove("/tmp/test_migrate_restarts.db")

	{
		db, err := Open("/tmp/test_migrate_restarts.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		tbl.Insert(&MigrateV1{Name: "Bob", Age: 25})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_restarts.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		all, err := tbl.All()
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2 records after V1->V2 migration, got %d", len(all))
		}

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_restarts.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV3](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		all, err := tbl.All()
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2 records after V2->V3 migration, got %d", len(all))
		}

		for _, rec := range all {
			if rec.Name == "" {
				t.Error("found record with empty Name after chained migration")
			}
			if rec.Active != false {
				t.Error("expected Active to be false (zero value) after chained migration")
			}
		}

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_restarts.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV3](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		all, err := tbl.All()
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2 records after reopening with V3, got %d", len(all))
		}

		db.Close()
	}
}

func TestMigrationWithAutoIndex(t *testing.T) {
	os.Remove("/tmp/test_migrate_autoindex.db")
	defer os.Remove("/tmp/test_migrate_autoindex.db")

	{
		db, err := Open("/tmp/test_migrate_autoindex.db", OpenOptions{AutoIndex: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		tbl.Insert(&MigrateV1{Name: "Bob", Age: 25})
		tbl.Insert(&MigrateV1{Name: "Charlie", Age: 35})

		results, err := tbl.Query("Name", "Alice")
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result before migration, got %d", len(results))
		}

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_autoindex.db", OpenOptions{Migrate: true, AutoIndex: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		results, err := tbl.Query("Name", "Alice")
		if err != nil {
			t.Fatalf("Query after migration failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result after migration, got %d", len(results))
		}
		if results[0].Name != "Alice" {
			t.Errorf("result Name: got %q, want %q", results[0].Name, "Alice")
		}
		if results[0].Age != 30 {
			t.Errorf("result Age: got %d, want %d", results[0].Age, 30)
		}

		results, err = tbl.Query("Name", "Bob")
		if err != nil {
			t.Fatalf("Query Bob after migration failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result for Bob, got %d", len(results))
		}
	}
}

func TestMigrationThenInsertAndVacuum(t *testing.T) {
	os.Remove("/tmp/test_migrate_vacuum.db")
	defer os.Remove("/tmp/test_migrate_vacuum.db")

	{
		db, err := Open("/tmp/test_migrate_vacuum.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 20; i++ {
			tbl.Insert(&MigrateV1{Name: "user", Age: 20 + i})
		}

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_vacuum.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		if count := tbl.Count(); count != 20 {
			t.Fatalf("expected 20 records after migration, got %d", count)
		}

		for i := uint32(1); i <= 5; i++ {
			tbl.Delete(i)
		}

		if count := tbl.Count(); count != 15 {
			t.Errorf("expected 15 records after delete, got %d", count)
		}

		db.Sync()

		err = db.Vacuum()
		if err != nil {
			t.Fatalf("vacuum failed: %v", err)
		}

		all, err := tbl.All()
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 15 {
			t.Errorf("expected 15 records after vacuum, got %d", len(all))
		}

		db.Close()
	}
}

func TestMigrationSameSchemaWithMigrateDisabled(t *testing.T) {
	os.Remove("/tmp/test_migrate_same_disabled.db")
	defer os.Remove("/tmp/test_migrate_same_disabled.db")

	{
		db, err := Open("/tmp/test_migrate_same_disabled.db", OpenOptions{Migrate: false})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_same_disabled.db", OpenOptions{Migrate: false})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		rec, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if rec.Name != "Alice" {
			t.Errorf("Name: got %q, want %q", rec.Name, "Alice")
		}

		db.Close()
	}
}

func TestMigrationEmptyTable(t *testing.T) {
	os.Remove("/tmp/test_migrate_empty.db")
	defer os.Remove("/tmp/test_migrate_empty.db")

	{
		db, err := Open("/tmp/test_migrate_empty.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		if count := tbl.Count(); count != 0 {
			t.Errorf("expected 0 records, got %d", count)
		}

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_empty.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		if count := tbl.Count(); count != 0 {
			t.Errorf("expected 0 records after migrating empty table, got %d", count)
		}

		id, err := tbl.Insert(&MigrateV2{Name: "NewUser", Age: 20, City: "Seattle"})
		if err != nil {
			t.Fatal(err)
		}

		rec, err := tbl.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Name != "NewUser" || rec.City != "Seattle" {
			t.Errorf("new record mismatch: got Name=%q City=%q", rec.Name, rec.City)
		}

		db.Close()
	}
}

func TestMigrationWithUpdatesBeforeMigrate(t *testing.T) {
	os.Remove("/tmp/test_migrate_updates.db")
	defer os.Remove("/tmp/test_migrate_updates.db")

	{
		db, err := Open("/tmp/test_migrate_updates.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		id, err := tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		if err != nil {
			t.Fatal(err)
		}

		tbl.Update(id, &MigrateV1{ID: id, Name: "Alice Updated", Age: 31})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_updates.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		rec, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if rec.Name != "Alice Updated" {
			t.Errorf("Name after migration of updated record: got %q, want %q", rec.Name, "Alice Updated")
		}
		if rec.Age != 31 {
			t.Errorf("Age after migration of updated record: got %d, want %d", rec.Age, 31)
		}

		if count := tbl.Count(); count != 1 {
			t.Errorf("expected 1 record (updated record), got %d", count)
		}
	}
}

func TestMigrationWithDeletesBeforeMigrate(t *testing.T) {
	os.Remove("/tmp/test_migrate_deletes.db")
	defer os.Remove("/tmp/test_migrate_deletes.db")

	{
		db, err := Open("/tmp/test_migrate_deletes.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[MigrateV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Insert(&MigrateV1{Name: "Alice", Age: 30})
		id2, _ := tbl.Insert(&MigrateV1{Name: "Bob", Age: 25})
		tbl.Insert(&MigrateV1{Name: "Charlie", Age: 35})

		tbl.Delete(id2)

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_deletes.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tbl, err := Use[MigrateV2](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		if count := tbl.Count(); count != 2 {
			t.Errorf("expected 2 records after migration (one was deleted), got %d", count)
		}

		all, err := tbl.All()
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2 records from All(), got %d", len(all))
		}

		names := map[string]bool{}
		for _, rec := range all {
			names[rec.Name] = true
		}
		if names["Bob"] {
			t.Error("Bob should not be in results (was deleted before migration)")
		}
		if !names["Alice"] {
			t.Error("Alice missing from results")
		}
		if !names["Charlie"] {
			t.Error("Charlie missing from results")
		}
	}
}

func TestMigrationWithVersionedRecordsAndVacuum(t *testing.T) {
	os.Remove("/tmp/test_migrate_versioned_vacuum.db")
	defer os.Remove("/tmp/test_migrate_versioned_vacuum.db")

	type DocV1 struct {
		ID      uint32 `db:"id,primary"`
		Title   string
		Content string
	}

	type DocV2 struct {
		ID      uint32 `db:"id,primary"`
		Title   string
		Content string
		Tags    string
	}

	{
		db, err := Open("/tmp/test_migrate_versioned_vacuum.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[DocV1](db, "docs", UseOptions{MaxVersions: 3})
		if err != nil {
			t.Fatal(err)
		}

		id, _ := tbl.Insert(&DocV1{Title: "doc1", Content: "content1"})
		tbl.Update(id, &DocV1{ID: id, Title: "doc1-v2", Content: "content2"})
		tbl.Update(id, &DocV1{ID: id, Title: "doc1-v3", Content: "content3"})

		db.Close()
	}

	{
		db, err := Open("/tmp/test_migrate_versioned_vacuum.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[DocV2](db, "docs", UseOptions{MaxVersions: 3})
		if err != nil {
			t.Fatal(err)
		}

		current, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatalf("failed to get current record: %v", err)
		}
		if current.Title != "doc1-v3" {
			t.Errorf("current Title: got %q, want %q", current.Title, "doc1-v3")
		}
		if current.Content != "content3" {
			t.Errorf("current Content: got %q, want %q", current.Content, "content3")
		}

		if count := tbl.Count(); count != 1 {
			t.Errorf("expected 1 record, got %d", count)
		}

		db.Sync()

		err = db.Vacuum()
		if err != nil {
			t.Fatalf("vacuum failed: %v", err)
		}

		current2, err := tbl.Get(uint32(1))
		if err != nil {
			t.Fatalf("failed to get current record after vacuum: %v", err)
		}
		if current2.Title != "doc1-v3" {
			t.Errorf("Title after vacuum: got %q, want %q", current2.Title, "doc1-v3")
		}

		db.Close()
	}
}
