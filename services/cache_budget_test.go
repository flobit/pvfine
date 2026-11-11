package services

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func sparseCache(t *testing.T, path string, size int64, age time.Duration) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	err = f.Truncate(size)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
}

func TestCacheBudgetMigrationAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(path, []byte(`{"theme":"dark"}`), 0600); err != nil {
		t.Fatal(err)
	}
	service := newSettingsService(path)
	settings, err := service.GetSettings()
	if err != nil || settings.CacheLimitGiB != 10 {
		t.Fatalf("migration: %d %v", settings.CacheLimitGiB, err)
	}
	for _, limit := range []int{0, 10, 10240} {
		settings.CacheLimitGiB = limit
		if err := service.SaveSettings(settings); err != nil {
			t.Fatal(err)
		}
		loaded, err := service.GetSettings()
		if err != nil || loaded.CacheLimitGiB != limit {
			t.Fatalf("round trip %d: %v", limit, err)
		}
	}
	for _, limit := range []int{-1, 10241} {
		settings.CacheLimitGiB = limit
		if service.SaveSettings(settings) == nil {
			t.Fatalf("accepted invalid limit %d", limit)
		}
	}
}

func TestCacheBudgetLRUAndHysteresis(t *testing.T) {
	root := t.TempDir()
	settings := newSettingsService(filepath.Join(t.TempDir(), "settings.json"))
	config := DefaultAppSettings()
	config.CacheLimitGiB = 1
	config.AutosavePath = filepath.Join(root, "autosave.pvf")
	if err := settings.SaveSettings(config); err != nil {
		t.Fatal(err)
	}
	service := newCacheServiceWithRoot(NewCore(), settings, root)
	paths := []string{filepath.Join(root, "archive-index", "old.db"), filepath.Join(root, "search-index", "middle.json.gz"), filepath.Join(root, "npk-image-index.json")}
	// 1200 MiB -> 800 MiB, below 80% of 1 GiB. A recently accessed
	// originally oldest entry must survive while the next oldest is evicted.
	for i, path := range paths {
		sparseCache(t, path, 400<<20, time.Duration(3-i)*time.Hour)
	}
	release := holdCachePaths(paths[0])
	release()
	result, err := service.EnforceBudget()
	if err != nil {
		t.Fatal(err)
	}
	if result.FreedBytes != 400<<20 || result.Usage.TotalBytes != 800<<20 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if _, err := os.Stat(paths[1]); !os.IsNotExist(err) {
		t.Fatal("least recently used entry survived")
	}
	if _, err := os.Stat(paths[0]); err != nil {
		t.Fatal("recently used entry removed")
	}
	result, err = service.EnforceBudget()
	if err != nil || result.FreedBytes != 0 {
		t.Fatalf("unnecessary second cleanup: %#v %v", result, err)
	}
}

func TestCacheBudgetProtectsLeasesBackupsAndSidecars(t *testing.T) {
	root := t.TempDir()
	settings := newSettingsService(filepath.Join(t.TempDir(), "settings.json"))
	config := DefaultAppSettings()
	config.CacheLimitGiB = 1
	config.AutosavePath = filepath.Join(root, "archive-index", "recovery.pvf")
	if err := settings.SaveSettings(config); err != nil {
		t.Fatal(err)
	}
	backup := config.AutosavePath
	live := filepath.Join(root, "archive-index", "live.db")
	old := filepath.Join(root, "archive-index", "old.db")
	for _, path := range []string{backup, live, old} {
		sparseCache(t, path, 700<<20, time.Hour)
	}
	sparseCache(t, old+"-wal", 20<<20, time.Hour)
	sparseCache(t, live+"-wal", 10<<20, time.Hour)
	release := holdCachePaths(live)
	service := newCacheServiceWithRoot(NewCore(), settings, root)
	result, err := service.EnforceBudget()
	if err != nil {
		t.Fatal(err)
	}
	if result.FreedBytes != 720<<20 || result.Usage.BackupBytes != 700<<20 || !result.Usage.OverBudget || result.Usage.ReclaimableBytes != 0 {
		t.Fatalf("protection: %#v", result)
	}
	for _, path := range []string{backup, live, live + "-wal"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(old + "-wal"); !os.IsNotExist(err) {
		t.Fatal("orphaned SQLite sidecar")
	}
	release()
	result, err = service.EnforceBudget()
	if err != nil || result.FreedBytes != 710<<20 || result.Usage.TotalBytes != 700<<20 {
		t.Fatalf("released lease: %#v %v", result, err)
	}
}

func TestCacheBudgetUnlimitedAndExternalBackup(t *testing.T) {
	root := t.TempDir()
	settings := newSettingsService(filepath.Join(t.TempDir(), "settings.json"))
	config := DefaultAppSettings()
	config.CacheLimitGiB = 0
	config.AutosavePath = filepath.Join(t.TempDir(), "recovery.pvf")
	if err := settings.SaveSettings(config); err != nil {
		t.Fatal(err)
	}
	sparseCache(t, config.AutosavePath, 2<<30, time.Hour)
	sparseCache(t, filepath.Join(root, "archive-index", "old.db"), 11<<30, time.Hour)
	service := newCacheServiceWithRoot(NewCore(), settings, root)
	result, err := service.EnforceBudget()
	if err != nil || result.FreedBytes != 0 || result.Usage.LimitBytes != 0 || result.Usage.OverBudget {
		t.Fatalf("unlimited: %#v %v", result, err)
	}
	config.CacheLimitGiB = 1
	if err := settings.SaveSettings(config); err != nil {
		t.Fatal(err)
	}
	result, err = service.EnforceBudget()
	if err != nil || result.FreedBytes != 11<<30 || result.Usage.BackupBytes != 2<<30 || !result.Usage.OverBudget {
		t.Fatalf("external backup: %#v %v", result, err)
	}
	if _, err := os.Stat(config.AutosavePath); err != nil {
		t.Fatal(err)
	}
}

func TestCacheBudgetAutomaticStartupAndLeaseRelease(t *testing.T) {
	root := t.TempDir()
	settings := newSettingsService(filepath.Join(t.TempDir(), "settings.json"))
	config := DefaultAppSettings()
	config.CacheLimitGiB = 1
	config.AutosavePath = filepath.Join(root, "autosave.pvf")
	if err := settings.SaveSettings(config); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(root, "archive-index", "live.db")
	disposable := filepath.Join(root, "search-index", "old.json.gz")
	sparseCache(t, live, 2<<30, time.Hour)
	sparseCache(t, disposable, 10<<20, time.Hour)
	release := holdCachePaths(live)
	defer release()
	service := newCacheServiceWithRoot(NewCore(), settings, root)
	if err := service.StartMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer service.ServiceShutdown()
	waitRemoved := func(path string) {
		t.Helper()
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(path); os.IsNotExist(err) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("automatic maintenance did not remove cache")
	}
	waitRemoved(disposable)
	if _, err := os.Stat(live); err != nil {
		t.Fatal("startup evicted a live lease")
	}
	release()
	waitRemoved(live)
}
