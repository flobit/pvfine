package services

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Leases cover the interval before an index is attached to core as well as
// its live session. Deletion and lease acquisition share one mutex; neither
// calls back into core while holding it.
var cacheLeases = struct {
	sync.Mutex
	paths map[string]int
}{paths: make(map[string]int)}
var cacheMaintenance = make(chan struct{}, 1)

func requestCacheMaintenance() {
	select {
	case cacheMaintenance <- struct{}{}:
	default:
	}
}

func holdCachePaths(paths ...string) func() {
	cacheLeases.Lock()
	for _, path := range paths {
		if path == "" {
			continue
		}
		cacheLeases.paths[path]++
		now := time.Now()
		_ = os.Chtimes(path, now, now)
	}
	cacheLeases.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			cacheLeases.Lock()
			for _, path := range paths {
				if path == "" {
					continue
				}
				cacheLeases.paths[path]--
				if cacheLeases.paths[path] == 0 {
					delete(cacheLeases.paths, path)
				}
			}
			cacheLeases.Unlock()
			requestCacheMaintenance()
		})
	}
}

func leasedCachePathsLocked() []string {
	var paths []string
	for path := range cacheLeases.paths {
		paths = append(paths, path, path+"-wal", path+"-shm", path+"-journal")
	}
	return paths
}

// StartMaintenance launches background cache budget cleanup loops.
func (s *CacheService) StartMaintenance(ctx context.Context) error {
	s.cancel, s.done = nil, make(chan struct{})
	ctx, s.cancel = context.WithCancel(ctx)
	go func() {
		defer close(s.done)
		requestCacheMaintenance()
		for {
			select {
			case <-ctx.Done():
				return
			case <-cacheMaintenance:
				// Coalesce completion/close notifications without delaying user operations.
				timer := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				if _, err := s.EnforceBudget(); err != nil {
					LogServiceError(err)
				}
			}
		}
	}()
	return nil
}

func (s *CacheService) ServiceShutdown() error {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
	return nil
}

type cacheUnit struct {
	path  string
	files []string
	size  int64
	used  time.Time
}

// Only recognized, complete cache entries are eligible. Temporary publication
// files and unknown files are counted but never removed automatically. A shard
// directory and a SQLite database with all sidecars are each one eviction unit.
func collectCacheUnits(root string) []cacheUnit {
	var units []cacheUnit
	for _, family := range []string{"archive-index", "search-index", "advanced-search", ""} {
		dir := filepath.Join(root, family)
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			name := entry.Name()
			if entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			recognized := false
			switch family {
			case "archive-index":
				recognized = !entry.IsDir() && !strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".db")
			case "search-index":
				recognized = !entry.IsDir() && strings.HasSuffix(name, ".json.gz")
			case "advanced-search":
				recognized = (strings.HasPrefix(name, "refs-v") && (entry.IsDir() || strings.HasSuffix(name, ".db"))) || (entry.IsDir() && strings.HasPrefix(name, "session-"))
			case "":
				recognized = !entry.IsDir() && name == "npk-image-index.json"
			}
			if !recognized {
				continue
			}
			path := filepath.Join(dir, name)
			info, err := entry.Info()
			if err != nil {
				continue
			}
			unit := cacheUnit{path: path, used: info.ModTime()}
			if entry.IsDir() {
				_ = walkCacheRoot(path, func(p string, i fs.FileInfo) {
					if i.Mode().IsRegular() {
						unit.files = append(unit.files, p)
						unit.size += i.Size()
					}
				})
			} else {
				for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
					p := path + suffix
					if i, err := os.Lstat(p); err == nil && i.Mode().IsRegular() {
						unit.files = append(unit.files, p)
						unit.size += i.Size()
					}
				}
			}
			units = append(units, unit)
		}
	}
	return units
}

func unitProtected(unit cacheUnit, protected []string) bool {
	if pathInUse(unit.path, protected) {
		return true
	}
	for _, path := range protected {
		if pathWithin(unit.path, path) {
			return true
		}
	}
	for _, path := range unit.files {
		if pathInUse(path, protected) {
			return true
		}
	}
	return false
}

// EnforceBudget applies the shared soft budget, retaining all live leases and
// recovery backups. Once exceeded, it aims for 80% to avoid repeated small IO.
func (s *CacheService) EnforceBudget() (CacheClearResult, error) {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	settings, err := s.settings.GetSettings()
	if err != nil {
		return CacheClearResult{}, err
	}
	usage, err := s.Usage()
	if err != nil {
		return CacheClearResult{}, err
	}
	if settings.CacheLimitGiB == 0 || usage.TotalBytes <= usage.LimitBytes {
		return CacheClearResult{Usage: usage}, nil
	}
	protected := append(s.inUsePaths(), s.autosaveSlotFiles()...)
	cacheLeases.Lock()
	protected = append(protected, leasedCachePathsLocked()...)
	units := collectCacheUnits(usage.Path)
	sort.Slice(units, func(i, j int) bool { return units[i].used.Before(units[j].used) })
	var freed int64
	target := usage.LimitBytes * 4 / 5
	for _, unit := range units {
		if usage.TotalBytes-freed <= target {
			break
		}
		if unitProtected(unit, protected) {
			continue
		}
		// Remove only the files measured above, never follow links or remove a
		// directory recursively. Count actual successful removals, including partial IO failures.
		for _, path := range unit.files {
			if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() {
				if os.Remove(path) == nil {
					freed += info.Size()
				}
			}
		}
		if info, err := os.Lstat(unit.path); err == nil && info.IsDir() {
			pruneEmptyCacheDirs(unit.path, nil)
			_ = os.Remove(unit.path)
		}
	}
	cacheLeases.Unlock()
	usage, err = s.Usage()
	return CacheClearResult{FreedBytes: freed, Usage: usage}, err
}
