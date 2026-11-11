package services

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"pvfine/internal/pvf"
	pvfversion "pvfine/internal/version"
)

type ArchiveDiffDraft struct {
	FileIndex int32  `json:"fileIndex"`
	Path      string `json:"path"`
	Text      string `json:"text"`
}

type ArchiveDiffRequest struct {
	Path   string             `json:"path"`
	Drafts []ArchiveDiffDraft `json:"drafts"`
}

type ArchiveDiffStatus struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	CurrentPath string `json:"currentPath"`
	OtherPath   string `json:"otherPath"`
	Processed   int    `json:"processed"`
	Total       int    `json:"total"`
	Added       int    `json:"added"`
	Modified    int    `json:"modified"`
	Deleted     int    `json:"deleted"`
	Error       string `json:"error"`
}

type ArchiveDiffNode struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	IsDir     bool   `json:"isDir"`
	Operation string `json:"operation"`
	Count     int    `json:"count"`
}

type ArchiveDiffFile struct {
	Path          string `json:"path"`
	Operation     string `json:"operation"`
	BeforeHash    string `json:"beforeHash"`
	AfterHash     string `json:"afterHash"`
	BeforeType    int32  `json:"beforeType"`
	AfterType     int32  `json:"afterType"`
	BeforeSize    int32  `json:"beforeSize"`
	AfterSize     int32  `json:"afterSize"`
	BeforeExists  bool   `json:"beforeExists"`
	AfterExists   bool   `json:"afterExists"`
	BeforeText    string `json:"beforeText"`
	AfterText     string `json:"afterText"`
	TextAvailable bool   `json:"textAvailable"`
	Message       string `json:"message"`
}

type archiveDiffSession struct {
	status           ArchiveDiffStatus
	cancel           context.CancelFunc
	current, other   *pvf.Archive
	changes          map[string]pvfversion.FileChange
	children         map[string][]ArchiveDiffNode
	filteredChildren map[string]map[string][]ArchiveDiffNode
	detail           *ArchiveDiffFile
	detailLimit      int32
	drafts           map[string]ArchiveDiffDraft
}

var archiveDiffSequence atomic.Uint64

// ArchiveDiffService owns an isolated read-only comparison. It never installs
// the comparison archive in core or initializes workspace-derived indexes.
type ArchiveDiffService struct {
	c       *core
	mu      sync.Mutex
	scanMu  sync.Mutex
	session *archiveDiffSession
}

func NewArchiveDiffService(c *core) *ArchiveDiffService {
	s := &ArchiveDiffService{c: c}
	c.archiveDiff = s
	return s
}

func (s *ArchiveDiffService) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != nil {
		s.session.cancel()
		s.session = nil
	}
}

func (s *ArchiveDiffService) Cancel(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != nil && s.session.status.ID == id {
		s.session.cancel()
		s.session.current, s.session.other = nil, nil
		s.session.changes, s.session.children = nil, nil
		s.session.status.State = "cancelled"
	}
}

func (s *ArchiveDiffService) Start(request ArchiveDiffRequest) (*ArchiveDiffStatus, error) {
	s.Release()
	// Lock ordering is always core then diff; workspace replacement can release
	// comparisons while holding the core lock without deadlocking detail reads.
	s.c.mu.RLock()
	defer s.c.mu.RUnlock()
	a := s.c.archive
	if a == nil {
		return nil, ErrNoArchive
	}
	if strings.TrimSpace(request.Path) == "" {
		return nil, fmt.Errorf("请选择对比归档")
	}
	stage := a.CloneForBatch()
	ctx, cancel := context.WithCancel(context.Background())
	draftsMap := make(map[string]ArchiveDiffDraft, len(request.Drafts))
	for _, draft := range request.Drafts {
		draftsMap[pvfversion.CanonicalPath(draft.Path)] = draft
	}
	p := &archiveDiffSession{
		cancel: cancel,
		status: ArchiveDiffStatus{
			ID: fmt.Sprint(archiveDiffSequence.Add(1)), State: "loading", CurrentPath: a.SourcePath(), OtherPath: request.Path,
		},
		drafts: draftsMap,
	}
	s.mu.Lock()
	if s.session != nil {
		s.session.cancel()
	}
	s.session = p
	status := p.status
	s.mu.Unlock()
	go s.compare(ctx, p, stage, request)
	return &status, nil
}

func (s *ArchiveDiffService) Status(id string) (*ArchiveDiffStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.findLocked(id, false)
	if err != nil {
		return nil, err
	}
	status := p.status
	return &status, nil
}

func (s *ArchiveDiffService) findLocked(id string, ready bool) (*archiveDiffSession, error) {
	p := s.session
	if p == nil || p.status.ID != id {
		return nil, fmt.Errorf("对比会话已关闭，请重新比较")
	}
	if ready && p.status.State != "ready" {
		return nil, fmt.Errorf("对比尚未完成")
	}
	return p, nil
}

func (s *ArchiveDiffService) compare(ctx context.Context, p *archiveDiffSession, stage *pvf.Archive, request ArchiveDiffRequest) {
	// Serialize scans so a cancelled worker finishes releasing its parsed
	// archive before the next worker opens another potentially large PVF.
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	fail := func(err error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.session == p && ctx.Err() == nil {
			p.status.State = "error"
			p.status.Error = err.Error()
		}
	}
	for _, draft := range request.Drafts {
		if ctx.Err() != nil {
			return
		}
		if draft.FileIndex < 0 || draft.FileIndex >= stage.FileCount() || pvfversion.CanonicalPath(stage.Path(draft.FileIndex)) != pvfversion.CanonicalPath(draft.Path) {
			fail(fmt.Errorf("文件草稿已过期，请重新比较"))
			return
		}
		if err := stage.SetText(draft.FileIndex, draft.Text); err != nil {
			fail(fmt.Errorf("应用草稿 %s 失败: %w", draft.Path, err))
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	// Minimal open: parsing only, no ArchiveService/core registration or events.
	other, err := pvf.Open(request.Path)
	if err != nil {
		fail(err)
		return
	}
	if ctx.Err() != nil {
		return
	}
	total := int(stage.FileCount() + other.FileCount())
	s.mu.Lock()
	if s.session != p || ctx.Err() != nil {
		s.mu.Unlock()
		return
	}
	p.status.State = "comparing"
	p.status.Total = total
	s.mu.Unlock()
	progress := func(n int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.session == p && p.status.State == "comparing" {
			p.status.Processed = n
		}
	}
	phase := func(state string, n, total int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.session == p && ctx.Err() == nil {
			p.status.State, p.status.Processed, p.status.Total = state, n, total
		}
	}
	changes, err := archiveDiffComparePhases(ctx, other, stage, progress, phase)
	if err != nil {
		fail(err)
		return
	}
	phase("building", 0, len(changes))
	children, filteredChildren, err := buildAllDiffTrees(ctx, changes, phase)
	if err != nil {
		fail(err)
		return
	}
	changeIndex := make(map[string]pvfversion.FileChange, len(changes))
	var added, deleted, modified int
	for _, change := range changes {
		if ctx.Err() != nil {
			return
		}
		changeIndex[change.Path] = change
		switch change.Operation {
		case pvfversion.OperationAdd:
			added++
		case pvfversion.OperationDelete:
			deleted++
		default:
			modified++
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != p || ctx.Err() != nil {
		return
	}
	p.current, p.other = stage, other
	p.children, p.filteredChildren, p.changes = children, filteredChildren, changeIndex
	p.status.Added, p.status.Deleted, p.status.Modified = added, deleted, modified
	p.status.Processed, p.status.Total = total, total
	p.status.State = "ready"
}

// Keep one CPU available for the desktop UI; small archives never spawn more
// workers than files. Do not cap CPU-bound comparison work at a fixed four.
func archiveDiffWorkerCount(files int32) int {
	return min(int(files), max(1, runtime.GOMAXPROCS(0)-1))
}

// Hash the same type-prefixed logical bytes as version snapshots without
// allocating a second full payload solely to prepend the four-byte type.
func archiveDiffContentHash(dataType int32, data []byte) string {
	var prefix [4]byte
	binary.LittleEndian.PutUint32(prefix[:], uint32(dataType))
	hash := sha256.New()
	_, _ = hash.Write(prefix[:])
	_, _ = hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

// Retained as a convenient synchronous helper for tree tests.
func archiveDiffChildren(changes []pvfversion.FileChange) map[string][]ArchiveDiffNode {
	result, _ := archiveDiffBuildChildren(context.Background(), changes, nil)
	return result
}

func archiveDiffBuildChildren(ctx context.Context, changes []pvfversion.FileChange, progress func(string, int, int)) (map[string][]ArchiveDiffNode, error) {
	type treeEntry struct {
		node ArchiveDiffNode
		key  string
	}
	type directory struct {
		entry    treeEntry
		parent   *directory
		children []*treeEntry
	}
	root := &directory{}
	dirs := map[string]*directory{"": root}
	var ensureDir func(string, string) *directory
	ensureDir = func(key, display string) *directory {
		if dir := dirs[key]; dir != nil {
			return dir
		}
		parentKey, parentDisplay, name := "", "", display
		if i := strings.LastIndexByte(key, '/'); i >= 0 {
			parentKey = key[:i]
		}
		if i := strings.LastIndexByte(display, '/'); i >= 0 {
			parentDisplay, name = display[:i], display[i+1:]
		}
		parent := ensureDir(parentKey, parentDisplay)
		dir := &directory{entry: treeEntry{node: ArchiveDiffNode{Path: display, Name: name, IsDir: true}, key: key}, parent: parent}
		dirs[key] = dir
		parent.children = append(parent.children, &dir.entry)
		return dir
	}
	// Directory identities and sort keys are computed once. Leaves append
	// directly, avoiding a map entry and repeated normalization per ancestor.
	for i, change := range changes {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if progress != nil {
				progress("building", i, len(changes))
			}
		}
		key := change.Path
		display := pvfversion.DisplayPath(change.DisplayPath)
		parentKey, parentDisplay, name := "", "", display
		if i := strings.LastIndexByte(key, '/'); i >= 0 {
			parentKey = key[:i]
		}
		if i := strings.LastIndexByte(display, '/'); i >= 0 {
			parentDisplay, name = display[:i], display[i+1:]
		}
		dir := ensureDir(parentKey, parentDisplay)
		dir.children = append(dir.children, &treeEntry{node: ArchiveDiffNode{Path: display, Name: name, Count: 1, Operation: string(change.Operation)}, key: key})
		for ancestor := dir; ancestor != nil; ancestor = ancestor.parent {
			ancestor.entry.node.Count++
		}
	}
	if progress != nil {
		progress("sorting-tree", 0, len(dirs))
	}
	result := make(map[string][]ArchiveDiffNode, len(dirs))
	sorted := 0
	for key, dir := range dirs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sort.Slice(dir.children, func(i, j int) bool {
			a, b := dir.children[i], dir.children[j]
			if a.node.IsDir != b.node.IsDir {
				return a.node.IsDir
			}
			return a.key < b.key
		})
		list := make([]ArchiveDiffNode, len(dir.children))
		for i, entry := range dir.children {
			list[i] = entry.node
		}
		if len(list) > 0 {
			result[key] = list
		}
		sorted++
		if progress != nil && sorted%64 == 0 {
			progress("sorting-tree", sorted, len(dirs))
		}
	}
	if progress != nil {
		progress("sorting-tree", len(dirs), len(dirs))
	}
	return result, ctx.Err()
}

// buildAllDiffTrees 根据变更列表构建全量树以及针对 add/modify/delete 的三棵过滤树
func buildAllDiffTrees(ctx context.Context, changes []pvfversion.FileChange, phase func(string, int, int)) (map[string][]ArchiveDiffNode, map[string]map[string][]ArchiveDiffNode, error) {
	allTree, err := archiveDiffBuildChildren(ctx, changes, phase)
	if err != nil {
		return nil, nil, err
	}

	var addChanges, modChanges, delChanges []pvfversion.FileChange
	for _, change := range changes {
		switch change.Operation {
		case pvfversion.OperationAdd:
			addChanges = append(addChanges, change)
		case pvfversion.OperationDelete:
			delChanges = append(delChanges, change)
		default:
			modChanges = append(modChanges, change)
		}
	}

	filtered := make(map[string]map[string][]ArchiveDiffNode, 3)
	if tree, err := archiveDiffBuildChildren(ctx, addChanges, nil); err == nil {
		filtered["add"] = tree
	}
	if tree, err := archiveDiffBuildChildren(ctx, modChanges, nil); err == nil {
		filtered["modify"] = tree
	}
	if tree, err := archiveDiffBuildChildren(ctx, delChanges, nil); err == nil {
		filtered["delete"] = tree
	}

	return allTree, filtered, nil
}

// FilteredChildren 按照目录路径与操作类型（all/add/modify/delete）返回子节点列表
func (s *ArchiveDiffService) FilteredChildren(id, path, operation string) ([]ArchiveDiffNode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.findLocked(id, true)
	if err != nil {
		return nil, err
	}
	canonicalPath := pvfversion.CanonicalPath(path)
	op := strings.ToLower(strings.TrimSpace(operation))
	if op == "" || op == "all" {
		return append([]ArchiveDiffNode{}, p.children[canonicalPath]...), nil
	}
	if p.filteredChildren != nil {
		if tree, ok := p.filteredChildren[op]; ok {
			return append([]ArchiveDiffNode{}, tree[canonicalPath]...), nil
		}
	}
	return []ArchiveDiffNode{}, nil
}

func (s *ArchiveDiffService) Children(id, path string) ([]ArchiveDiffNode, error) {
	return s.FilteredChildren(id, path, "all")
}

func (s *ArchiveDiffService) FileDiff(id, path string) (*ArchiveDiffFile, error) {
	limit := s.c.editableByteLimit()
	// Capture references under the session lock. Parsing/detail IO never holds
	// it, so closing or cancelling remains responsive.
	s.mu.Lock()
	p, err := s.findLocked(id, true)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	canonicalPath := pvfversion.CanonicalPath(path)
	change, ok := p.changes[canonicalPath]
	before, after := p.other, p.current
	if ok && p.detail != nil && p.detail.Path == change.DisplayPath && p.detailLimit == limit {
		result := *p.detail
		s.mu.Unlock()
		return &result, nil
	}
	s.mu.Unlock()
	if !ok {
		// 若文件已从差异列表中移除（例如刚刚合入完成），但在归档中仍存在，则平滑返回无差异视图
		if before == nil || after == nil {
			return nil, fmt.Errorf("文件不在差异列表中")
		}
		bi, bok := before.Find(canonicalPath)
		ai, aok := after.Find(canonicalPath)
		if !bok && !aok {
			return nil, fmt.Errorf("文件不在差异列表中")
		}
		var bType, aType int32
		if bok {
			bType, _, _ = before.FileContentInfo(bi)
		}
		if aok {
			aType, _, _ = after.FileContentInfo(ai)
		}
		display := path
		if bok {
			display = before.Path(bi)
		} else if aok {
			display = after.Path(ai)
		}
		change = pvfversion.FileChange{
			Path:        canonicalPath,
			DisplayPath: pvfversion.DisplayPath(display),
			Operation:   "none",
			BeforeType:  bType,
			AfterType:   aType,
		}
	}
	result := &ArchiveDiffFile{Path: change.DisplayPath, Operation: string(change.Operation), BeforeHash: change.BeforeHash, AfterHash: change.AfterHash, BeforeType: change.BeforeType, AfterType: change.AfterType}
	bi, bok := before.Find(change.Path)
	ai, aok := after.Find(change.Path)
	result.BeforeExists, result.AfterExists = bok, aok
	if bok {
		_, result.BeforeSize, _ = before.FileContentInfo(bi)
	}
	if aok {
		_, result.AfterSize, _ = after.FileContentInfo(ai)
	}
	isText := func(typ int32) bool { return typ == pvf.TypeScript || typ == pvf.TypeUnicode }
	mixed := (bok && !isText(result.BeforeType)) || (aok && !isText(result.AfterType))
	large := result.BeforeSize > limit || result.AfterSize > limit
	wantText := !mixed && !large
	if bok {
		result.BeforeText, result.BeforeHash, err = archiveDiffDetailSide(before, bi, result.BeforeHash, limit, wantText)
		if err != nil {
			return nil, err
		}
	}
	if aok {
		result.AfterText, result.AfterHash, err = archiveDiffDetailSide(after, ai, result.AfterHash, limit, wantText)
		if err != nil {
			return nil, err
		}
	}
	if mixed {
		result.Message = "二进制内容不同，不支持行级文本差异"
		if bok && aok && result.BeforeType != result.AfterType {
			result.Message = "文件类型发生变化，不支持行级文本差异"
		}
	} else if large {
		result.Message = "文件超过文本大小阈值，仅显示差异摘要；可在设置中调整阈值"
	} else {
		result.TextAvailable = true
	}
	// Retain only the last detail, with a strict byte cap; never keep decoded
	// text for the whole archive. A changed display limit invalidates the cache.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != p || p.status.State != "ready" {
		return nil, fmt.Errorf("对比会话已失效")
	}
	p.detail = nil
	if len(result.BeforeText)+len(result.AfterText) <= 8<<20 {
		cached := *result
		p.detail, p.detailLimit = &cached, limit
	}
	return result, nil
}

// Added/deleted files are classified from paths alone. Compute their logical
// hash only when requested, using the same raw read as their displayed text.
func archiveDiffDetailSide(a *pvf.Archive, index int32, knownHash string, limit int32, wantText bool) (string, string, error) {
	dataType, size, err := a.FileContentInfo(index)
	if err != nil {
		return "", "", err
	}
	textType := dataType == pvf.TypeScript || dataType == pvf.TypeUnicode
	if textType && size > limit {
		return "", knownHash, nil
	}
	if !wantText && knownHash != "" {
		return "", knownHash, nil
	}
	raw, err := a.RawBytes(index)
	if err != nil {
		return "", "", err
	}
	if !textType {
		return "", archiveDiffContentHash(dataType, raw), nil
	}
	text, err := a.CanonicalTextFromRaw(index, raw)
	if err != nil {
		return "", "", err
	}
	if knownHash == "" {
		knownHash = archiveDiffContentHash(dataType, []byte(text))
	}
	if !wantText {
		text = ""
	}
	return text, knownHash, nil
}
