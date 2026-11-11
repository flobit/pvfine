//go:build !mcp_only

package services

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wailsapp/wails/v3/pkg/application"

	modpkg "pvfine/internal/mod"
	"pvfine/internal/pvf"
	pvfversion "pvfine/internal/version"
)

// OpenDialog 弹出文件选择框并加载归档。
func (s *ArchiveService) OpenDialog() (*ArchiveInfo, error) {
	path, err := application.Get().Dialog.OpenFile().
		CanChooseFiles(true).
		CanChooseDirectories(false).
		AddFilter("PVF 归档", "*.pvf").
		AddFilter("所有文件", "*").
		SetTitle("打开 PVF 归档").
		PromptForSingleSelection()
	if err != nil {
		return nil, err // 用户取消等
	}
	if path == "" {
		return nil, nil
	}
	info, err := s.Open(path)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// ImportFilesDialog opens a native multi-selection picker and imports the
// selected files or directories into targetDir.
func (s *ArchiveService) ImportFilesDialog(targetDir, mode string) (*ImportResult, error) {
	paths, err := s.SelectImportFilesDialog()
	if err != nil || len(paths) == 0 {
		return nil, err
	}
	return s.ImportFiles(paths, targetDir, mode)
}

// SelectImportFilesDialog opens the native multi-selection picker without
// changing the current archive. The returned paths can be previewed first.
func (s *ArchiveService) SelectImportFilesDialog() ([]string, error) {
	return application.Get().Dialog.OpenFile().
		CanChooseFiles(true).
		CanChooseDirectories(true).
		AddFilter("所有文件", "*").
		SetTitle("导入文件").
		PromptForMultipleSelection()
}

// SaveAsDialog 弹出保存对话框并另存为新 PVF。返回保存路径。
func (s *EditorService) SaveAsDialog() (string, error) {
	finishTask := s.c.archiveTasks.begin()
	defer finishTask()
	s.c.mu.RLock()
	a := s.c.archive
	if a == nil {
		s.c.mu.RUnlock()
		return "", ErrNoArchive
	}
	s.c.mu.RUnlock()
	defName := "Script_new.pvf"
	if src := a.SourcePath(); src != "" {
		defName = filepath.Base(src)
	}
	path, err := application.Get().Dialog.SaveFile().
		SetFilename(defName).
		AddFilter("PVF 归档", "*.pvf").
		SetMessage("另存为 PVF").
		PromptForSingleSelection()
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", nil // 用户取消
	}
	s.c.mu.RLock()
	sameArchive := s.c.archive == a
	s.c.mu.RUnlock()
	if !sameArchive {
		return "", ErrNoArchive
	}
	return s.saveCopy(path, true, "")
}

// ExportFilesDialog 将选中的文件或目录导出到目标目录,文件内容使用渲染后的 UTF-8 文本。
// 目录会递归展开,并保留归档内的相对路径。
func (s *EditorService) ExportFilesDialog(scopes []string) (string, error) {
	finishTask := s.c.archiveTasks.begin()
	defer finishTask()
	s.c.mu.RLock()
	a := s.c.archive
	if a == nil {
		s.c.mu.RUnlock()
		return "", ErrNoArchive
	}
	var selections []exportSelection
	if s.c.diskIndex != nil {
		selections = collectDiskExportSelections(a, s.c.diskIndex, scopes)
	} else {
		selections = collectExportSelections(a, s.c.sortedPaths, scopes)
	}
	s.c.mu.RUnlock()
	if len(selections) == 0 {
		return "", fmt.Errorf("没有可导出的文件")
	}
	dir, err := application.Get().Dialog.OpenFile().
		CanChooseFiles(false).
		CanChooseDirectories(true).
		CanCreateDirectories(true).
		SetTitle("选择导出目标目录").
		PromptForSingleSelection()
	if err != nil {
		return "", err
	}
	if dir == "" {
		return "", nil
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	s.c.mu.RLock()
	defer s.c.mu.RUnlock()
	if s.c.archive != a {
		return "", ErrNoArchive
	}
	for _, selection := range selections {
		text, err := a.Text(selection.index)
		if err != nil {
			return "", fmt.Errorf("渲染 %q 失败: %w", selection.path, err)
		}
		dst, err := safeExportPath(dir, selection.path)
		if err != nil {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(dst, []byte(text), 0o644); err != nil {
			return "", err
		}
	}
	return dir, nil
}

// UnpackDialog 选择目录后,在后台协程把整包解包到该目录。
// 进度通过事件 "unpack:progress" {done,total} 推送,结束发 "unpack:done"。
func (s *EditorService) UnpackDialog() (bool, error) {
	if !s.c.unpackRunning.CompareAndSwap(false, true) {
		return false, fmt.Errorf("解包正在进行中")
	}
	s.c.mu.RLock()
	a := s.c.archive
	s.c.mu.RUnlock()
	if a == nil {
		s.c.unpackRunning.Store(false)
		return false, ErrNoArchive
	}
	dir, err := application.Get().Dialog.OpenFile().
		CanChooseFiles(false).
		CanChooseDirectories(true).
		CanCreateDirectories(true).
		SetTitle("选择解包目标目录").
		PromptForSingleSelection()
	if err != nil {
		s.c.unpackRunning.Store(false)
		return false, err
	}
	if dir == "" {
		s.c.unpackRunning.Store(false)
		return false, nil // 用户取消
	}

	finishTask := s.c.archiveTasks.begin()
	s.c.mu.Lock()
	if s.c.archive != a || s.c.unpackCancel.Load() {
		s.c.mu.Unlock()
		finishTask()
		s.c.unpackRunning.Store(false)
		return false, nil
	}
	s.c.unpackCancel.Store(false)
	s.c.mu.Unlock()
	go func() {
		defer finishTask()
		defer s.c.unpackRunning.Store(false)
		emit := emitEvent
		total := int(a.FileCount())
		progress := func(done, tot int) { emit("unpack:progress", map[string]int{"done": done, "total": tot}) }
		cancel := func() bool { return s.c.unpackCancel.Load() }

		err := a.ExtractTo(dir, progress, cancel)
		if err == pvf.ErrCancelled {
			emit("unpack:done", map[string]any{"ok": false, "message": "解包已取消", "dir": dir})
			return
		}
		if err != nil {
			emit("unpack:done", map[string]any{"ok": false, "message": "解包失败: " + err.Error(), "dir": dir})
			return
		}
		emit("unpack:done", map[string]any{"ok": true, "message": fmt.Sprintf("已解包 %d 个文件到 %s", total, dir), "dir": dir})
	}()
	return true, nil
}

// ExecuteDialog picks the parent directory only after the frontend confirms.
func (s *ExportService) ExecuteDialog(id string) (string, error) {
	return s.ExecuteSelectionDialog(id, nil)
}

// ExecuteSelectionDialog applies exclusions to the frozen preview. Dependencies
// are not resolved again, so an explicitly excluded file cannot reappear.
func (s *ExportService) ExecuteSelectionDialog(id string, excluded []string) (string, error) {
	s.mu.Lock()
	s.c.mu.RLock()
	valid := s.plan != nil && s.plan.preview.ID == id && s.validLocked(s.plan)
	var selectionErr error
	if valid {
		_, selectionErr = s.selectedPlan(s.plan, excluded)
	}
	s.c.mu.RUnlock()
	s.mu.Unlock()
	if !valid {
		return "", ErrExportStale
	}
	if selectionErr != nil {
		return "", selectionErr
	}
	dir, err := application.Get().Dialog.OpenFile().
		CanChooseFiles(false).CanChooseDirectories(true).CanCreateDirectories(true).
		SetTitle("选择导出目标目录").PromptForSingleSelection()
	if err != nil || dir == "" {
		return "", err
	}
	return s.executeSelectedTo(id, dir, excluded)
}

// SelectMergeDialog reads metadata only after a source has been selected and validated.
func (s *ExportService) SelectMergeDialog(formatID string) (*ExportMergeSource, error) {
	f, err := s.registry.Get(formatID)
	if err != nil {
		return nil, err
	}
	if f.Reader == nil {
		return nil, fmt.Errorf("该 mod 格式不支持读取")
	}
	source, err := application.Get().Dialog.OpenFile().CanChooseFiles(formatID == modpkg.CN115Format).
		CanChooseDirectories(true).SetTitle("选择要合并的同格式 MOD 目录或 ZIP / .dnfmod").PromptForSingleSelection()
	if err != nil || source == "" {
		return nil, err
	}
	return s.mergeSourceMetadata(source, formatID)
}

func (s *ModImportService) PrepareDialog(formatID string) (*ModImportPreview, error) {
	if _, err := s.readableFormat(formatID); err != nil {
		return nil, err
	}
	dir, err := application.Get().Dialog.OpenFile().CanChooseFiles(formatID == modpkg.CN115Format).CanChooseDirectories(true).SetTitle("选择 mod 目录或 ZIP / .dnfmod").PromptForSingleSelection()
	if err != nil || dir == "" {
		return nil, err
	}
	return s.Prepare(dir, formatID)
}

// ExportCommitFilesDialog exports the files touched by one commit. Add/modify
// entries use their after-state; deleted entries use their before-state so the
// exported directory represents every file involved in that version.
func (s *VersionService) ExportCommitFilesDialog(commitID string) (string, error) {
	commitID = strings.TrimSpace(commitID)
	if commitID == "" {
		return "", errors.New("提交 ID 不能为空")
	}
	s.c.mu.RLock()
	if err := s.c.ensureVersionReadyLocked(); err != nil {
		s.c.mu.RUnlock()
		return "", err
	}
	repo := s.c.versionRepo
	if repo == nil {
		s.c.mu.RUnlock()
		return "", ErrVersionNotEnabled
	}
	if _, err := repo.GetCommit(commitID); err != nil {
		s.c.mu.RUnlock()
		return "", err
	}
	changes, err := repo.Changes(commitID)
	s.c.mu.RUnlock()
	if err != nil {
		return "", err
	}
	if len(changes) == 0 {
		return "", errors.New("该版本没有可导出的文件")
	}

	dir, err := application.Get().Dialog.OpenFile().
		CanChooseFiles(false).
		CanChooseDirectories(true).
		CanCreateDirectories(true).
		SetTitle("选择版本文件导出目录").
		PromptForSingleSelection()
	if err != nil {
		return "", err
	}
	if dir == "" {
		return "", nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	s.c.mu.RLock()
	defer s.c.mu.RUnlock()
	if s.c.versionRepo != repo {
		return "", ErrVersionNotEnabled
	}
	base := s.c.versionBaseArchive
	if base == nil {
		base, err = pvf.Open(repo.BasePath())
		if err != nil {
			return "", err
		}
	}
	seenDestinations := make(map[string]struct{}, len(changes))
	exported := 0
	for _, change := range changes {
		hash := change.AfterHash
		dataType := change.AfterType
		path := change.DisplayPath
		if hash == "" {
			hash = change.BeforeHash
			dataType = change.BeforeType
			if path == "" {
				path = change.OldDisplayPath
			}
		}
		if hash == "" || path == "" {
			continue
		}
		content, err := readVersionContent(repo, base, pvfversion.Entry{
			Path: path, DataType: dataType, Hash: hash,
		})
		if err != nil {
			return "", fmt.Errorf("读取版本文件 %q 失败: %w", path, err)
		}
		destination, err := safeExportPath(dir, path)
		if err != nil {
			return "", err
		}
		if _, exists := seenDestinations[destination]; exists {
			return "", fmt.Errorf("版本文件导出路径冲突: %s", path)
		}
		seenDestinations[destination] = struct{}{}
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(destination, content.Raw, 0o644); err != nil {
			return "", err
		}
		exported++
	}
	if exported == 0 {
		return "", errors.New("该版本没有可导出的文件")
	}
	return dir, nil
}

// ChooseCachePath asks the user for a backup file location.
func (s *AutosaveService) ChooseCachePath() (string, error) {
	if s == nil {
		return "", errors.New("定时缓存服务不可用")
	}
	name := "autosave.pvf"
	if current, err := s.cachePath(); err == nil && current != "" {
		name = filepath.Base(current)
	}
	path, err := application.Get().Dialog.SaveFile().
		SetFilename(name).
		AddFilter("PVF 备份缓存", "*.pvf").
		SetMessage("选择定时缓存文件").
		PromptForSingleSelection()
	if err != nil {
		return "", err
	}
	return path, nil // 空字符串表示用户取消
}

// ImportBookmarkBookDialog opens and parses one portable bookmark book.
func (s *BookmarkService) ImportBookmarkBookDialog() (*BookmarkBookFile, error) {
	paths, err := application.Get().Dialog.OpenFile().
		CanChooseFiles(true).
		CanChooseDirectories(false).
		AddFilter("书签簿 JSON", "*.json").
		SetTitle("导入书签簿").
		PromptForSingleSelection()
	if err != nil {
		return nil, err
	}
	if paths == "" {
		return nil, nil
	}
	data, err := os.ReadFile(paths)
	if err != nil {
		return nil, fmt.Errorf("读取书签簿导入文件失败: %w", err)
	}
	book, err := parseBookmarkBookFile(data)
	if err != nil {
		return nil, err
	}
	return &book, nil
}

// ExportBookmarkBookDialog saves one portable bookmark book selected by the
// frontend. Built-in books are intentionally exportable.
func (s *BookmarkService) ExportBookmarkBookDialog(book BookmarkBookFile) (string, error) {
	if err := normalizeBookmarkBookFile(&book); err != nil {
		return "", err
	}
	filename := safeBookmarkFilename(book.Name) + ".json"
	path, err := application.Get().Dialog.SaveFile().
		SetFilename(filename).
		AddFilter("书签簿 JSON", "*.json").
		SetMessage("导出书签簿").
		PromptForSingleSelection()
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", nil
	}
	data, err := marshalBookmarkJSON(book)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("写入书签簿导出文件失败: %w", err)
	}
	return path, nil
}

func (s *ArchiveDiffService) SelectArchive() (string, error) {
	return application.Get().Dialog.OpenFile().CanChooseFiles(true).CanChooseDirectories(false).
		AddFilter("PVF 归档", "*.pvf").SetTitle("选择对比归档").PromptForSingleSelection()
}

// SelectNPKDirectory opens the native directory chooser, persists the
// selected path and starts a new asynchronous index generation.
func (s *ImageService) SelectNPKDirectory() (ImageIndexStatus, error) {
	app := application.Get()
	if app == nil {
		return s.IndexStatus(), errors.New("当前运行环境不支持目录选择")
	}
	path, err := app.Dialog.OpenFile().
		CanChooseFiles(false).
		CanChooseDirectories(true).
		CanCreateDirectories(false).
		SetTitle("选择 NPK 目录").
		PromptForSingleSelection()
	if err != nil {
		return s.IndexStatus(), err
	}
	if strings.TrimSpace(path) == "" {
		return s.IndexStatus(), nil
	}
	directory, err := canonicalImageDirectory(path)
	if err != nil {
		return s.IndexStatus(), err
	}
	if s.settings != nil {
		if _, err := s.settings.UpdateNPKDirectory(directory); err != nil {
			return s.IndexStatus(), err
		}
	}
	s.switchDirectory(directory)
	return s.IndexStatus(), nil
}

func (s *SkillService) ChooseDirectory() (string, error) {
	return application.Get().Dialog.OpenFile().CanChooseFiles(false).CanChooseDirectories(true).
		CanCreateDirectories(true).SetTitle("选择 skill 安装目录或项目根目录").PromptForSingleSelection()
}
