package git

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/jamespfennell/subwaydata.nyc/metadata"
)

type Session struct {
	dir          string
	metadataPath string
	readOnly     bool
	m            sync.Mutex
}

func NewReadOnlySession(url, branch, metadataPath string) (*Session, error) {
	s, err := newSession(fmt.Sprintf("https://%s", url), branch, metadataPath)
	if err != nil {
		return nil, err
	}
	s.readOnly = false
	return s, nil
}

func NewWritableSession(url, user, password, email, branch, metadataPath string) (*Session, error) {
	s, err := newSession(fmt.Sprintf("https://%s:%s@%s", user, password, url), branch, metadataPath)
	if err != nil {
		return nil, err
	}
	if err := s.runCommand("config", "user.email", email); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.runCommand("config", "user.name", user); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func newSession(fullUrl, branch, metadataPath string) (*Session, error) {
	dir, err := os.MkdirTemp("", "subwaydata.nyc-git-*")
	if err != nil {
		return nil, err
	}
	s := &Session{dir: dir}
	if err := s.runCommand("clone", "-b", branch, "--single-branch", fullUrl, "."); err != nil {
		_ = s.Close()
		return nil, err
	}
	return &Session{
		dir:          dir,
		metadataPath: metadataPath,
	}, nil
}

func (s *Session) Close() error {
	return os.RemoveAll(s.dir)
}

func (s *Session) ReadMetadata() (*metadata.Metadata, error) {
	s.m.Lock()
	defer s.m.Unlock()
	// TODO: reset the repository and pull
	// git reset --hard @{u}
	// git clean -df
	// git pull

	metadataPath := filepath.Join(s.dir, s.metadataPath)
	metadataB, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}
	m := &metadata.Metadata{}
	if err := json.Unmarshal(metadataB, m); err != nil {
		return nil, err
	}
	return m, nil
}

type UpdateMetadataFunc func(*metadata.Metadata) (bool, string)

// UpdateMetadata updates the metadata stored in the repository.
func (s *Session) UpdateMetadata(f UpdateMetadataFunc) error {
	if s.readOnly {
		return fmt.Errorf("cannot update the metadata in a read-only Git session")
	}
	s.m.Lock()
	defer s.m.Unlock()
	// TODO: reset the repository and pull

	metadataPath := filepath.Join(s.dir, s.metadataPath)
	metadataB, err := os.ReadFile(metadataPath)
	if err != nil {
		return err
	}
	m := &metadata.Metadata{}
	if err := json.Unmarshal(metadataB, m); err != nil {
		return err
	}
	commit, commitMsg := f(m)
	if !commit {
		return nil
	}
	newMetadataB, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(metadataPath, newMetadataB, 0600); err != nil {
		return err
	}
	if err := s.runCommand("add", metadataPath); err != nil {
		return err
	}
	if err := s.runCommand("commit", "-m", commitMsg); err != nil {
		return err
	}
	if err := s.runCommand("push"); err != nil {
		return err
	}
	return nil
}

func (s *Session) runCommand(args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = s.dir
	_, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	return nil
}
