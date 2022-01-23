package git

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
)

type UpdateMetadataFunc func(*metadata.Metadata) (bool, string)

// UpdateMetadata updates the metadata stored in the subwaydata.nyc Git repository.
func UpdateMetadata(f UpdateMetadataFunc, config *config.Config) error {
	session, err := newSession()
	if err != nil {
		return err
	}
	defer session.close()
	branch := config.GitBranch
	if branch == "" {
		branch = "main"
	}
	if err := session.runCommand("clone", "-b", branch, "--single-branch",
		fmt.Sprintf("https://%s:%s@%s", config.GitUser, config.GitPassword, config.GitUrl), "."); err != nil {
		return err
	}
	if err := session.runCommand("config", "user.email", config.GitEmail); err != nil {
		return err
	}
	if err := session.runCommand("config", "user.name", config.GitUser); err != nil {
		return err
	}

	metadataPath := filepath.Join(session.repoPath(), config.MetadataPath)
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
	if err := session.runCommand("add", metadataPath); err != nil {
		return err
	}
	if err := session.runCommand("commit", "-m", commitMsg); err != nil {
		return err
	}
	if err := session.runCommand("push"); err != nil {
		return err
	}
	return nil
}

type session struct {
	dir string
}

func newSession() (*session, error) {
	dir, err := os.MkdirTemp("", "subwaydata.nyc-git-*")
	if err != nil {
		return nil, err
	}
	fmt.Printf("Using temp directory %s for Git\n", dir)
	return &session{
		dir: dir,
	}, nil
}

func (s *session) close() error {
	return os.RemoveAll(s.dir)
}

func (s *session) runCommand(args ...string) error {
	cmd := exec.Command("git", args...)
	log.Printf("Running Git command %s\n", args)
	cmd.Dir = s.dir
	b, err := cmd.CombinedOutput()
	log.Printf("Git response: %s\n", string(b))
	if err != nil {
		log.Printf("Git error: %s\n", err)
		return err
	}

	return nil
}

func (s *session) repoPath() string {
	return s.dir
}
