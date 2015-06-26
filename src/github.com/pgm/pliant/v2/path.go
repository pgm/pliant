package v2

import (
	"strings"
)

type Path struct {
	path []string
}

func (p *Path) Split() (*Path, string) {
	// split a path into parent directory path and filename
	//if len()p.path
	last := len(p.path) - 1
	return &Path{path: p.path[0:(last)]}, p.path[last]
}

func NewPath(path string) *Path {
	if path == "" {
		return &Path{path: make([]string, 0)}
	}
	// TODO: add validation of components of path and canonicalize path
	components := strings.Split(path, "/")
	return &Path{path: components}
}

func (p *Path) IsRoot() bool {
	return len(p.path) == 0
}
