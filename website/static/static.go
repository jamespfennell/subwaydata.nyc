package static

import (
	_ "embed"
)

//go:embed stylesheet.css
var stylesheetCss string

//go:embed west-8th-street.jpg
var west8thStreetJpg string

type File struct {
	Path    string
	Content string
}

func (f File) FullPath() string {
	return "/static/" + f.Path
}

type Files struct {
	StylesheetCss    File
	West8thStreetJpg File
}

func (files Files) All() []File {
	return []File{
		files.StylesheetCss,
		files.West8thStreetJpg,
	}
}

func Get() Files {
	return Files{
		StylesheetCss: File{
			Path:    "stylesheet.css",
			Content: stylesheetCss,
		},
		West8thStreetJpg: File{
			Path:    "west-8th-street.jpg",
			Content: west8thStreetJpg,
		},
	}
}
