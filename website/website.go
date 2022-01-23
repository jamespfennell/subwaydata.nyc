package main

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"net/http"
	"path"
	"regexp"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/jamespfennell/subwaydata.nyc/website/html"
)

//go:embed static/*
var staticFiles embed.FS

const dataDownloadPath = `/data/([a-z]+)-(\d{4}-\d{2}-\d{2})-(csv|sql|gtfsrt)\.tar\.xz`

var dataDownloadPathRegex = regexp.MustCompile(dataDownloadPath)

var extToContentType = map[string]string{
	".css":  "text/css",
	".html": "text/html",
	".jpg":  "image/jpeg",
	".json": "application/json",
}

var flagPort = flag.Int("port", 8080, "the port to run the HTTP server on")

func main() {
	static := newStaticProvider("/", staticFiles)
	static.register(http.DefaultServeMux)
	location, err := time.LoadLocation("EST")
	if err != nil {
		panic(fmt.Sprintf("Failed to load EST location: %s", err))
	}
	h := handlerFactory{
		config:        metadata.NewProvider(),
		staticHandler: static,
		estLocation:   location,
	}
	templates := html.GetTemplates()
	http.HandleFunc("/", h.TemplateRootHandler(templates.Home, templates.PageNotFound))
	http.HandleFunc("/explore-the-data", h.TemplateHandler(templates.ExploreTheData))
	http.HandleFunc("/programmatic-access", h.TemplateHandler(templates.ProgrammaticAccess))
	http.HandleFunc("/data-schema", h.TemplateHandler(templates.DataSchema))
	http.HandleFunc("/how-it-works", h.TemplateHandler(templates.HowItWorks))
	http.HandleFunc("/config/nycsubway.json", h.ConfigHandler("nycsubway"))
	// http.HandleFunc("/data/", h.DataHandler())
	log.Printf("Launching HTTP server on port %d\n", *flagPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *flagPort), nil))
}

type handlerFactory struct {
	config        *metadata.Provider
	staticHandler staticProvider
	estLocation   *time.Location
}

func (h handlerFactory) TemplateHandler(t *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		input := struct {
			NYCSubway   *metadata.Metadata
			StaticFiles staticProvider
		}{
			NYCSubway:   h.config.Config("nycsubway"),
			StaticFiles: h.staticHandler,
		}
		// TODO: handle the status error with a public message?
		if err := t.Execute(w, input); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Printf("Failed to write templated response: %s\n", err)
		}
		w.Header().Set("Content Type", extToContentType[".html"])
	}
}

func (h handlerFactory) TemplateRootHandler(t *template.Template, t404 *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var f http.HandlerFunc
		if r.URL.Path != "/" {
			w.WriteHeader(http.StatusNotFound)
			f = h.TemplateHandler(t404)
		} else {
			f = h.TemplateHandler(t)
		}
		f(w, r)
	}
}

func (h handlerFactory) ConfigHandler(id string) http.HandlerFunc {
	if h.config.Config(id) == nil {
		panic(fmt.Sprintf("Cannot set up config handler for non-existant config %s", id))
	}
	return func(w http.ResponseWriter, r *http.Request) {
		b, err := json.MarshalIndent(h.config.Config(id), "", "  ")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Printf("Failed to marshal config: %s\n", err)
		}
		if _, err := io.Copy(w, bytes.NewReader(b)); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Printf("Failed to write config response: %s\n", err)
		}
		w.Header().Set("Content-Type", "application/json")
	}
}

/*
func (h handlerFactory) DataHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		match := dataDownloadPathRegex.FindStringSubmatch(r.URL.Path)
		if match == nil {
			// No such file
			w.WriteHeader(http.StatusNotFound)
			return
		}
		t, err := time.ParseInLocation("2006-01-02", match[2], h.estLocation)
		if err != nil {
			// The date provided is invalid
			w.WriteHeader(http.StatusNotFound)
			return
		}
		c := h.config.Config(match[1])
		if c == nil {
			// Invalid dataset ID
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var d *metadata.Day
		// TODO: make this not O(n) by storing available days in a hash map
		for _, day := range c.ProcessedDays {
			if t.Equal(time.Time(day.Day)) {
				d = &day.Day
				break
			}
		}
		if d == nil {
			// There is no data for this day
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// TODO: redirect to the right placeg
		http.Redirect(w, r, "https://realtimerail.nyc", http.StatusSeeOther)
	}
}
*/

type staticProvider struct {
	keyToPath     map[string]string
	pathToContent map[string][]byte
}

func newStaticProvider(root string, files embed.FS) staticProvider {
	s := staticProvider{
		keyToPath:     map[string]string{},
		pathToContent: map[string][]byte{},
	}
	err := fs.WalkDir(files, ".", func(filePath string, d fs.DirEntry, _ error) error {
		if d.IsDir() {
			return nil
		}
		if extToContentType[path.Ext(filePath)] == "" {
			return fmt.Errorf("unknown file format for extension %s", path.Ext(filePath))
		}
		b, err := files.ReadFile(filePath)
		if err != nil {
			return err
		}
		filePath = root + filePath
		dir, base := path.Split(filePath)
		hash := sha256.Sum256(b)
		hashedFilePath := fmt.Sprintf("%s%x.%s", dir, hash[:8], base)
		s.keyToPath[filePath] = hashedFilePath
		s.pathToContent[hashedFilePath] = b
		log.Printf("Registered static file %s under %s\n", filePath, hashedFilePath)
		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize static handler: %s", err))
	}
	return s
}

func (s staticProvider) Path(k string) string {
	return s.keyToPath[k]
}

func (s staticProvider) register(mux *http.ServeMux) {
	for p, content := range s.pathToContent {
		content := content
		contentType := extToContentType[path.Ext(p)]
		mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", contentType)
			// TODO: cache control
			// TODO: handle error
			io.Copy(w, bytes.NewReader(content))
		})
	}
}
