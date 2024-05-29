package website

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/jamespfennell/subwaydata.nyc/website/html"
	"github.com/jamespfennell/subwaydata.nyc/website/static"
)

const (
	contentTypeHtml = "text/html"
	contentTypeCss  = "text/css"
	contentTypeJpg  = "image/jpeg"
	contentTypeJson = "application/json"
)

func Run(metadataUrl string, port int) {
	d := newDynamicContent(metadataUrl)
	pageNotFound := html.PageNotFound()
	http.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			rw.WriteHeader(http.StatusNotFound)
			writeResponse(rw, pageNotFound, contentTypeHtml)
			return
		}
		writeResponse(rw, d.getHome(), contentTypeHtml)
	})
	http.HandleFunc("/explore-the-data", func(rw http.ResponseWriter, r *http.Request) {
		writeResponse(rw, d.getExploreTheData(), contentTypeHtml)
	})
	http.HandleFunc("/metadata.json", func(rw http.ResponseWriter, r *http.Request) {
		writeResponse(rw, d.getMetadataJson(), contentTypeJson)
	})
	programmaticAccess := html.ProgrammaticAccess()
	http.HandleFunc("/programmatic-access", func(rw http.ResponseWriter, r *http.Request) {
		writeResponse(rw, programmaticAccess, contentTypeHtml)
	})
	dataSchema := html.DataSchema()
	http.HandleFunc("/data-schema", func(rw http.ResponseWriter, r *http.Request) {
		writeResponse(rw, dataSchema, contentTypeHtml)
	})
	howItWorks := html.HowItWorks()
	http.HandleFunc("/how-it-works", func(rw http.ResponseWriter, r *http.Request) {
		writeResponse(rw, howItWorks, contentTypeHtml)
	})
	http.HandleFunc("/refresh-metadata", func(rw http.ResponseWriter, r *http.Request) {
		d.update()
	})
	http.HandleFunc("/data/", func(rw http.ResponseWriter, r *http.Request) {
		path := r.URL.Path[6:]
		path, ok := d.getDataRedirect(path)
		if !ok {
			rw.WriteHeader(http.StatusNotFound)
			writeResponse(rw, pageNotFound, contentTypeHtml)
			return
		}
		http.Redirect(rw, r, fmt.Sprintf("https://data.subwaydata.nyc/%s", path), http.StatusFound)
	})

	for _, file := range static.Get().All() {
		file := file
		var contentType string
		ext := path.Ext(file.Path)
		switch ext {
		case ".jpg":
			contentType = contentTypeJpg
		case ".css":
			contentType = contentTypeCss
		default:
			panic("unknown content type in static files")
		}
		http.HandleFunc(file.FullPath(), func(rw http.ResponseWriter, r *http.Request) {
			writeResponse(rw, file.Content, contentType)
		})
	}

	log.Printf("Launching HTTP server on port %d\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

type dynamicContent struct {
	updateMutex sync.RWMutex
	metadataUrl string

	home           string
	exploreTheData string
	metadataJson   string
	dataRedirects  map[string]string
}

func newDynamicContent(metadataUrl string) *dynamicContent {
	d := dynamicContent{
		metadataUrl:    metadataUrl,
		home:           html.Home(nil),
		exploreTheData: html.ExploreTheData(nil),
		metadataJson:   "\"failed to load metadata\"",
		dataRedirects:  map[string]string{},
	}
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	firstUpdateDone := make(chan struct{})
	go func() {
		if err := d.update(); err != nil {
			log.Printf("Initial metadata update failed: %s", err)
		}
		firstUpdateDone <- struct{}{}
	}()
	select {
	case <-t.C:
		log.Printf("Timed out before initial metadata finished")
	case <-firstUpdateDone:
	}
	go func() {
		t := time.NewTicker(5 * time.Minute)
		for {
			<-t.C
			if err := d.update(); err != nil {
				log.Printf("Failed to update metadata: %s", err)
			}
		}
	}()
	return &d
}

func (d *dynamicContent) update() error {
	res, err := http.Get(d.metadataUrl)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	var m metadata.Metadata
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	home := html.Home(&m)
	exploreTheData := html.ExploreTheData(&m)
	redirects := map[string]string{}
	for i := range m.ProcessedDays {
		redirects[fmt.Sprintf("subwaydatanyc_%s_csv.tar.xz", m.ProcessedDays[i].Day)] = m.ProcessedDays[i].Csv.Path
	}
	d.updateMutex.Lock()
	defer d.updateMutex.Unlock()
	d.home = home
	d.exploreTheData = exploreTheData
	d.metadataJson = string(b)
	d.dataRedirects = redirects
	return nil
}

func (d *dynamicContent) getHome() string {
	d.updateMutex.RLock()
	defer d.updateMutex.RUnlock()
	return d.home
}

func (d *dynamicContent) getExploreTheData() string {
	d.updateMutex.RLock()
	defer d.updateMutex.RUnlock()
	return d.exploreTheData
}

func (d *dynamicContent) getMetadataJson() string {
	d.updateMutex.RLock()
	defer d.updateMutex.RUnlock()
	return d.metadataJson
}

func (d *dynamicContent) getDataRedirect(url string) (string, bool) {
	d.updateMutex.RLock()
	defer d.updateMutex.RUnlock()
	s, b := d.dataRedirects[url]
	return s, b
}

func writeResponse(w http.ResponseWriter, s string, contentType string) {
	w.Header().Set("Content-Type", contentType)
	if _, err := io.Copy(w, strings.NewReader(s)); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("Failed to write response: %s\n", err)
	}
}
