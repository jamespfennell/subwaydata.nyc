package main

import (
	"embed"
	_ "embed"
	"fmt"
	config2 "github.com/jamespfennell/transitdata.nyc/config"
	"html/template"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

//go:embed html/*
var htmlFiles embed.FS

//go:embed static/*
var staticFiles embed.FS

const dataDownloadPath = `/data/([a-z]+)-(\d{4})-(\d{2})-(\d{2})-(csv|sql|gtfsrt)\.tar\.xz`
var dataDownloadPathRegex = regexp.MustCompile(dataDownloadPath)

func main() {
	// TODO: make our own fileServer that reads everything into memory and also verifies that there are only
	//  css and jpg files?
	http.Handle("/static/", http.FileServer(http.FS(staticFiles)))
	http.HandleFunc("/data/", dataDownloadHandler)


	pageHandler := pageHandler{}
	pageHandler.addPage(page{
		pattern:  "/",
		title:    "Home",
		template: "home.html",
	})
	pageHandler.addPage(page{
		pattern:  "/software",
		title:    "Software",
		template: "software.html",
	})
	http.Handle("/", &pageHandler)
	// TODO /config/nycsubway.json
	log.Println("Launching HTTP server")
	log.Fatal(http.ListenAndServe(":80", nil))

}

type page struct {
	pattern string
	title string
	template string
}

type pageHandler struct {
	patternToPage map[string]page
	config *config2.Config
}

func (h *pageHandler) addPage(p page) {
	if h.patternToPage == nil {
		h.patternToPage = map[string]page{}
	}
	if _, err := htmlFiles.ReadFile("html/" + p.template); err != nil {
		panic(fmt.Sprintf("Page %q references a template %q that does not exist", p.title, p.template))
	}
	h.patternToPage[p.pattern] = p
}

func (h *pageHandler)  ServeHTTP(w http.ResponseWriter, r *http.Request) {
	page, ok := h.patternToPage[r.URL.Path]
	if !ok {
		notFoundHandler(w, r)
		return
	}
	b, _ := htmlFiles.ReadFile("html/" + page.template)
	writeHtmlPage(w, page.title, template.HTML(b))
}

func dataDownloadHandler(w http.ResponseWriter, r *http.Request) {
	match := dataDownloadPathRegex.FindStringSubmatch(r.URL.Path)
	if match == nil {
		w.WriteHeader(http.StatusNotFound)
		writeHtmlPage(w, "Error", "File must be in the form")
		return
	}
	location, err := time.LoadLocation("EST")
	if err != nil {
		fmt.Println(err)
	}
	day := time.Date(atoi(match[2]), time.Month(atoi(match[3])), atoi(match[4]), 0, 0, 0, 0, location)
	// TODO: verify the day by re-stringifying it
	fmt.Println(match)
	fmt.Println(day)
	fmt.Fprintf(w, r.URL.Path)
}

func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	writeHtmlPage(w, "404: Page not found", "<h2>404: Page not found</h2>")
}

func writeHtmlPage(w io.Writer, title string, body template.HTML) {
	t := template.New("")
	b, _ := htmlFiles.ReadFile("html/page_template.html")
	t.Parse(string(b))
	input := struct {
		Title string
		Body template.HTML
	}{title, body}
	t.Execute(w, input)
}

func atoi(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}
