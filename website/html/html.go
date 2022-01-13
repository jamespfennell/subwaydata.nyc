package html

import (
	"embed"
	"fmt"
	"html/template"
	"reflect"
)

//go:embed *.html
var files embed.FS

const rootTemplate = "layout.html"

type Templates struct {
	Home               *template.Template `html:"home.html"`
	ExploreTheData     *template.Template `html:"explore-the-data.html"`
	ProgrammaticAccess *template.Template `html:"programmatic-access.html"`
	HowItWorks         *template.Template `html:"how-it-works.html"`
	DataSchema         *template.Template `html:"data-schema.html"`
	PageNotFound       *template.Template `html:"404.html"`
}

var t Templates

func GetTemplates() Templates {
	return t
}

func init() {
	rootB, err := files.ReadFile(rootTemplate)
	if err != nil {
		panic(fmt.Sprintf("Could not read the root template %s", rootTemplate))
	}
	str := reflect.TypeOf(t)
	for i := 0; i < str.NumField(); i++ {
		field := str.Field(i)
		path := field.Tag.Get("html")
		if path == "" {
			panic(fmt.Sprintf("Templates.%s does not have a path specified", field.Name))
		}
		b, err := files.ReadFile(path)
		if err != nil {
			panic(fmt.Sprintf("Templates.%s references a path %s that does not exist", field.Name, path))
		}
		tmpl := template.New(field.Name)
		tmpl = template.Must(tmpl.Parse(string(rootB)))
		tmpl = template.Must(tmpl.Parse(string(b)))
		reflect.ValueOf(&t).Elem().Field(i).Set(reflect.ValueOf(tmpl))
	}
}
