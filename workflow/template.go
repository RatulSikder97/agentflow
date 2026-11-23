package workflow

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"text/template"
)

// Data is the value templates are rendered against:
//
//	.input          run input (map)
//	.steps.<id>     {"output": <value>, "status": "succeeded"} for finished steps
//	.item / .index  current element inside a map step
type Data map[string]any

var funcs = template.FuncMap{
	"json": func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	},
	"lower": strings.ToLower,
	"upper": strings.ToUpper,
	"trim":  strings.TrimSpace,
	"join": func(sep string, v any) (string, error) {
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Slice {
			return "", fmt.Errorf("join: want list, got %T", v)
		}
		parts := make([]string, rv.Len())
		for i := range parts {
			parts[i] = fmt.Sprint(rv.Index(i).Interface())
		}
		return strings.Join(parts, sep), nil
	},
	"contains": func(substr string, s any) bool { return strings.Contains(fmt.Sprint(s), substr) },
	"default": func(def, v any) any {
		if isEmpty(v) {
			return def
		}
		return v
	},
	"first": func(vs ...any) any {
		for _, v := range vs {
			if !isEmpty(v) {
				return v
			}
		}
		return nil
	},
}

func isEmpty(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String, reflect.Slice, reflect.Map:
		return rv.Len() == 0
	case reflect.Pointer, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}

var cache sync.Map // template text -> *template.Template

func compile(text string) (*template.Template, error) {
	if t, ok := cache.Load(text); ok {
		return t.(*template.Template), nil
	}
	t, err := template.New("step").Funcs(funcs).Option("missingkey=error").Parse(text)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	cache.Store(text, t)
	return t, nil
}

// Render executes a Go text/template against data.
func Render(text string, data Data) (string, error) {
	if !strings.Contains(text, "{{") {
		return text, nil
	}
	t, err := compile(text)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	if err := t.Execute(&b, map[string]any(data)); err != nil {
		return "", fmt.Errorf("render template: %w", err)
	}
	return b.String(), nil
}

// RenderValue renders every string found in v (recursing into maps and
// lists) and returns a new value with the same shape.
func RenderValue(v any, data Data) (any, error) {
	switch t := v.(type) {
	case string:
		return Render(t, data)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			r, err := RenderValue(e, data)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			r, err := RenderValue(e, data)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			out[i] = r
		}
		return out, nil
	default:
		return v, nil
	}
}

// Truthy renders a condition and interprets the result as a boolean.
func Truthy(text string, data Data) (bool, error) {
	s, err := Render(text, data)
	if err != nil {
		return false, err
	}
	s = strings.TrimSpace(s)
	if s == "" || s == "<no value>" {
		return false, nil
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, fmt.Errorf("condition %q rendered %q, want true/false", text, s)
	}
	return b, nil
}

// Lookup resolves a dotted path such as "steps.plan.output.topics" in data.
// Integer segments index into lists.
func Lookup(path string, data Data) (any, error) {
	var cur any = map[string]any(data)
	for _, seg := range strings.Split(strings.TrimPrefix(path, "."), ".") {
		switch c := cur.(type) {
		case map[string]any:
			v, ok := c[seg]
			if !ok {
				return nil, fmt.Errorf("lookup %q: key %q not found", path, seg)
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(c) {
				return nil, fmt.Errorf("lookup %q: bad index %q", path, seg)
			}
			cur = c[i]
		default:
			return nil, fmt.Errorf("lookup %q: cannot descend into %T at %q", path, cur, seg)
		}
	}
	return cur, nil
}
