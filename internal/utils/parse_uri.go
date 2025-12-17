package utils

import (
	"net/url"
	"strings"
)

func ParseURI(uri string) (*url.URL, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "" && !strings.Contains(u.Host, ".") && u.Path != "" && !strings.Contains(u.Path, ":") {
		if u.Host == "" && u.Path != "" {
			parsedAsHost, err := url.Parse("http://" + uri)
			if err == nil {
				parsedAsHost.Scheme = ""
				return parsedAsHost, nil
			}
		}
	}
	return u, nil
}
