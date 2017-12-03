// +build !plan9

package cache

import(
	"github.com/ncw/rclone/fs"
	"net/http"
	"net/url"
	"fmt"
	"encoding/json"
	"strings"
	"github.com/pkg/errors"
)

const (
	// defPlexLoginUrl is the default URL for Plex login
	defPlexLoginUrl = "https://plex.tv/users/sign_in.json"
)

// plexConnector is managing the cache integration with Plex
type plexConnector struct {
	url	*url.URL
	token string
	f		*Fs
}

// newPlexConnector connects to a Plex server and generates a token
func newPlexConnector(f *Fs, plexUrl, username, password string) (*plexConnector, error) {
	u, err := url.ParseRequestURI(strings.TrimRight(plexUrl, "/"))
	if err != nil {
		return nil, err
	}

	pc := &plexConnector{
		f: f,
		url: u,
		token: "",
	}

	err = pc.authenticate(username, password)
	if err != nil {
		return nil, err
	}

	return pc, nil
}

// newPlexConnector connects to a Plex server and generates a token
func newPlexConnectorWithToken(f *Fs, plexUrl, token string) (*plexConnector, error) {
	u, err := url.ParseRequestURI(strings.TrimRight(plexUrl, "/"))
	if err != nil {
		return nil, err
	}

	pc := &plexConnector{
		f: f,
		url: u,
		token: token,
	}

	return pc, nil
}

func (p *plexConnector) fillDefaultHeaders(req *http.Request) {
	req.Header.Add("X-Plex-Client-Identifier", fmt.Sprintf("rclone (%v)", p.f.String()))
	req.Header.Add("X-Plex-Product", fmt.Sprintf("rclone (%v)", p.f.Name()))
	req.Header.Add("X-Plex-Version", fs.Version)
	req.Header.Add("Accept", "application/json")
	if p.token != "" {
		req.Header.Add("X-Plex-Token", p.token)
	}
}

// authenticate will generate a token based on a username/password
func (p *plexConnector) authenticate(username, password string) error {
	form := url.Values{}
	form.Set("user[login]", username)
	form.Add("user[password]", password)
	req, err := http.NewRequest("POST", defPlexLoginUrl, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	p.fillDefaultHeaders(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	var data map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&data)
	token, ok := get(data, "user", "authToken").(string)
	if !ok {
		return errors.New("failed to parse token")
	}
	p.token = token

	return nil
}

func (p *plexConnector) isConnected() bool {
	return p.token != ""
}

func (p *plexConnector) isPlaying(co *Object) bool {
	isPlaying := false
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/status/sessions", p.url.String()), nil)
	if err != nil {
		return false
	}
	p.fillDefaultHeaders(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	var data map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&data)
	size, ok := get(data, "MediaContainer", "size").(float64)
	if !ok || size < float64(1) {
		fs.Errorf("plex", "empty container: %v", data)
		return false
	}
	videos, ok := get(data, "MediaContainer", "Video").([]interface{})
	if !ok || len(videos) < 1 {
		fs.Errorf("plex", "empty videos: %v", data)
		return false
	}
	fs.Errorf("plex", "got videos: %v", videos)
	for _, v := range videos {
		key, ok := get(v, "key").(string)
		if !ok {
			fs.Errorf("failed to understand: %v", key)
			continue
		}
		req, err := http.NewRequest("GET", fmt.Sprintf("%s%s", p.url.String(), key), nil)
		if err != nil {
			return false
		}
		p.fillDefaultHeaders(req)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		var data map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&data)

		fp, ok := get(data, "MediaContainer", "Metadata", 0, "Media", 0, "Part", 0, "file").(string)
		if !ok {
			fs.Errorf("failed to understand: %v", fp)
			continue
		}
		fs.Errorf("plex", "searching %v in %v", co.Remote(), fp)
		if strings.Contains(fp, co.Remote()) {
			isPlaying = true
			break
		}
	}

	return isPlaying
}

// credit: https://stackoverflow.com/a/28878037
func get(m interface{}, path ...interface{}) interface{} {
	for _, p := range path {
		switch idx := p.(type) {
		case string:
			m = m.(map[string]interface{})[idx]
		case int:
			m = m.([]interface{})[idx]
		}
	}
	return m
}
