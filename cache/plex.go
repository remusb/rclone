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
	"time"
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

// fillDefaultHeaders will add common headers to requests
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
	tokenGen, ok := get(data, "user", "authToken")
	if !ok {
		return errors.New("failed to parse token")
	}
	token, ok := tokenGen.(string)
	if !ok {
		return errors.New("failed to parse token")
	}
	p.token = token

	return nil
}

// isConnected checks if this Plex
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
	sizeGen, ok := get(data, "MediaContainer", "size")
	if !ok {
		fs.Errorf("plex", "empty container: %v", data)
		return false
	}
	size, ok := sizeGen.(float64)
	if !ok || size < float64(1) {
		fs.Errorf("plex", "empty container: %v", data)
		return false
	}
	videosGen, ok := get(data, "MediaContainer", "Video")
	if !ok {
		fs.Errorf("plex", "empty videos: %v", data)
		return false
	}
	videos, ok := videosGen.([]interface{})
	if !ok || len(videos) < 1 {
		fs.Errorf("plex", "empty videos: %v", data)
		return false
	}
	for _, v := range videos {
		keyGen, ok := get(v, "key")
		if !ok {
			fs.Errorf("plex", "failed to find: key")
			continue
		}
		key, ok := keyGen.(string)
		if !ok {
			fs.Errorf("plex", "failed to understand: key")
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

		remote := co.Remote()
		if cr, yes := co.CacheFs.isWrappedByCrypt(); yes {
			remote, err = cr.DecryptFileName(co.Remote())
			if err != nil {
				fs.Errorf("plex", "can not decrypt wrapped file: %v", err)
				continue
			}
		}
		fpGen, ok := get(data, "MediaContainer", "Metadata", 0, "Media", 0, "Part", 0, "file")
		if !ok {
			fs.Errorf("plex", "failed to understand: %v", data)
			continue
		}
		fp, ok := fpGen.(string)
		if !ok {
			fs.Errorf("plex", "failed to understand: %v", fp)
			continue
		}
		fs.Errorf("plex", "searching %v in %v", remote, fp)
		if strings.Contains(fp, remote) {
			isPlaying = true
			break
		}
	}

	return isPlaying
}

func (p *plexConnector) isPlayingAsync(co *Object, response chan bool) {
	time.Sleep(time.Second * 3) // FIXME random guess here
	res := p.isPlaying(co)
	response <- res
}

// credit: https://stackoverflow.com/a/28878037
func get(m interface{}, path ...interface{}) (interface{}, bool) {
	for _, p := range path {
		switch idx := p.(type) {
		case string:
			if mm, ok := m.(map[string]interface{}); ok {
				if val, found := mm[idx]; found {
					m = val
					continue
				}
			}
			return nil, false
		case int:
			if mm, ok := m.([]interface{}); ok {
				if len(mm) > idx {
					m = mm[idx]
					continue
				}
			}
			return nil, false
		}
	}
	return m, true
}
