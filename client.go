package schemareg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	postContentType = "application/vnd.schemaregistry.v1+json"
)

type Config struct {
	Host string
}

type Interface interface {
	Config() *Config
	SchemaById(int) (string, error)
	RegisterSubjectVersion(string, string) (int, error)
	SchemaIsCompatibleWithSubjectVersion(string, string, string) (bool, error)
	Subjects() ([]string, error)
}

type client struct {
	config *Config
	httpc  *http.Client
}

type ResponseCodeError struct {
	code int
}

func (e ResponseCodeError) Error() string {
	return fmt.Sprintf("Bad Response Code: %d", e.code)
}

func NewClient(c *Config, httpc *http.Client) Interface {
	if httpc == nil {
		httpc = &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   5,
				ResponseHeaderTimeout: 2 * time.Second,
			},

			Timeout: 5 * time.Second,
		}
	}
	return client{c, httpc}
}

func extractBodyMap(r *http.Response) (respContents map[string]interface{}, err error) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &respContents)
	return
}

func extractBodyList(r *http.Response) (respContents []string, err error) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &respContents)
	return
}

func (c client) Config() *Config {
	return c.config
}

func (c client) SchemaById(id int) (string, error) {
	resp, err := c.httpc.Get(c.url(fmt.Sprintf("/schemas/ids/%d", id)))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", ResponseCodeError{resp.StatusCode}
	}
	respContents, err := extractBodyMap(resp)
	if err != nil {
		return "", err
	}
	schema := respContents["schema"].(string)
	return schema, nil
}

func (c client) RegisterSubjectVersion(subject, schema string) (int, error) {
	reqData := map[string]string{
		"schema": schema,
	}
	b, err := json.Marshal(reqData)
	if err != nil {
		return 0, err
	}
	resp, err := c.httpc.Post(c.url(fmt.Sprintf("/subjects/%s/versions", subject)),
		postContentType, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, ResponseCodeError{resp.StatusCode}
	}
	respContents, err := extractBodyMap(resp)
	if err != nil {
		return 0, err
	}
	id := respContents["id"].(float64)
	return int(id), nil
}

func (c client) Subjects() ([]string, error) {
	resp, err := c.httpc.Get(c.url("/subjects"))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, ResponseCodeError{resp.StatusCode}
	}
	return extractBodyList(resp)
}

func (c client) SchemaIsCompatibleWithSubjectVersion(subject, schema, version string) (bool, error) {
	reqData := map[string]string{
		"schema": schema,
	}
	b, err := json.Marshal(reqData)
	if err != nil {
		return false, err
	}
	resp, err := c.httpc.Post(c.url(fmt.Sprintf("/compatibility/subjects/%s/versions/%s", subject, version)),
		postContentType, bytes.NewReader(b))
	if err != nil {
		return false, err
	}
	if resp.StatusCode != 200 {
		return false, ResponseCodeError{resp.StatusCode}
	}
	respContents, err := extractBodyMap(resp)
	if err != nil {
		return false, err
	}
	isCompatible := respContents["is_compatible"].(bool)
	return isCompatible, nil
}

func (c client) url(path string) string {
	return fmt.Sprintf("%s%s", c.config.Host, path)
}
