package queueclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

func jsonHeaders() http.Header {
	headers := http.Header{}
	headers.Add("Content-Type", "application/json")
	headers.Add("Accept", "application/json")
	return headers
}

func httpGet(requestPath string, out interface{}) error {
	return httpDo(http.MethodGet, requestPath, nil, out)
}

func httpDo(method string, requestPath string, in interface{}, out interface{}) error {
	client := http.DefaultClient
	endpoint := requestPath
	var body io.Reader
	inBytes, err := json.Marshal(in)
	if err != nil {
		return err
	}
	body = bytes.NewReader(inBytes)
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return err
	}

	req.Header = jsonHeaders()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode > 299 || resp.StatusCode < 200 {
		return fmt.Errorf("http error status code: %d", resp.StatusCode)
	}

	if out != nil {
		outBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return json.Unmarshal(outBytes, out)
	}

	return nil
}
