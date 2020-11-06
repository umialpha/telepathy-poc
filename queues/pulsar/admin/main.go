package main

import (
	"fmt"

	"encoding/json"
	"net/http"
)

func main() {
	resp, _ := http.Get("http://40.119.250.46/admin/v2/persistent/public/default/my-topic/stats")
	p := map[string]interface{}{}
	json.NewDecoder(resp.Body).Decode(&p)
	resp.Body.Close()
	fmt.Println(p)

	// res, err := http.Get("http://www.google.com/robots.txt")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// robots, err := ioutil.ReadAll(res.Body)
	// res.Body.Close()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("%s", robots)
}
