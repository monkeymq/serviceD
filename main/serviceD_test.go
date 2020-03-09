package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
)

var leader = "http://10.140.203.25:16000/"

func BenchmarkServiceDWrite(b *testing.B) {

	for n :=0; n<b.N; n++ {
		url := leader + "set?key=test&value=" + strconv.Itoa(n)
		resp, err := http.Get(url)
		defer resp.Body.Close()

		if err != nil {
			fmt.Fprint(os.Stderr, err)
		}
	}

}

func BenchmarkServiceDGet(b *testing.B) {
	for n :=0; n<b.N; n++ {
		url := leader + "get?key=test"
		resp, err := http.Get(url)
		defer resp.Body.Close()

		if err != nil {
			fmt.Fprint(os.Stderr, err)
		}
	}
}
