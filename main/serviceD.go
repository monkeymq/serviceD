package main

import (
	"fmt"
	"os"
	"serviceD"
)

func main() {

	opts := serviceD.NewOptions()
	err := serviceD.New(opts).Open()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(0)
	}

	select {}
}
