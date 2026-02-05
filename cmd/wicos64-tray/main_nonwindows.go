//go:build !windows

package main

import "fmt"

func main() {
	fmt.Println("wicos64-tray is a Windows-only tray controller.")
}
