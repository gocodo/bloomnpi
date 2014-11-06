package main

import (
	"os"
	"fmt"
	"github.com/untoldone/bloomapi-npi/npi"
	"github.com/spf13/viper"
)

func showUsage() {
	fmt.Println("Usage: bloomapi-npi <command>")
	fmt.Println("=============================\n")
	fmt.Println("Avaialable commands:")
	fmt.Println("bloomapi-npi bootstrap  # setup NPI datasource in BloomAPI")
	fmt.Println("bloomapi-npi fetch      # fetch latest NPI data and add to BloomAPI")
	fmt.Println("bloomapi-npi drop       # remove all NPI tables")
}

func main() {
	if (len(os.Args) != 2) {
		fmt.Println("Invalid command usage\n")
		showUsage()
		os.Exit(1)
	}

	arg := os.Args[1]

	viper.SetConfigType("toml")

	switch arg {
	case "bootstrap":
		npi.Bootstrap()
	case "fetch":
		npi.Fetch()
	case "drop":
		npi.Drop()
	default:
		fmt.Println("Invalid command:", arg)
		showUsage()
		os.Exit(1)
	}
}