package main

import (
	"os"
	"fmt"
	"github.com/gocodo/bloomnpi/npi"
	"github.com/spf13/viper"
)

func showUsage() {
	fmt.Println("Usage: bloomapi-npi <command>")
	fmt.Println("=============================\n")
	fmt.Println("Avaialable commands:")
	fmt.Println("bloomapi-npi bootstrap    # setup NPI datasource in BloomAPI")
	fmt.Println("bloomapi-npi fetch        # fetch latest NPI data and add to BloomAPI")
	fmt.Println("bloomapi-npi drop         # remove all NPI tables")
	fmt.Println("bloomapi-npi search-index # index NPI in elasticsearch")
}

func main() {
	if (len(os.Args) != 2) {
		fmt.Println("Invalid command usage\n")
		showUsage()
		os.Exit(1)
	}

	arg := os.Args[1]

	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath("./")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	switch arg {
	case "bootstrap":
		npi.Bootstrap()
	case "fetch":
		npi.Fetch()
	case "drop":
		npi.Drop()
	case "search-index":
		npi.SearchIndex()
	default:
		fmt.Println("Invalid command:", arg)
		showUsage()
		os.Exit(1)
	}
}