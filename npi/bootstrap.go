package npi

import (
	"log"
	"io/ioutil"
	"github.com/untoldone/bloomdb"
)

func Bootstrap () {
	bloomdb := bloomdb.CreateDB()

	file, err := ioutil.ReadFile("sql/create.sql")
	if err != nil {
		log.Fatal("Failed to read file.", err)
	}

	metaSql := string(file[:])
	conn, err := bloomdb.SqlConnection()
	if err != nil {
		log.Fatal("Failed to get database connection.", err)
	}
	defer conn.Close()

	_, err = conn.Exec(metaSql)
	if err != nil {
		log.Fatal("Failed to create metadata tables.", err)
	}
}