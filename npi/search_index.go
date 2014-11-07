package npi

import (
	"log"
	"fmt"
	"time"
	"io/ioutil"
	"github.com/untoldone/bloomapi-npi/bloomdb"
	elastigo "github.com/mattbaird/elastigo/lib"
)

func loadJsonQuery() (string, error) {
	file, err := ioutil.ReadFile("sql/demo.sql")
	if err != nil {
		return "", err
	}

	metaSql := string(file[:])

	return metaSql, nil
}

func SearchIndex() {
	sqlQuery, err := loadJsonQuery()
	if err != nil {
		log.Fatal(err)
	}

	startTime := time.Now()

	bdb := bloomdb.CreateDB()

	conn, err := bdb.SqlConnection()
	if err != nil {
		log.Fatal("Failed to get database connection.", err)
	}
	defer conn.Close()

	rows, err := conn.Query(sqlQuery)
	if err != nil {
		log.Fatal("Failed to query for rows.", err)
	}
	defer rows.Close()

	c := elastigo.NewConn()

	indexer := c.NewBulkIndexerErrors(10, 60)
	indexer.Start()

	count := 0

	for rows.Next() {
		var doc, id string
		err := rows.Scan(&doc, &id)
		if err != nil {
			log.Fatal(err)
		}

		count = count + 1
		if count % 10000 == 0 {
			fmt.Println(count, "Records Indexed in", time.Now().Sub(startTime))
		}
		
		indexer.Index("source", "npi", id, "", nil, doc, false)
	}

	indexer.Stop()
}