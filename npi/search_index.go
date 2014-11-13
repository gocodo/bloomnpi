package npi

import (
	"log"
	"fmt"
	"time"
	"text/template"
	"bytes"
	"database/sql"
	"encoding/json"
	"github.com/gocodo/bloomdb"
	elastigo "github.com/mattbaird/elastigo/lib"
)

func updateLastSeen(db *sql.DB) (error) {
	var deactivationDate time.Time
	err := db.QueryRow("SELECT deactivation_date FROM npis WHERE deactivation_date IS NOT NULL ORDER BY deactivation_date desc LIMIT 1").Scan(&deactivationDate)
	if err != nil {
		return err
	}

	var lastUpdated time.Time
	err = db.QueryRow("SELECT last_update_date FROM npis WHERE last_update_date IS NOT NULL ORDER BY last_update_date desc LIMIT 1").Scan(&lastUpdated)
	if err != nil {
		return err
	}

	var lastSeen time.Time
	if lastUpdated.After(deactivationDate) {
		lastSeen = lastUpdated
	} else {
		lastSeen = deactivationDate
	}
	_, err = db.Exec("UPDATE npi_indexed SET indexed_through = '" + lastSeen.Format("2006-01-02") + "'")
	if err != nil {
		return err
	}

	return nil
}

func loadJsonQuery(db *sql.DB) (string, error) {
	var lastSeen time.Time

	err := db.QueryRow("SELECT indexed_through FROM npi_indexed").Scan(&lastSeen)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)

	t, err := template.New("elasticsearch.sql.template").ParseFiles("sql/elasticsearch.sql.template")
	if err != nil {
		return "", err
	}

	err = t.Execute(buf, struct { QueryAfter string }{lastSeen.Format("2006-01-02")})
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func deNull(doc map[string]interface{}) {
	for k, v := range doc {
		if v == nil {
			delete(doc, k)
		} else {
			switch v.(type) {
			case map[string]interface{}:
				deNull(v.(map[string]interface{}))
			case []interface{}:
				for _, elm := range v.([]interface{}) {
					deNull(elm.(map[string]interface{}))
				}
			}
		}
	}
}

func removeNulls(doc string) (string, error) {
	var dat map[string]interface{}
	err := json.Unmarshal([]byte(doc), &dat)
	if err != nil {
		return "", err
	}
	deNull(dat)
	result, err := json.Marshal(dat)
	if err != nil {
		return "", err
	}

	return string(result), nil
}

func SearchIndex() {
	startTime := time.Now()

	bdb := bloomdb.CreateDB()

	conn, err := bdb.SqlConnection()
	if err != nil {
		log.Fatal("Failed to get database connection.", err)
	}
	defer conn.Close()

	sqlQuery, err := loadJsonQuery(conn)
	if err != nil {
		log.Fatal(err)
	}

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

		doc, err = removeNulls(doc)
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

	err = updateLastSeen(conn)
	if err != nil {
		log.Fatal(err)
	}
}