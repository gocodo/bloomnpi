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
)

func buildIndexQuery(createdAt time.Time) (string, error) {
	buf := new(bytes.Buffer)

	t, err := template.New("elasticsearch.sql.template").ParseFiles("sql/elasticsearch.sql.template")
	if err != nil {
		return "", err
	}

	tCreatedAt := createdAt.Format(time.RFC3339)

	err = t.Execute(buf, struct { CreatedAt string }{tCreatedAt})
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func buildDeleteQuery(createdAt time.Time) (string, error) {
	tCreatedAt := createdAt.Format(time.RFC3339)
	return "SELECT usgov_hhs_npis_revisions.id FROM usgov_hhs_npis_revisions WHERE bloom_action = 'DELETE' AND bloom_updated_at > '" + tCreatedAt + "';", nil
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
	startTime := time.Now().UTC()

	bdb := bloomdb.CreateDB()
	conn, err := bdb.SqlConnection()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	var lastUpdated time.Time
	err = conn.QueryRow("SELECT last_updated FROM search_types WHERE name = 'usgov.hhs.npi'").Scan(&lastUpdated)
	if err == sql.ErrNoRows {
		lastUpdated = time.Unix(0, 0)
		_, err := conn.Exec("INSERT INTO search_types (name, last_updated, last_checked) VALUES ('usgov.hhs.npi', $1, $1)", lastUpdated)
		if err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}

	c := bdb.SearchConnection()

	indexer := c.NewBulkIndexerErrors(10, 60)
	indexer.BulkMaxBuffer = 10485760
	indexer.Start()

	indexCount := 0
	deleteCount := 0

	query, err := buildDeleteQuery(lastUpdated)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := conn.Query(query)
	if err != nil {
		log.Fatal("Failed to query for rows.", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}

		deleteCount += 1
		if deleteCount % 10000 == 0 {
			fmt.Println(deleteCount, "Records Deleted in", time.Now().Sub(startTime))
		}

		indexer.Delete("source", "usgov.hhs.npi", id)
	}

	indexer.Flush()
	fmt.Println(deleteCount, "Records Deleted in", time.Now().Sub(startTime))

	query, err = buildIndexQuery(lastUpdated)
	if err != nil {
		log.Fatal(err)
	}

	insertRows, err := conn.Query(query)
	if err != nil {
		fmt.Println("Error with query:", query)
		log.Fatal(err)
	}
	defer insertRows.Close()

	for insertRows.Next() {
		var doc, id string
		err := insertRows.Scan(&doc, &id)
		if err != nil {
			log.Fatal(err)
		}

		doc, err = removeNulls(doc)
		if err != nil {
			log.Fatal(err)
		}

		indexCount += 1
		if indexCount % 10000 == 0 {
			fmt.Println(indexCount, "Records Indexed in", time.Now().Sub(startTime))
		}

		indexer.Index("source", "usgov.hhs.npi", id, "", "", nil, doc)
	}

	indexer.Flush()
	// There seems to be a bug in elastigo ... unsure why this sometimes fails
	// Should be fixed at some point ...
	//indexer.Stop()
	fmt.Println(indexCount, "Records Indexed in", time.Now().Sub(startTime))

	if indexCount > 0 || deleteCount > 0 {
		_, err = conn.Exec("UPDATE search_types SET last_updated = $1, last_checked = $1 WHERE name = 'usgov.hhs.npi'", startTime)
	} else {
		_, err = conn.Exec("UPDATE search_types SET last_checked = $1 WHERE name = 'usgov.hhs.npi'", startTime)
	}

	if err != nil {
		log.Fatal(err)
	}
}
