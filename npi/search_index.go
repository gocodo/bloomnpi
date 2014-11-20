package npi

import (
	"log"
	"fmt"
	"time"
	"sort"
	"regexp"
	"text/template"
	"bytes"
	"database/sql"
	"encoding/json"
	"github.com/gocodo/bloomdb"
)

var monthRegex = regexp.MustCompile("NPPES_Data_Dissemination_[a-zA-Z]+")

type npiFile struct {
	Id string
	File string
}

type byFile []npiFile

func (a byFile) Len() int {
	return len(a)
}

func (a byFile) Less(i, j int) bool {
	iMonth := monthRegex.MatchString(a[i].File)
	jMonth := monthRegex.MatchString(a[j].File)
	if iMonth {
		return true
	} else if jMonth {
		return false
	} else {
		return a[i].File < a[j].File
	}
}

func (a byFile) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func updateIndexed(db *sql.DB) (error) {
	_, err := db.Exec("Update npi_files SET indexed = true")
	if err != nil {
		return err
	}

	return nil
}

func loadJsonQuery(db *sql.DB) (string, error) {
	rows, err := db.Query("SELECT id, file FROM npi_files WHERE indexed != true")
	if err != nil {
		return "", err
	}

	var (
		id string
		file string
		query string
		files []npiFile
	)

	for rows.Next() {
		err := rows.Scan(&id, &file)
		if err != nil {
			return "", err
		}
		files = append(files, npiFile{id, file})
	}

	sort.Sort(byFile(files))

	for _, file := range files {
		buf := new(bytes.Buffer)

		t, err := template.New("elasticsearch.sql.template").ParseFiles("sql/elasticsearch.sql.template")
		if err != nil {
			return "", err
		}

		err = t.Execute(buf, struct { FileId string }{file.Id})
		if err != nil {
			return "", err
		}

		query = query + buf.String()
	}

	return query, nil
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

	if sqlQuery == "" {
		return
	}

	rows, err := conn.Query(sqlQuery)
	if err != nil {
		log.Fatal("Failed to query for rows.", err)
	}
	defer rows.Close()

	c := bdb.SearchConnection()

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

	err = updateIndexed(conn)
	if err != nil {
		log.Fatal(err)
	}
}