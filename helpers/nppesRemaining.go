package helpers

import (
	"database/sql"
	"fmt"
)

func NppesUnprocessed (db *sql.DB, files []string) ([]string, error) {
	var processed = []string{}

	for _, file := range files {
		var processedFile string
		err := db.QueryRow("SELECT file FROM npi_files WHERE file = '" + file + "'").Scan(&processedFile)
		switch {
		case err == sql.ErrNoRows:
			processed = append(processed, file)
		case err != nil:
			return nil, err
		}
	}

	fmt.Println(processed)

	return processed, nil;
}

func NppesMarkProcessed (db *sql.DB, file string) (error) {
	_, err := db.Exec("INSERT INTO npi_files (file) VALUES ('" + file + "')")
	if err != nil {
		return err
	}

	return nil
}