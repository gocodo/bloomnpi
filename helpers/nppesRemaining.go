package helpers

import (
	"database/sql"
)

func NppesUnprocessed (db *sql.DB, files []string) ([]string, error) {
	processed := []string{}
	unprocessed := []string{}

	rows, err := db.Query(`SELECT version FROM sources
JOIN source_versions
ON source_versions.source_id = sources.id
WHERE name = 'usgov.hhs.npi'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var version string
		if err = rows.Scan(&version); err != nil {
			return nil, err
		}
		processed = append(processed, version)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	for _, file := range files {
		found := false
		for _, prevFile := range processed {
			if prevFile == file {
				found = true
				break
			}
		}
		if !found {
			unprocessed = append(unprocessed, file)
		}
	}

	return unprocessed, nil;
}

func NppesMarkProcessed (db *sql.DB, file string) (error) {
	_, err := db.Exec("INSERT INTO source_versions (source_id, version) VALUES ((SELECT id FROM sources WHERE name = 'usgov.hhs.npi'), ($1))", file)
	if err != nil {
		return err
	}

	return nil
}