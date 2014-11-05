package bloomdb

import (
	"database/sql"
	"github.com/lib/pq"
)

func Insert(db *sql.DB, table string, columns []string, rows chan []string) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(table, columns...))
	if err != nil {
		return err
	}

	for rawRow := range rows {
		row := make([]interface{}, len(rawRow))
		for i, column := range rawRow {
			if column == "" {
				row[i] = nil
			} else {
				row[i] = column
			}
		}

		_, err = stmt.Exec(row...)
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}