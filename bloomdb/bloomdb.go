package bloomdb

import (
	"database/sql"
	"github.com/spf13/viper"
)

type BloomDatabase struct {
	sqlConnStr string
}

func (bdb *BloomDatabase) SqlConnection() (*sql.DB, error) {
	db, err := sql.Open("postgres", "postgres://localhost/bloomapi-npi?sslmode=disable")
	if err != nil {
		return nil, err
	}

	return db, nil
}

func CreateDB () *BloomDatabase {
	return &BloomDatabase {
		viper.GetString("sqlConnStr"),
	}
}