package npi

import (
	"sort"
	"fmt"
	"io/ioutil"
	"os"
	"github.com/untoldone/bloomdb"
	"github.com/untoldone/bloomnpi/helpers"
)

func Fetch() {
	monthly, weekly, err := helpers.FilesAvailable()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	bdb := bloomdb.CreateDB()
	db, err := bdb.SqlConnection()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	monthlyTodos, err := helpers.NppesUnprocessed(db, []string{monthly})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	weeklyTodos, err := helpers.NppesUnprocessed(db, weekly)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if len(monthlyTodos) == 1 {
		Drop()
		Bootstrap()

		err := helpers.Download(monthlyTodos[0])
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		reader, err := helpers.OpenReader("data/" + monthlyTodos[0] + ".zip")
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer reader.Close()

		Upsert(reader)

		file, err := ioutil.ReadFile("sql/index.sql")
		if err != nil {
			fmt.Println("Failed to read file.", err)
			return
		}

		_, err = db.Exec(string(file[:]))
		if err != nil {
			fmt.Println("Failed to read file.", err)
			return
		}
	}

	sort.Strings(weeklyTodos)

	for _, weeklyTodo := range weeklyTodos {
		err := helpers.Download(weeklyTodo)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		reader, err := helpers.OpenReader("data/" + weeklyTodo + ".zip")
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer reader.Close()

		Upsert(reader)
	}

	doneTodos := append(monthlyTodos, weeklyTodos...)
	for _, doneTodo := range doneTodos {
		_, err := db.Exec("INSERT INTO npi_files (file) VALUES ('" + doneTodo + "')")
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		os.Remove("data/" + doneTodo + ".zip")
	}
}