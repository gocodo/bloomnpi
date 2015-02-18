package npi

import (
	"sort"
	"fmt"
	"os"
	"github.com/gocodo/bloomdb"
	"github.com/gocodo/bloomnpi/helpers"
)

func Fetch() {
	bdb := bloomdb.CreateDB()
	db, err := bdb.SqlConnection()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	monthly, weekly, err := helpers.FilesAvailable()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

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

		monthlyKey := bloomdb.MakeKey(monthlyTodos[0])

		Upsert(reader, monthlyKey, true)
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

		weeklyKey := bloomdb.MakeKey(weeklyTodo)

		Upsert(reader, weeklyKey, false)
	}

	doneTodos := append(monthlyTodos, weeklyTodos...)
	for _, doneTodo := range doneTodos {
		err = helpers.NppesMarkProcessed(db, doneTodo)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		os.Remove("data/" + doneTodo + ".zip")
	}
}