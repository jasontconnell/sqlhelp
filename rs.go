package sqlhelp

import (
	"database/sql"
	//"fmt"
)

func GetResultsChannel(db *sql.DB, query string, results chan map[string]interface{}) error {
	rows, eerr := db.Query(query)
	if eerr != nil {
		return eerr
	}

	cols, cerr := rows.Columns()
	if cerr != nil {
		return cerr
	}

	vals := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		vals[i] = new(interface{})
	}

	go func() {
		defer rows.Close()
		defer close(results)
		i := 0
		for rows.Next() {
			scerr := rows.Scan(vals...)

			if scerr != nil {
				panic(scerr)
			}

			valmap := make(map[string]interface{})
			for i, col := range cols {
				valmap[col] = *(vals[i].(*interface{}))
			}
			results <- valmap
			i++
		}
	}()

	return nil
}

func GetResultsChannelExp(db *sql.DB, query string, results chan map[string]interface{}) error {
	rows, eerr := db.Query(query)
	if eerr != nil {
		return eerr
	}

	cols, cerr := rows.Columns()
	if cerr != nil {
		return cerr
	}

	vals := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		vals[i] = new(interface{})
	}

	go func() {
		defer rows.Close()
		i := 0
		for rows.Next() {
			scerr := rows.Scan(vals...)

			if scerr != nil {
				panic(scerr)
			}

			valmap := make(map[string]interface{})
			for i, col := range cols {
				valmap[col] = *(vals[i].(*interface{}))
			}
			results <- valmap
			i++
		}
	}()

	return nil
}

func GetResultSet(db *sql.DB, query string) ([]map[string]interface{}, error) {
	ret := []map[string]interface{}{}

	rows, eerr := db.Query(query)
	if eerr != nil {
		return nil, eerr
	}

	defer rows.Close()

	cols, cerr := rows.Columns()
	if cerr != nil {
		return nil, cerr
	}

	vals := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		vals[i] = new(interface{})
	}

	for rows.Next() {
		scerr := rows.Scan(vals...)

		if scerr != nil {
			return nil, scerr
		}

		valmap := make(map[string]interface{})
		for i, col := range cols {
			valmap[col] = *(vals[i].(*interface{}))
		}

		ret = append(ret, valmap)
	}

	return ret, nil
}
