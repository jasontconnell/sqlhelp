package rs

import (
    "database/sql"
)

func GetResultsChannel(db *sql.DB, query string) (chan map[string]interface{}, chan error) {
    results := make(chan map[string]interface{})
    errchan := make(chan error)

    go func() {
        rows, eerr := db.Query(query)
        if eerr != nil {
            errchan <- eerr
            close(errchan)
            return
        }

        cols, cerr := rows.Columns()
        if cerr != nil {
            errchan <- cerr
            return
        }

        vals := make([]interface{}, len(cols))
        for i := 0; i < len(cols); i++ {
            vals[i] = new(interface{})
        }

        for rows.Next() {
            scerr := rows.Scan(vals...)

            if scerr != nil {
                errchan <- scerr
                close(errchan)
                return
            }

            valmap := make(map[string]interface{})
            for i, col := range cols {
                valmap[col] = *(vals[i].(*interface{}))
            }

            results <- valmap

        }
    }()

    return results, errchan
}

func GetResultSet(db *sql.DB, query string) ([]map[string]interface{}, error) {
    ret := []map[string]interface{}{}

    rows, eerr := db.Query(query)
    if eerr != nil {
        return nil, eerr
    }
   
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
