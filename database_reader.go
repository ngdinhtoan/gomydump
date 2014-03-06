package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"log"
	"os"
)

const (
	TablesChunk = 50
)

type DatabaseReader struct {
	host     string
	port     int
	socket   string
	database string
	username string
	password string
	db       *sql.DB
}

// query function will create a statement and execute the statement and return result.
func Query(db *sql.DB, queryString string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := db.Prepare(queryString)
	if err != nil {
		log.Fatalf("Error when prepare query:\n %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)

	return rows, err
}

func (dbReader *DatabaseReader) init() {
	db, err := dbReader.getConnection()
	if err != nil {
		log.Fatalf("Couldn't connect to database\n %v", err)
	}

	dbReader.db = db
}

func (dbReader *DatabaseReader) getConnectString() string {
	host := fmt.Sprintf("tcp(%s:%d)", dbReader.host, dbReader.port)
	if dbReader.socket != "" {
		host = fmt.Sprintf("unix(%s)", dbReader.socket)
	}

	auth := dbReader.username
	if dbReader.password != "" {
		auth = fmt.Sprintf("%s:%s", dbReader.username, dbReader.password)
	}

	dsn := fmt.Sprintf("%s@%s", auth, host)
	if dbReader.database != "" {
		dsn = fmt.Sprintf("%s/%s", dsn, dbReader.database)
	}

	return dsn
}

func (dbReader *DatabaseReader) getConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", dbReader.getConnectString())
	if err != nil {
		return db, err
	}

	err = db.Ping()
	return db, err
}

func (dbReader *DatabaseReader) getTables() {
	rows, err := Query(dbReader.db, "show full tables where Table_type != 'VIEW'")
	if err != nil {
		log.Fatalf("Error when query:\n%v", err)
	}
	defer rows.Close()

	num := 0
	fileFinishChan := make(chan string, 0)
	file, err := os.Create(fmt.Sprintf("%s/%s.sql", *exportDir, dbReader.database))
	for rows.Next() {
		var tableName string
		var tableType string
		err := rows.Scan(&tableName, &tableType)
		if err != nil {
			log.Fatalf("Read data error:\n%v", err)
		}

		if num == 0 {
			log.Printf("Start a chunk with %d tables", TablesChunk)
		}
		num++

		// use separated connection for each table
		db, err := dbReader.getConnection()
		if err != nil {
			log.Fatalf("Error when connect to database: %v", err)
		}
		table := NewTableExport(db, dbReader.database, tableName, fileFinishChan)

		go table.process()

		if num == TablesChunk {
			// wait for all table in chunk finish before beginning with another chunk
			for i := 0; i < TablesChunk; i++ {
				fileFinish := <-fileFinishChan
				// copy file from each table to one file
				// log.Printf("Finish %s", fileFinish)
				srcFile, err := os.Open(fileFinish)
				handleError(err, "Could not open file")
				io.Copy(file, srcFile)
				srcFile.Close()
				os.Remove(fileFinish)
			}
			log.Print("Chunk finish!")
			num = 0
		}
	}

	if num != 0 {
		for i := 0; i < num; i++ {
			fileFinish := <-fileFinishChan
			// copy file from each table to one file
			// log.Printf("Finish %s", fileFinish)
			srcFile, err := os.Open(fileFinish)
			handleError(err, "Could not open file")
			io.Copy(file, srcFile)
			srcFile.Close()
			os.Remove(fileFinish)
		}
		log.Print("Chunk finish!")
	}

	err = file.Close()
	if err != nil {
		os.Remove(file.Name())
	}
	log.Print("Done!")
	close(fileFinishChan)
}
