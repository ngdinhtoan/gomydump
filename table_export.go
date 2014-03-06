package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"log"
	"os"
	"os/exec"
)

const (
	RowsChunk = 100
)

type TableExport struct {
	db             *sql.DB
	dbName         string
	tableName      string
	columns        map[string]string
	rowCount       int
	fileFinishChan chan string
	writer         *FileWriter
}

func NewTableExport(db *sql.DB, dbName string, tableName string, fileFinishChan chan string) *TableExport {
	tableExport := &TableExport{
		db:             db,
		dbName:         dbName,
		tableName:      tableName,
		fileFinishChan: fileFinishChan}

	return tableExport
}

func (table *TableExport) exportFileName() string {
	return *exportDir + table.tableName + ".sql"
}

func (table *TableExport) mysqldump() {
	args := []string{}
	if *socket != "" {
		args = append(args, fmt.Sprintf("--socket=%s", *socket))
	} else {
		args = append(args, fmt.Sprintf("--host=%s --port=%d", *host, *port))
	}
	if *username != "" {
		args = append(args, fmt.Sprintf("-u%s", *username))
	}
	if *password != "" {
		args = append(args, fmt.Sprintf("-p%s", *password))
	}
	args = append(args, table.dbName, table.tableName)

	// open the out file for writing
	outfile, err := os.Create(table.exportFileName())
	handleError(err, "Could not create file")
	defer outfile.Close()

	cmd := exec.Command("mysqldump", args...)
	cmd.Stdout = outfile

	stderr, err := cmd.StderrPipe()

	err = cmd.Start()
	handleError(err, "Error when starts dumping table via mysqldump")

	go io.Copy(os.Stderr, stderr)

	err = cmd.Wait()

	// export finish, close db connect to avoid 'too many connections' error
	table.fileFinishChan <- table.exportFileName()
}

func (table *TableExport) process() {
	if *verbose {
		log.Printf("Processing table %s...", table.tableName)
	}

	// use mysqldump command to export each table
	_, err := exec.LookPath("mysqldump")
	if err != nil && *mysqldump == true {
		log.Fatalln("Could not find 'mysqldump' command.")
	}

	if *mysqldump {
		// close db connection as don't use anymore to avoid too many connections error
		table.db.Close()
		table.mysqldump()
		return
	}

	table.writer = NewFileWriter(table.exportFileName())

	table.rowCount = table.getRowCount()
	table.initColumnList()

	go table.writer.start()

	tableComment := `----
-- Dump data of table ` + table.tableName + `
----`
	table.writer.write(tableComment)
	table.writer.writeNewLine()

	// drop table if exist
	table.writer.write(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", table.tableName))
	table.writer.writeNewLine()

	createTableString, err := table.getCreateTableQuery()
	handleError(err, "Could not get create table query")
	table.writer.write(createTableString + ";")
	table.writer.writeNewLine()

	// write insert statement

	table.writer.writeNewLine()
	table.writer.finish()
	// export finish, close db connect to avoid 'too many connections' error
	table.db.Close()
	table.fileFinishChan <- table.exportFileName()
}

func (table *TableExport) initColumnList() {
	query := fmt.Sprint("SELECT `COLUMN_NAME`, `DATA_TYPE` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? AND `TABLE_CATALOG` = 'def' ORDER BY `ORDINAL_POSITION`")
	rows, err := Query(table.db, query, table.dbName, table.tableName)
	if err != nil {
		log.Fatalf("Coun't get list of column of table '%s': %v", table.tableName, err)
	}
	defer rows.Close()

	table.columns = make(map[string]string)
	var colName, colType string
	for rows.Next() {
		err := rows.Scan(&colName, &colType)
		if err != nil {
			log.Fatalf("Coun't get list of column of table '%s': %v", table.tableName, err)
		}
		table.columns[colName] = colType
	}
}

func (table *TableExport) getNumberOfColumn() int {
	return len(table.columns)
}

func (table *TableExport) getCreateTableQuery() (string, error) {
	rows, err := Query(table.db, fmt.Sprintf("SHOW CREATE TABLE `%s`", table.tableName))
	if err != nil {
		log.Fatalf("Couldn't get create table query:\n %v", err)
	}
	defer rows.Close()

	var tableName, createTableQuery string
	if rows.Next() {
		err := rows.Scan(&tableName, &createTableQuery)
		return createTableQuery, err
	}

	return "", fmt.Errorf("Couldn't get create table query for table `%s`: %v", table.tableName, rows.Err())
}

func (table *TableExport) fetchData(offset int, dataOut chan [][]string) {
	//results := make([]string, 0, RowsChunk)

	query := fmt.Sprintf("SELECT * FROM `%s` OFFSET %d LIMIT %d", table.tableName, offset, RowsChunk)
	rows, err := Query(table.db, query)
	if err != nil {
		log.Fatalf("Couldn't fetch data: %v", err)
	}
	defer rows.Close()

	// travel on data and return data
}

func (table *TableExport) getRowCount() int {
	rows, err := Query(table.db, fmt.Sprintf("SELECT COUNT(1) FROM `%s`", table.tableName))
	if err != nil {
		log.Fatalf("Error when try to get number row of table '%s': %v", table.tableName, err)
	}
	defer rows.Close()

	var rowCount int
	if rows.Next() {
		err = rows.Scan(&rowCount)
		if err != nil {
			log.Fatalf("Error when try to get number row of table '%s': %v", table.tableName, err)
		}
		return rowCount
	}

	return 0
}
