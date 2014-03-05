package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
)

var username = flag.String("username", "root", "MySQL username.")
var password = flag.String("password", "", "MySQL password.")
var host = flag.String("host", "127.0.0.1", "MySQL host.")
var port = flag.Int("port", 3306, "MySQL port.")
var socket = flag.String("socket", "", "MySQL socket file.")
var database = flag.String("database", "", "MySQL database which will be dumped.")
var verbose = flag.Bool("verbose", false, "Show detail log")
var exportDir = flag.String("export-dir", "/tmp/", "Export directory which will hold exported file.")
var mysqldump = flag.Bool("use-mysqldump", false, "Use mysqldump command to export tables.")

func checkParameter() {
	if *database == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		fmt.Fprint(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func init() {
	flag.Parse()
	checkParameter()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Go MySQL Dumpper")

	dbReader := &DatabaseReader{
		host:     *host,
		port:     *port,
		socket:   *socket,
		database: *database,
		username: *username,
		password: *password}

	dbReader.init()
	dbReader.getTables()
	dbReader.db.Close()
}

func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf(msg+": %v", err)
	}
}
