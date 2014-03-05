package main

import (
	"os"
)

type FileWriter struct {
	inData   chan string
	fileName string
}

func NewFileWriter(fileName string) *FileWriter {
	fileWriter := &FileWriter{fileName: fileName}
	fileWriter.init()
	return fileWriter
}

func (writer *FileWriter) init() {
	writer.inData = make(chan string, 0)
}

func (writer *FileWriter) write(data string) {
	writer.inData <- data
}

func (writer *FileWriter) writeNewLine() {
	writer.inData <- "\n\n"
}

func (writer *FileWriter) start() {
	// create file to write data
	file, err := os.Create(writer.fileName)
	handleError(err, "Could not create file")

	// fetch data and write to file
	for data := range writer.inData {
		file.WriteString(data)
	}

	err = file.Close()
	handleError(err, "Could not close file")
}

func (writer *FileWriter) finish() {
	// close income data chanel
	close(writer.inData)
}
