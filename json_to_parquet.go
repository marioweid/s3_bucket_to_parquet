package main

import (
	"encoding/json"
	"fmt"
	types "go_code/go_packages"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/joho/godotenv"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func read_json_files(folder string) ([]types.DailyExport, error) {
	var allExports []types.DailyExport

	// Glob all jsons
	files, _ := filepath.Glob(folder)

	for _, file := range files {
		// Read file content
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}

		// Unmarshal the JSON into a slice of records
		var records []types.DailyExport
		err = json.Unmarshal(data, &records)
		if err != nil {
			return nil, err
		}

		// Append the records from this file to the global slice
		allExports = append(allExports, records...)
	}
	return allExports, nil
}

func writeParquet(filename string, records []types.DailyExport) error {
	var err error
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		log.Fatal("Can't create parquet writer", err)
		return err
	}

	pw, err := writer.NewParquetWriter(fw, new(types.DailyExport), 4)
	if err != nil {
		log.Fatal("Can't create parquet writer", err)
		return err
	}

	pw.RowGroupSize = 5 * 1024 * 1024
	pw.PageSize = 1 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, dailyexport := range records {

		if err = pw.Write(dailyexport); err != nil {
			log.Fatal("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Fatal("WriteStop error", err)
		return err
	}

	log.Println("Write Finished")
	fw.Close()
	return err
}

func main() {
	// Measure runtime
	start := time.Now()

	// Read env file
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error while loading .env file!")
	}
	// Process Json files
	all_exports, _ := read_json_files(fmt.Sprintf("%s/*.json", os.Getenv("SUBFOLDER")))
	writeParquet("out/go.parquet", all_exports)

	elapsed := time.Since(start)
	log.Printf("Time %s", elapsed)
}
