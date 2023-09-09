package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	
	// Insert tuples into db
	statement, _ = db.Prepare(insertTuple)

	for _, metaData := range fileMetas {
		ind := 0
		for _, hashStr := range metaData.BlockHashList {
			statement.Exec(metaData.Filename, metaData.Version, ind, hashStr)
			ind += 1
		}
	}

	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = ``

const getTuplesByFileName string = `select fileName, version, hashIndex, hashValue 
from indexes order by fileName, version, hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	
	rows, err := db.Query(getTuplesByFileName, "cse")
	if err != nil {
		fmt.Println(err.Error())
	}

	var fname string
	var version int32
	var hashStr string
	var hashInd int32

	for rows.Next() {
		rows.Scan(&fname, &version, &hashInd, &hashStr)

		_, err := fileMetaMap[fname]

		if(!err){
			metaData := FileMetaData{}
			metaData.Filename = fname
			metaData.Version = version
			fileMetaMap[fname] = &metaData
		}

		fileMetaMap[fname].BlockHashList = append(fileMetaMap[fname].BlockHashList, hashStr)		
	}

	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}

func GetHashAndBlockListFromFile(pathToFile string, blockSize int) ([]string, [][]byte) {
	content, fileErr := os.ReadFile(pathToFile)
	if fileErr != nil {
		log.Fatal("Unable to open file ", pathToFile)
	}

	hashList := make([]string, 0)
	blockList := make([][]byte, 0)
	for i:=0; i < len(content); i+=blockSize {
		end := i + blockSize
		if end > len(content) {
			end  = len(content)
		}
		hashList = append(hashList, GetBlockHashString(content[i : end]))
		blockList = append(blockList, content[i : end])
	}

	return hashList, blockList
}