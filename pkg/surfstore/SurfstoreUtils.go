package surfstore

import (
	"reflect"
	"os"
	"path/filepath"
	"log"
)

func DownloadFilesFromRemote(client RPCClient, localIndex, modIndex, remoteIndex map[string]*FileMetaData) {
	for fname, metaData := range remoteIndex {
		_, status1 := localIndex[fname]
		_, status2 := modIndex[fname]

		if !status1 || localIndex[fname].Version < remoteIndex[fname].Version {
			pathToFile, _ := filepath.Abs(ConcatPath(client.BaseDir, fname))

			// Check if file was deleted
			if(metaData.BlockHashList[0] == "0") {
				// Delete the file from base dir
				if(status1) {
					os.Remove(pathToFile)
				}
			} else {
				// Download file from remote
				content := make([]byte, 0)
				// blockStoreAddrs := make([]string, 0)
				// client.GetBlockStoreAddrs(&blockStoreAddr)
				blockStoreMap := make(map[string][]string)
				client.GetBlockStoreMap(metaData.BlockHashList, &blockStoreMap)
				hashToBlockMap := make(map[string]*Block)
				for bsAddr, hashLst := range blockStoreMap {
					for _, hashStr := range hashLst {
						block := Block{}
						client.GetBlock(hashStr, bsAddr, &block)
						hashToBlockMap[hashStr] = &block
					}
				}

				for _, hashStr := range metaData.BlockHashList {
					block := hashToBlockMap[hashStr]
					content = append(content, block.BlockData[0:block.BlockSize]...)
				}

				
				writeErr := os.WriteFile(pathToFile, content, 0644)
				if writeErr != nil {
					log.Fatal("Error in writing to file downloaded from remote")
				}
			}
			
			copyMetaData := *metaData
			localIndex[fname] = &copyMetaData

			if(status2) {
				delete(modIndex, fname)
			}
		}
	}
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	modIndex := make(map[string]*FileMetaData)
	remoteIndex := make(map[string]*FileMetaData)

	localIndex, lIerr := LoadMetaFromMetaFile(client.BaseDir)
	if(lIerr != nil) {
		log.Fatal("Error in loading from local index file")
	}

	rIerr := client.GetFileInfoMap(&remoteIndex)
	if(rIerr != nil) {
		log.Fatal("Error in receiving remote index file")
	}

	// Get list of files in base dir
	pathToFile, _ := filepath.Abs(client.BaseDir) 
	files, _ := os.ReadDir(pathToFile)
	fileNames := make(map[string]int32)

	// iterate over files in base dir
	for _, file := range files {
		if !file.IsDir() && file.Name() != "index.db" {
			pathToFile, _ = filepath.Abs(ConcatPath(client.BaseDir, file.Name()))
			fileNames[file.Name()] = 1
			hashList, _ := GetHashAndBlockListFromFile(pathToFile, client.BlockSize)

			if(len(hashList) == 0){
				continue
			}
			_, status := localIndex[file.Name()]

			// If new file was added or existing file was modified, add an entry in modIndex
			if !status || !reflect.DeepEqual(hashList, localIndex[file.Name()].BlockHashList) {
				metaData := FileMetaData{}
				metaData.Filename = file.Name()
				if !status {
					metaData.Version = 1
				} else {
					metaData.Version = localIndex[file.Name()].Version + 1
				} 

				metaData.BlockHashList = hashList
				modIndex[file.Name()] = &metaData
			}
		}
	}

	// Check for deleted files in base dir
	for fname, mData := range localIndex {
		_, found := fileNames[fname]
		if !found && mData.BlockHashList[0] != "0" {
			metaData := FileMetaData{}
			metaData.Filename = fname
			metaData.Version = localIndex[fname].Version + 1
			metaData.BlockHashList = []string {"0"}
			modIndex[fname] = &metaData
		}
	}

	// Case 1: File in remote but not in local and mod
	DownloadFilesFromRemote(client, localIndex, modIndex, remoteIndex)

	
	// Case 2: File in mod but not in local and remote
	//updateSuccess := true
	for fname, metaData := range modIndex {
		//_, status1 := localIndex[fname]
		_, status2 := remoteIndex[fname]

		if !status2 || remoteIndex[fname].Version + 1 == modIndex[fname].Version {
			// Upload file to remote
			pathToFile, _ := filepath.Abs(ConcatPath(client.BaseDir, fname))

			// Check if file was deleted
			if(metaData.BlockHashList[0] != "0") {
				hashList, blockList := GetHashAndBlockListFromFile(pathToFile, client.BlockSize)
				hashToBlockMap := make(map[string][]byte)
				for i:=0; i<len(hashList); i++ {
					hashToBlockMap[hashList[i]] = blockList[i]
				}

				// blockStoreAddrs := make([]string, 0)
				// client.GetBlockStoreAddr(&blockStoreAddrs)
				blockStoreMap := make(map[string][]string)
				client.GetBlockStoreMap(metaData.BlockHashList, &blockStoreMap)

				for bsAddr, hashLst := range blockStoreMap {
					for _, hashStr := range hashLst {
						block := Block{BlockData: hashToBlockMap[hashStr], BlockSize: int32(len(hashToBlockMap[hashStr]))}
						succ := true
						client.PutBlock(&block, bsAddr, &succ)

						if !succ {
							log.Fatal("Error in sending block to remote")
						}
					}
				}
			}

			returnedVersion := metaData.Version
			client.UpdateFile(metaData, &returnedVersion)

			if returnedVersion == -1 {
				// Update failed. Sync from remote
				//updateSuccess = false
			} else {
				copyMetaData := *metaData
				localIndex[fname] = &copyMetaData
			}
		}
	}

	// if !updateSuccess {
	// 	rIerr = client.GetFileInfoMap(&remoteIndex)
	// 	if(rIerr != nil) {
	// 		log.Fatal("Error in receiving remote index file")
	// 	}

	// 	DownloadFilesFromRemote(client, localIndex, modIndex, remoteIndex)
	// }

	WriteMetaFile(localIndex, client.BaseDir)
}
