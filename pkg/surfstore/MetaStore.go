package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"

	"fmt"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddrs []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// TODO - Implement error scenario
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	oldMetaData, status := m.FileMetaMap[fileMetaData.Filename]

	if(!status || oldMetaData.Version + 1 == fileMetaData.Version) {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	} else {
		return &Version{Version: -1}, fmt.Errorf("Version mismatch")
	}
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	bsMap := BlockStoreMap{}
	bsMap.BlockStoreMap = make(map[string]*BlockHashes)

	for _, blockHash := range blockHashesIn.Hashes {
		responsibleServer := m.ConsistentHashRing.GetResponsibleServer(blockHash)

		if _, status := bsMap.BlockStoreMap[responsibleServer]; !status {
			bsMap.BlockStoreMap[responsibleServer] = &BlockHashes{}
			bsMap.BlockStoreMap[responsibleServer].Hashes = make([]string, 0)
		}

		bsMap.BlockStoreMap[responsibleServer].Hashes = append(bsMap.BlockStoreMap[responsibleServer].Hashes, blockHash)
	}

	return &bsMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// TODO - Implement error scenario
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddrs: blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
