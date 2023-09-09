package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	blockP, ok := bs.BlockMap[blockHash.Hash]

	if !ok {
		return nil, fmt.Errorf("Block was not found in the map")
	} else {
		return blockP, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// TODO :- Implement error scenario
	bs.BlockMap[GetBlockHashString(block.BlockData)] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// TODO - Implement error scenario
	blockHashesOut := BlockHashes{}

	for _, str := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[str]
		if(ok) {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, str)
		}
	}

	return &blockHashesOut, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashes := BlockHashes{}
	blockHashes.Hashes = make([]string, 0)

	for hash, _ := range bs.BlockMap {
		blockHashes.Hashes = append(blockHashes.Hashes, hash)
	}

	return &blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}