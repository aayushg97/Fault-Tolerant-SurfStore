package surfstore

import (
	context "context"
	"time"
	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	
	*succ = b.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashesOut = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	
	*blockHashes = b.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the server
	var retVal error
	retVal = nil

	for i:=0; i<len(surfClient.MetaStoreAddrs); i++ {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		b, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			retVal = err
			continue
		}

		*serverFileInfoMap = b.FileInfoMap
		return conn.Close()
	}

	// close the connection
	return retVal
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the server
	var retVal error
	retVal = nil

	for i:=0; i<len(surfClient.MetaStoreAddrs); i++ {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		b, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			conn.Close()
			retVal = err
			continue
		}

		*latestVersion = b.Version
		return conn.Close()
	}

	// close the connection
	return retVal
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// connect to the server
	var retVal error
	retVal = nil

	for i:=0; i<len(surfClient.MetaStoreAddrs); i++ {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		bHashes := BlockHashes{}
		bHashes.Hashes = blockHashesIn
		b, err := c.GetBlockStoreMap(ctx, &bHashes)
		if err != nil {
			conn.Close()
			retVal = err
			continue
		}

		for key, value := range b.BlockStoreMap {
			(*blockStoreMap)[key] = value.Hashes
		}

		return conn.Close()
	}

	// close the connection
	return retVal
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// connect to the server
	var retVal error
	retVal = nil

	for i:=0; i<len(surfClient.MetaStoreAddrs); i++ {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		b, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			retVal = err
			continue
		}

		*blockStoreAddrs = b.BlockStoreAddrs
		return conn.Close()
	}

	// close the connection
	return retVal
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
