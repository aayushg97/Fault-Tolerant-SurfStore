package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"net"
	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
    // TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
	}

	server.id = id
	server.peers = config.RaftAddrs
	server.commitIndex = int64(-1)
	server.lastApplied = int64(-1)

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
    grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)

	l, e := net.Listen("tcp", server.peers[server.id])
	if e != nil {
		return e
	}

	return grpcServer.Serve(l)
}
