package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	grpc "google.golang.org/grpc"
	"net"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddrs := []string{}
	if len(args) >= 1 {
		blockStoreAddrs = args
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

// TODO - Change signature to handle multiple block addrs
func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	//Step 1 : Create a new Server
	grpcServer := grpc.NewServer()

	//Step 2: Register rpc services
	if(serviceType=="meta") {
		srv := surfstore.NewMetaStore(blockStoreAddrs)
		surfstore.RegisterMetaStoreServer(grpcServer, srv)
	} else if (serviceType == "block") {
		srv := surfstore.NewBlockStore()
		surfstore.RegisterBlockStoreServer(grpcServer, srv)
	} else if (serviceType == "both") {
		srv1 := surfstore.NewMetaStore(blockStoreAddrs)
		surfstore.RegisterMetaStoreServer(grpcServer, srv1)
		srv2 := surfstore.NewBlockStore()
		surfstore.RegisterBlockStoreServer(grpcServer, srv2)
	}
	
	//Step 3: start listening on hostAddr
	l, e := net.Listen("tcp", hostAddr)
	if e != nil {
		return e
	}

	return grpcServer.Serve(l)
}
