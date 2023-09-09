package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	grpc "google.golang.org/grpc"
	"sync"
	"fmt"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log         []*UpdateOperation

	metaStore *MetaStore

	// Added for discussion
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
    // Return error if not leader
	if !s.isLeader {
		return &FileInfoMap{}, ERR_NOT_LEADER
	}

	// Return error if crashed
	if s.isCrashed {
		return &FileInfoMap{}, ERR_SERVER_CRASHED
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: nil,
	})

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		return s.metaStore.GetFileInfoMap(ctx, empty)
	}

    return &FileInfoMap{}, fmt.Errorf("Error in getting fileinfomap")
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
    // Return error if not leader
	if !s.isLeader {
		return &BlockStoreMap{}, ERR_NOT_LEADER
	}

	// Return error if crashed
	if s.isCrashed {
		return &BlockStoreMap{}, ERR_SERVER_CRASHED
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: nil,
	})

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		return s.metaStore.GetBlockStoreMap(ctx, hashes)
	}

    return &BlockStoreMap{}, fmt.Errorf("Error in getting block store map")
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
    // Return error if not leader
	if !s.isLeader {
		return &BlockStoreAddrs{}, ERR_NOT_LEADER
	}

	// Return error if crashed
	if s.isCrashed {
		return &BlockStoreAddrs{}, ERR_SERVER_CRASHED
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: nil,
	})

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		return s.metaStore.GetBlockStoreAddrs(ctx, empty)
	}

    return &BlockStoreAddrs{}, fmt.Errorf("Error in getting block store address")
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
    // Return error if not leader
	if !s.isLeader {
		return &Version{Version: -1}, ERR_NOT_LEADER
	}

	// Return error if crashed
	if s.isCrashed {
		return &Version{Version: -1}, ERR_SERVER_CRASHED
	}
	
	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return &Version{Version: -1}, fmt.Errorf("Version mismatch")
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies

	responses := make(chan bool, len(s.peers)-1)
	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		go s.sendToFollower(ctx, addr, responses)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	if totalAppends > len(s.peers)/2 {
		// TODO put on correct channel
		*s.pendingCommits[len(s.pendingCommits)-1] <- true
		// TODO update commit Index correctly
		s.commitIndex = int64(len(s.log) - 1)
		s.lastApplied = s.commitIndex
		//fmt.Println("Majority obtained")
	} else {
		//*s.pendingCommits[len(s.pendingCommits)-1] <- false
		//fmt.Println("Majority not obtained")
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool) {
	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  int64(-1),
		PrevLogIndex: s.lastApplied,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	if len(s.log) > 0 && s.lastApplied >= 0 && s.lastApplied < int64(len(s.log)) {
		dummyAppendEntriesInput.PrevLogTerm = s.log[s.lastApplied].Term
	}

	// TODO check all errors
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)

	retVal, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)

	if err != nil || (retVal.Success==false && retVal.Term > s.term) {
		responses <- false
		// fmt.Println("Error here")
		// fmt.Println(err)
		// if (retVal.Success==false && retVal.Term > s.term) {
		// 	fmt.Println("condition false")
		// }
		return
	}

	// TODO check output
	responses <- true

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	result := AppendEntryOutput{Success:false}

	if s.isCrashed {
		return &result, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	}

	// TODO actually check entries
	if input.Term < s.term {
		result.Success = false
		result.Term = s.term
		return &result, nil//fmt.Errorf("Input term less than current term")
	}

	// if input.prevLogIndex >= len(s.log) || s.log[input.prevLogIndex].Term != input.prevLogTerm {
	// 	result.Success = false
	// 	result.Term = s.term
	// 	result.MatchedIndex = s.lastApplied
	// 	return result, nil//fmt.Errorf("Term of entry at prevLogIndex does not match prevLogTerm")
	// }

	
	s.log = input.Entries

	for s.lastApplied < input.LeaderCommit {
		entry := s.log[s.lastApplied+1]
		if entry.FileMetaData != nil {
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
		s.lastApplied++
	}

	s.commitIndex = input.LeaderCommit

	result.Success = true
	result.ServerId = s.id
	result.Term = s.term
	result.MatchedIndex = s.lastApplied

	return &result, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
    if s.isCrashed {
		return &Success{}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	// TODO update state
	return &Success{Flag:true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
    if s.isCrashed {
		return &Success{}, ERR_SERVER_CRASHED
	}

	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  int64(-1),
		PrevLogIndex: s.lastApplied,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	if len(s.log) > 0 && s.lastApplied >= 0 && s.lastApplied < int64(len(s.log)) {
		dummyAppendEntriesInput.PrevLogTerm = s.log[s.lastApplied].Term
	}

	answer := Success{Flag:true}
	totalResponses := 1
	totalAppends := 1

	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		// TODO check all errors
		conn, _ := grpc.Dial(addr, grpc.WithInsecure())
		client := NewRaftSurfstoreClient(conn)

		result, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		
		totalResponses++

		if err != nil {
			// fmt.Println("Error is here")
			// fmt.Println(err)
			continue
		}

		if result.Success {
			totalAppends++
		}

		answer.Flag = result.Success

		if result.Success == false {
			break
		}
	}

	if totalResponses == len(s.peers) && totalAppends > len(s.peers)/2 && int64(len(s.pendingCommits)) > s.commitIndex+1 {
		*s.pendingCommits[len(s.pendingCommits)-1] <- true
		s.commitIndex = int64(len(s.log) - 1)
		s.lastApplied = s.commitIndex
	}

	return &answer, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
