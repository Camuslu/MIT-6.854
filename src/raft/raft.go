package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	lastTimeHeardFromPeers time.Time
	isLeader               bool

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	logs        []RaftLog
}

type RaftLog struct {
	Term int
}

const BroadcastTime = 101
const ElectionTimeout = 1500
const WaitTimeForRPC = 300
const DummyTime = "2006-01-02"

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int // leader's current term
	LeaderId     int //
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself (???)
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		fmt.Printf("[Peer %v][HeartBeat Denied] from candidate %v; candidate term is %v < my term %v\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
	} else {
		fmt.Printf("[Peer %v][HeartBeat Acknowledged] from leader %v; leader term is %v >= my term %v\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = args.Term
		rf.currentTerm = args.Term
		if rf.isLeader {
			rf.isLeader = false
			fmt.Printf("[Peer %v] back to follower \n", rf.me)
		}
		rf.lastTimeHeardFromPeers = time.Now() // not sure about this part (what if the arg.term < me.term ? )
	}
	reply.Success = true // not sure about this

	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
// Servers can only vote for one leader per term
// Also deny vote to candidate with less up-to-date log
// - If last term in log different, higher term is more up to date
// - Else if same term, higher log entry is more up to date
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	fmt.Printf("[Peer %v] handling request vote...from %v for term %v; my term is %v; my votedFor is %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("[Peer %v][RequestVote Denied] from %v as candidate term is %v < peer term %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	} else if args.Term > rf.currentTerm {
		fmt.Printf("[Peer %v][RequestVote Approved] from %v approved: candidate term is %v > peer term %v \n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.lastTimeHeardFromPeers = time.Now()
		rf.votedFor = args.CandidateId
	} else if args.Term == rf.currentTerm && args.LastLogIndex > len(rf.logs) && rf.votedFor == -1 {
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.lastTimeHeardFromPeers = time.Now()
		rf.votedFor = args.CandidateId
		fmt.Printf("[Peer %v][RequestVote Approved] from %v approved: same term and longer log \n", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted = false
		fmt.Printf("[Peer %v][RequestVote Denied] from %v denied \n", rf.me, args.CandidateId)
		// not sure about this part...
		if args.Term > rf.currentTerm {
			reply.Term = args.Term
			rf.currentTerm = args.Term
		} else {
			reply.Term = rf.currentTerm
		}
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	t, _ := time.Parse(DummyTime, DummyTime)
	var justStarted = rf.lastTimeHeardFromPeers == t

	if justStarted {
		ms := 50 + (rand.Int63() % 300) // something in [50, 350] range
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		now := time.Now()
		var electionTimeOutSinceLastMsgFromLeader = (now.Sub(rf.lastTimeHeardFromPeers).Milliseconds() > ElectionTimeout && rf.isLeader == false)

		if electionTimeOutSinceLastMsgFromLeader {
			rf.currentTerm += 1 // increment term
			fmt.Printf("[Peer %v Candidate Start Election] for term %v\n", rf.me, rf.currentTerm)
			// candidate status now
			rf.mu.Lock()
			rf.votedFor = rf.me // vote for itself
			rf.lastTimeHeardFromPeers = time.Now()
			var voteCount int32
			voteCount += 1
			rf.mu.Unlock()
			var wg sync.WaitGroup
			// replyChannel := make(chan RequestVoteReply, len(rf.peers))
			for idx, _ := range rf.peers {
				if idx != rf.me {
					wg.Add(1)
					defer wg.Done()
					go func(peerIdx int) {
						reply := rf.sendOneRequestVote(peerIdx)
						time.Sleep(WaitTimeForRPC * time.Millisecond)
						if reply.VoteGranted {
							atomic.AddInt32(&voteCount, 1)
							fmt.Printf("[Peer %v][RequestVote Approved received] from peer %v: approved\n", rf.me, peerIdx)
							if int(atomic.LoadInt32(&voteCount)) > len(rf.peers)/2 {
								fmt.Printf("[Candidate %v Won Election] for term %v with vote count = %v\n", rf.me, rf.currentTerm, voteCount)
								rf.lastTimeHeardFromPeers = time.Now()
								rf.isLeader = true
								return
							}
						} else if reply.Term > 0 {
							fmt.Printf("[Peer %v][RequestVote Denied received] from peer %v: denied\n", rf.me, peerIdx)
							if reply.Term > rf.currentTerm {
								rf.mu.Lock()
								rf.currentTerm = reply.Term
								rf.lastTimeHeardFromPeers = time.Now()
								rf.isLeader = false
								rf.mu.Unlock()
								// break // become follower again
							}
						} else { // false return; what to do here?
							fmt.Printf("[Peer %v][RequestVote Blank result received] from peer %v\n", rf.me, peerIdx)
						}
					}(idx)
				}
			}
			// wg.Wait()
			if int(atomic.LoadInt32(&voteCount)) <= len(rf.peers)/2 {
				fmt.Printf("[Candidate %v Lost Election] with vote count = %v\n", rf.me, voteCount)
			}
			// rf.votedFor = -1
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendOneRequestVote(peerIdx int) *RequestVoteReply {
	// send one request from rf to peer idx
	var lastLogTerm int
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	} else {
		lastLogTerm = rf.currentTerm
	}
	arg := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs),
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	fmt.Printf("[Peer %v][RequestVote] sending to %v \n", rf.me, peerIdx)
	rf.sendRequestVote(peerIdx, &arg, &reply)
	return &reply
}

func (rf *Raft) sendHeartBeatTicker() {

	for rf.killed() == false {
		var wg sync.WaitGroup
		if rf.isLeader {
			for idx, _ := range rf.peers {
				if idx != rf.me {
					go func(peerIdx int) {
						wg.Add(1)
						defer wg.Done()
						reply := rf.sendOneHeartBeat(peerIdx)
						time.Sleep(WaitTimeForRPC * time.Millisecond)
						if reply.Term > rf.currentTerm {
							rf.isLeader = false
							rf.currentTerm = reply.Term
							rf.lastTimeHeardFromPeers = time.Now()
							fmt.Printf("[Peer %v][HeartBeat Denied Received] from %v; updated term %v \n", rf.me, peerIdx, reply.Term)
							return
							// break // become a follower
						} else if reply.Success {
							fmt.Printf("[Peer %v][HeartBeat Acknowledged] received from %v \n", rf.me, peerIdx)
						} else {
							fmt.Printf("[Peer %v][HeartBeat Blank] received from %v \n", rf.me, peerIdx)
						}
					}(idx)
				}
			}
			// wg.Wait()
			time.Sleep(BroadcastTime * time.Millisecond)
		}
	}
}

func (rf *Raft) sendOneHeartBeat(peerIdx int) *AppendEntriesReply {
	var prevLogTerm int
	if len(rf.logs) > 0 {
		prevLogTerm = rf.logs[len(rf.logs)-1].Term
	} else {
		prevLogTerm = rf.currentTerm
	}
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.logs),
		PrevLogTerm:  prevLogTerm,
		Entries:      make([]RaftLog, 0),
	}
	reply := AppendEntriesReply{}
	fmt.Printf("[Peer %v][HeartBeat sending] to %v \n", rf.me, peerIdx)
	rf.sendHeartBeat(peerIdx, &arg, &reply)
	return &reply
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	t, _ := time.Parse(DummyTime, DummyTime)
	rf.lastTimeHeardFromPeers = t // dummy start time
	rf.isLeader = false
	rf.votedFor = -1
	rf.currentTerm = 1

	go rf.ticker()
	go rf.sendHeartBeatTicker()

	return rf
}
