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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

// 定义选举超时的上下界(官方定义的选举时间范围，太小则心跳太频繁，太大又选举错误)
const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
	replicateInterval  time.Duration = 200 * time.Millisecond //这里一定要比上面的选举下界还要小,才能抑制一个有异心的peer发起选举
)

// 重置选举时间
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// 判断当前有没有超时
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout //这里直接使用了since这一函数表示时间间隔了
}

type Role string //等价于C语言中的typedef

// 1.定义一个节点的三大类角色
const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.

// 这是一个Raft集群中的节点，里面包含实验所必须的属性
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role            Role          //自身目前的角色
	currentTerm     int           //当前的任期
	voteFor         int           //代表当前任期是否给其他节点投过票，如果返回-1就是没有
	electionStart   time.Time     //选举开始时间
	electionTimeout time.Duration //选举的时间间隔，就是现在时间减去开始时间
}

// 转变为Follower的函数
func (rf *Raft) becomeFollowerLocked(term int) { //这是一个方法，所属对象是Raft，参数是term，无返回值
	if term < rf.currentTerm { //如果当前节点任期大于传过来比较的任期，那么直接表明当前节点不可能转变为Follower，因为没理由
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower,lower term:T%d", term) //定义日志打印
		return
	}
	//如果不是，那么当前节点就会转变为follower
	LOG(rf.me, rf.currentTerm, DError, "%s->Follower,For T%s->T%s", rf.role, rf.currentTerm, term) //定义状态转化的日志打印
	rf.role = Follower
	if term > rf.currentTerm { //如果传来的比较的任期大于节点的任期，那么将投票置为空，以便可以在新的term里进行投票
		rf.voteFor = -1
	}
	rf.currentTerm = term //我们将term置为传进来的term
}

// 转变为Candidate的函数
func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DVote, "Leader can't become Candidate") //变成Candidate的唯一目的就是变成Leader，咋可能变回去呢！
		return
	}
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1) //这里代表在投票的主题上，自身节点变为Candidate状态了，并且给自身投了一票
	rf.currentTerm++
	rf.role = Candidate
	rf.voteFor = rf.me //给自身投了一票
}

// 转变为Leader的函数
func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate { //Leader一进一出，只有Candidate变为Leader，Leader也只能变为follower，所以首先判断不是Candidate就直接寄。
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (PartA).
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
	// Your code here (PartC).
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
	// Your code here (PartC).
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
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct { //这是自己的投票选举参数
	// Your data here (PartA, PartB).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct { //这是其他peer的返回值，携带了是否投给Candidate票的信息，以及自己的term
	// Your data here (PartA).
	Term        int
	VoteGranted bool //是否给他投票
}

// 回调函数 所有 Peer 在运行时都有可能收到要票请求，RequestVote 这个回调函数，就是定义该 Peer 收到要票请求的处理逻辑。
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false //先不投票，满足条件之后，在投票
	//	对齐任期(传参小于当前任期，直接返回并打印日志)
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	//如果传来的任期大于自身的任期，那么自身就变为Follower，
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	//	检查是否已经给别人投过票(因为可能有同时要票的，俗称分裂，就是此现象)
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId { //为什么要加后面的？防止一投再投
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Already voted to S%d", args.CandidateId, rf.voteFor)
		return
	}
	reply.VoteGranted = true      //满足了条件，投票
	rf.voteFor = args.CandidateId //投给了谁？记下这个ID
	//只有投票给对方后，才能重置选举 timer
	rf.resetElectionTimerLocked() //因为投给别人票了，所以这里需要重置自己的选举时间，不能让自己挑战leader的权威
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote granted", args.CandidateId)
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

	// Your code here (PartB).

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

func (rf *Raft) startElection(term int) bool {
	votes := 0

	//单次 RPC 包括构造 RPC 参数、发送 RPC等待结果、对 RPC 结果进行处理三个部分。
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		//发送RPC
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)
		//处理这个响应
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d,Lost or error", peer)
			return
		}
		//	对齐任期
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		//检查上下文是否丢失(上下文就是指的Term和Role)
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVOteReply for S%d", peer)
			return
		}
		//统计投票数
		if reply.VoteGranted {
			votes++
		}

		if votes > len(rf.peers)/2 && rf.role != Leader {
			rf.becomeLeaderLocked()
			go rf.replicationTicker(term) //已经变成leader了，发起心跳和日志同步
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVOteReply for S%d", rf.role)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me { //如果不是自己就直接投票++
			votes++
			continue
		}
		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}

		go askVoteFromPeer(peer, args) //RPC需要长时间的IO，不能在临界区里面进行，否则会造成死锁，一直不释放锁
	}

	return true
}

// 满足条件时，转变为Candidate，然后异步地(同步造成主Loop检查延迟)发起选举
func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm) //发起选举
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//这里睡眠也是随机一个时间，不是固定时间，防止同时检测到超时
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 检查上下文是否丢失。即在一个任期内，只要你的角色没有变化就能放心推进状态机
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

// 心跳 Loop：在当选 Leader 后起一个后台线程，等间隔的发送心跳/复制日志，称为 replicationTicker
// 由于不用构造随机超时间隔，心跳 Loop 会比选举 Loop 简单很多：
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		//与选举 Loop 不同的是，这里的 startReplication 有个返回值，主要是检测“上下文”是否还在（ ContextLost ）——
		//一旦发现 Raft Peer 已经不是这个 term 的 Leader 了，就立即退出 Loop。
		ok := rf.startReplication(term) //单轮心跳
		if !ok {
			return
		}
		time.Sleep(replicateInterval)
	}
}

// 追加日志的结构体定义(对象定义)
type AppendEntriesArgs struct { //RPC的请求参数
	Term     int
	LeaderId int
}

type AppendEntriesReply struct { //RPC的返回参数
	Term    int
	Success bool
}

// 对端的回调函数
// 心跳接收方在收到心跳时，只要 Leader 的 term 不小于自己，就对其进行认可，变为 Follower，并重置选举时钟，承诺一段时间内不发起选举。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	//	对齐任期
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}
	rf.resetElectionTimerLocked()
	reply.Success = true

}

// 真正发送心跳RPC的函数
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesArgs) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) //注意要修改这里的路由参数为Raft.AppendEntries，这样才能找到对端路由的参数
	return ok
}

// 单轮心跳
// 和 Candidate 的选举逻辑类似，Leader 会给除自己外的所有其他 Peer 发送心跳。在发送前要检测“上下文”是否还在，如果不在了，就直接返回 false ——告诉外层循环 replicationTicker 可以终止循环了。
// 因此 startReplication 的返回值含义为：是否成功的发起了一轮心跳。
func (rf *Raft) startReplication(term int) bool {
	//RPC 回调函数 单次 RPC
	//在不关心日志时，心跳的返回值处理比较简单，只需要对齐下 term 就行。如果后续还要进行其他处理，则还要检查 context 是否丢失。
	//该函数的目的是将一个日志条目（AppendEntriesArgs）复制到一个指定的节点（peer）
	replicationToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesArgs{}
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crushed", peer)
			return
		}
		//	对齐任期(接到心跳当然对齐心跳了！)
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) { //检查是不是依然是这个Term的leader
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm) //记录了是从那个Term开始丢的
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			continue
		}
		args := &AppendEntriesArgs{ //这是请求参数，构造心跳(复制日志)的的RPC请求参数
			Term:     term,
			LeaderId: rf.me,
		}
		go replicationToPeer(peer, args)
	}
	return true

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

// 这是创建并初始化一个peer节点的工作！
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower //节点从一开始就是follower
	rf.currentTerm = 0 //表示当前节点任期为0
	rf.voteFor = -1    //表示当前节点为给其他投票

	// initialize from state persisted before a crash(宕机重启之后读取持久化信息以覆盖上面的初始化字段，这是Part C的内容，现在不用管)
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	//开始异步进入选举
	go rf.electionTicker()

	return rf
}
