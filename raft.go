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

import "sync"
import "labrpc"
import "time"
import "fmt"
import "bytes"
import "encoding/gob"
import "math/rand"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
    Index       int
    Command     interface{}
    UseSnapshot bool   // ignore for lab2; only used in lab3
    Snapshot    []byte // ignore for lab2; only used in lab3
}

//枚举状态
const (
    Follower = iota
    Candidate
    Leader
)

//设定几个时间域值
const heartbeatTime = 50
const min_electionTime = 400
const max_electionTime = 500

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex
    peers     []*labrpc.ClientEnd
    persister *Persister
    me        int // index into peers[]

    //persistent state on all servers
    currentTerm int
    votedFor int
    logs []LogEntry

    //volatile state on all servers
    commitIndex int
    lastApplied int

    //volatile state on leaders
    nextIndex []int
    matchIndex []int

    vote_count int//记录获得的投票总数
    applyCh chan ApplyMsg

    //以下两个chan变量主要用于解决，投票成功以及成功收到log entry后
    //follower和leader子线程reset主线程Timer失败的问题。
    skip_block1_Ch  chan struct{}//通知主线程，vote完成，可以跳过阻塞重新计时
    skip_block2_Ch  chan struct{}//通知主线程，append完成，可以跳过阻塞重新计时

    state int

    timer *time.Timer
}



// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isleader bool
    // Your code here.
    term = rf.currentTerm
    if rf.state == Leader{
        isleader = true
    }else{
        isleader = false
    }
    return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here.
    // Example:
    // w := new(bytes.Buffer)
    // e := gob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.logs)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    // Your code here.
    // Example:
    // r := bytes.NewBuffer(data)
    // d := gob.NewDecoder(r)
    // d.Decode(&rf.xxx)
    // d.Decode(&rf.yyy)
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if data == nil || len(data) < 1{
        return
    }
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
    d.Decode(&rf.logs)
}


type LogEntry struct{
    Command interface{}
    Term int
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    // Your data here.
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
    // Your data here.
    Term int
    VoteGranted bool
}

type AppendEntriesArgs struct{
    Term int
    LeaderId int
    PrevLogTerm int
    PrevLogIndex int
    Entries []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct{
    Term int
    Success bool
    CommitIndex int
}


//该函数用于随机一个broadcast 或者随机一个election Timeout.
//参数单位ms
func randomTimeout() time.Duration{
    randomTime := min_electionTime + rand.Intn(max_electionTime - min_electionTime)
    return time.Duration(randomTime)*time.Millisecond
}

func (rf *Raft) resetTimer(){

    rf.timer = time.NewTimer(randomTimeout())

    switch rf.state{
    case Follower:
        rf.timer.Reset(randomTimeout())
    case Candidate:
        rf.timer.Reset(randomTimeout())
    default:

    }
}

func (rf *Raft) run(){
    electionTimeout := randomTimeout()
    rf.timer = time.NewTimer(electionTimeout)
    for{
        if false{
            fmt.Printf("peerId: %d term: %d state: %d\n", rf.me, rf.currentTerm, rf.state)
        }

        switch rf.state{
        case Follower:
            select{
            case <- rf.skip_block1_Ch://收到vote完成信号，跳出rf.timer.C的阻塞，
                                      //重新判断rf.state,并且重新计时
            case <- rf.skip_block2_Ch://收到append完成信号，跳出rf.timer.C的阻塞，
                                      //重新判断rf.state,并且重新计时
            case <- rf.timer.C:

                rf.waitForTimeout()
            }
        case Candidate:
            select{
            case <- rf.skip_block2_Ch:
            case <- rf.timer.C:
                rf.waitForTimeout()
            default://如果得到majority票数，那么立即切换状态到Leader，
                    //广播Leader信号，防止其他成员发生选举超时
                if rf.vote_count > len(rf.peers)/2 {
                    rf.state = Leader
                    for i:=0; i<len(rf.peers); i++{
                        if i == rf.me{
                            continue
                        }
                        rf.nextIndex[i] = len(rf.logs)
                        rf.matchIndex[i] = -1
                    }
                    rf.resetTimer()
                }
            }
        case Leader:
            rf.waitForTimeout()
            time.Sleep(heartbeatTime*(1e6))
        }
    }
}

func (rf *Raft) waitForTimeout(){
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != Leader{
        rf.state = Candidate
        rf.currentTerm = rf.currentTerm + 1
        rf.votedFor = rf.me
        rf.vote_count = 1//先投自己一票
        rf.persist()//将当前的状态写入存储器保存
        args := RequestVoteArgs{
            //作为候选人县公开自己的身份信息，包括Term和LastLogIndex
            //方便大家选举参考。
            Term: rf.currentTerm,
            CandidateId: rf.me,
            LastLogIndex: len(rf.logs) - 1,
        }
        //判断logs中是否有信息，如果有的话，把候选人的LastLoTerm信息就打包上
        if len(rf.logs) > 0 {
            args.LastLogTerm = rf.logs[args.LastLogIndex].Term//将当前的term也写入args
        }

        for i := 0; i < len(rf.peers); i++{
            if i == rf.me{
                continue
            }
            go func(server int, args RequestVoteArgs){//多线程并发，发起投票请求
                var reply RequestVoteReply
                //通过reply指针获取返回的vote信息
                ok := rf.sendRequestVote(server, args, &reply)
                if ok{
                    rf.solveVote(reply)//回调机制，处理别人对自己的vote
                }
            }(i, args)
        }

    }else{//如果是leader就要持续地广播，维护自己的身份
        rf.AppendEntriestoAll()
    }
    rf.resetTimer()
}

//
// example RequestVote RPC handler.
//
//在某个candidate发起投票选举之后，接收到信息并判断是否给它投票
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here.
    rf.mu.Lock()
    defer rf.mu.Unlock()
    is_Vote := true

    if len(rf.logs) > 0{
        //两种情况不投票
        //1、该candidate的term不够新
        //2、或者该candidate的term和自己相当，并且自己的logIndex比较大
        if (rf.logs[len(rf.logs)-1].Term > args.LastLogTerm) || (rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex){
            is_Vote = false
        }
    }

    if args.Term < rf.currentTerm{
        reply.Term = rf.currentTerm
        reply.VoteGranted = false

    }

    if args.Term == rf.currentTerm{
        //如果本次term中没有投过票，避免在一个term中多次投票
        if rf.votedFor == -1 && is_Vote {
            rf.votedFor = args.CandidateId
            rf.persist()//记住自己投票选举的人
        }
        reply.Term = rf.currentTerm
        reply.VoteGranted = (rf.votedFor == args.CandidateId)

    }
    //如果自己不够新，那么把自己的状态置为Follower,为别人投票
    if args.Term > rf.currentTerm{
        rf.state = Follower
        rf.currentTerm = args.Term
        rf.votedFor = -1

        if is_Vote{
            rf.votedFor = args.CandidateId
            rf.persist()//记住自己投票选举的人
        }
        rf.resetTimer()

        reply.Term = args.Term
        reply.VoteGranted = (rf.votedFor == args.CandidateId)

    }
    if reply.VoteGranted == true {//给主线程发vote完成信号
        go func() { rf.skip_block1_Ch <- struct{}{} }()
    }
}

//
//finish and get the vote result
//
func (rf *Raft) solveVote(reply RequestVoteReply){
    rf.mu.Lock()
    defer rf.mu.Unlock()
    //避免过时的信息
    if reply.Term < rf.currentTerm{
        return
    }
    //如果对方比自己更新，那么自动降级为follower
    if reply.Term > rf.currentTerm{
        rf.currentTerm = reply.Term
        rf.state = Follower
        rf.votedFor = -1//将自己置为为投票状态
        rf.resetTimer()
        return
    }
    //如果自己在candidate期间获得投票
    if rf.state == Candidate && reply.VoteGranted{
        rf.vote_count += 1
        if rf.vote_count >= (len(rf.peers)/2+1){
            rf.state = Leader
            for i:=0; i<len(rf.peers); i++{
                if i == rf.me{
                    continue
                }
                rf.nextIndex[i] = len(rf.logs)
                rf.matchIndex[i] = -1
            }
        }
        rf.resetTimer()
    }
    return
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}


func (rf *Raft) AppendEntriestoAll(){

    for i:=0;i<len(rf.peers);i++{
        if i==rf.me{
            continue
        }else
        {
            var args AppendEntriesArgs//args保存Leader的一系列信息，发给Follower
            args.Term = rf.currentTerm
            args.LeaderId = rf.me
            args.PrevLogIndex = rf.nextIndex[i] - 1

            if args.PrevLogIndex >=0{//如果PrevLogIndex存在的话，必然也有相应的PrevLogTerm
                args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
            }

            if rf.nextIndex[i] < len(rf.logs){
                //Leader可能有一些日志没有commit
                args.Entries = rf.logs[rf.nextIndex[i]:]
            }

            args.LeaderCommit = rf.commitIndex//LeaderCommit有助于Follower追随。

            go func(server int, args AppendEntriesArgs){//并发广播AppendEntries RPC
                var reply AppendEntriesReply
                ok:=rf.AppendEntriestoOne(server, args, &reply)//单播RPC回调函数
                if ok{
                    rf.solveAppend(server, reply)//
                }
            }(i, args)
        }
    }
}

func (rf *Raft) solveAppend(server int, reply AppendEntriesReply){
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != Leader{//如果发现比自己更新的Leader,自己随时可能成为Follower
        return
    }

    if reply.Term > rf.currentTerm{
        //自己的term不够大，说明有更新的Leader,自己成为Follower
        rf.currentTerm = reply.Term
        rf.state = Follower
        rf.votedFor = -1
        rf.resetTimer()
        return
    }

    if reply.Success{
        //收到Follower commit成功的信息。
        rf.nextIndex[server] = reply.CommitIndex + 1
        rf.matchIndex[server] = reply.CommitIndex
        reply_count :=1
        for i:= 0; i < len(rf.peers); i++{
            if i == rf.me{
                continue
            }
            if rf.matchIndex[i] >= rf.matchIndex[server]{
                reply_count += 1
            }
        }
        if reply_count > len(rf.peers)/2 &&
            rf.commitIndex < rf.matchIndex[server] &&
            rf.logs[rf.matchIndex[server]].Term == rf.currentTerm{
                //判断Leader是否需要commit logs：1）大多数已经commit,
                //2)当前Leader确实还没commit
                //3）当前的term是符合一致性要求的。
                rf.commitIndex = rf.matchIndex[server]
                go rf.commit()
        }

    }else{
        //如果commit失败，并且自己还是Leader身份，那么继续广播AppendEntries RPC
        rf.nextIndex[server] = reply.CommitIndex + 1
        rf.AppendEntriestoAll()
    }
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply){
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm{//如果收到来自某个Leader的Term比自己还小，否认其身份
        reply.Success = false
        reply.Term = rf.currentTerm
    }else
    {
        rf.state = Follower//如果Leader发现自己的term比别人小，重置自己的状态为Follower
        rf.currentTerm = args.Term
        rf.votedFor = -1
        reply.Term = args.Term
        //一开始Leader和Follower进行通信时，nextIndex的值是Leader的logs的size大小，所以系统需要找到匹配的term和index
        if args.PrevLogIndex >= 0 && (len(rf.logs)-1 < args.PrevLogIndex ||rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm){
            reply.CommitIndex = len(rf.logs) - 1
            if reply.CommitIndex > args.PrevLogIndex{
                reply.CommitIndex = args.PrevLogIndex
            }
            for reply.CommitIndex >= 0{
                if rf.logs[reply.CommitIndex].Term == args.PrevLogTerm{
                    break
                }
                reply.CommitIndex--
            }
            reply.Success = false
        }else if args.Entries != nil{
            //如果PrevlogIndex和PrevLogTerm匹配成功，那么可以commit Leader的logs。
            rf.logs = rf.logs[:args.PrevLogIndex + 1]
            rf.logs = append(rf.logs, args.Entries...)
            if len(rf.logs) - 1 >= args.LeaderCommit{
                rf.commitIndex = args.LeaderCommit
                go rf.commit()
            }
            reply.CommitIndex = len(rf.logs) - 1
            //暂时不会更新rf.commitIndex = reply.commitIndex,
            //等Leader先commit，Follower自动更新为LeaderCommit。
            reply.Success = true
        }else{
            //没有需要commit的日志，但是要保持和Leader的一致性
            if len(rf.logs)-1 >= args.LeaderCommit{
                rf.commitIndex = args.LeaderCommit
                go rf.commit()
            }
            reply.CommitIndex = args.PrevLogIndex
            reply.Success = true
        }
    }
    rf.persist()
    rf.resetTimer()//每次收到Leader的heartbeat就重新计时，防止等待超时而发起vote
    go func() { rf.skip_block2_Ch <- struct{}{} }()//给主线程发append完成信号
}

func (rf *Raft) commit(){
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.commitIndex > len(rf.logs) - 1{
        //rf.commitIndex = min(len(rf.logs - 1), LeaderCommit)
        rf.commitIndex = len(rf.logs) - 1
    }
    //所有的commit成功都要给client发一次commit回复，即写一次通道
    //这似乎和那个演示动画有点不一样，但是如果不这样第二个FailAgree测试就不会通过
    for i := rf.lastApplied + 1; i <= rf.commitIndex; i++{
        rf.applyCh <- ApplyMsg{Index: i + 1, Command:rf.logs[i].Command}
    }
    rf.lastApplied = rf.commitIndex
}

func (rf *Raft) AppendEntriestoOne(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    index := -1
    term := -1
    isLeader := (rf.state == Leader)

    if isLeader{

        entry := LogEntry{
            Term: rf.currentTerm,
            Command: command,
        }
        term = rf.currentTerm
        rf.logs = append(rf.logs, entry)
        index = len(rf.logs)//nextIndex
        rf.persist()
        rf.AppendEntriestoAll()
    }


    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here.
    rf.currentTerm = 0
    rf.votedFor = -1
    rf.logs = make([]LogEntry, 0)

    rf.commitIndex = -1
    rf.lastApplied = -1

    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    rf.state = Follower
    rf.applyCh = applyCh
    rf.skip_block1_Ch = make(chan struct{})
    rf.skip_block2_Ch = make(chan struct{})
    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    rf.persist()
    rf.resetTimer()
    go rf.run()

    return rf
}
