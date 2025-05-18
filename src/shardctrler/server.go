package shardctrler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// util functions
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

const InvalidGID int = 0

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	replyChan map[int64]chan Config
	killCh    chan struct{}

	// persistant data
	configs     []Config // indexed by config num
	lastApplied map[int64]int64
}

type Op struct {
	// Your data here.
	Operation string
	ClientId  int64
	MsgId     int64
	ReqestId  int64

	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int
}

func (sc *ShardCtrler) isRepeated(clientId int64, msgId int64) bool {
	if lastMsgId, ok := sc.lastApplied[clientId]; ok {
		return lastMsgId >= msgId
	}
	return false
}

func (config *Config) deepCopy() ([NShards]int, map[int][]string) {
	var shards [NShards]int
	groups := make(map[int][]string)
	for gid, servers := range config.Groups {
		groups[gid] = make([]string, len(servers))
		copy(groups[gid], servers)
	}
	for i := 0; i < NShards; i++ {
		shards[i] = config.Shards[i]
	}
	return shards, groups

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Operation: "Join",
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ReqestId:  nrand(),
	}
	err, _ := sc.applyOp(op)
	reply.WrongLeader = err == ErrWrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Operation: "Leave",
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ReqestId:  nrand(),
	}
	err, _ := sc.applyOp(op)
	reply.WrongLeader = err == ErrWrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Operation: "Move",
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ReqestId:  nrand(),
	}
	err, _ := sc.applyOp(op)
	reply.WrongLeader = err == ErrWrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Operation: "Query",
		Num:       args.Num,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ReqestId:  nrand(),
	}
	err, config := sc.applyOp(op)
	reply.WrongLeader = err == ErrWrongLeader
	reply.Err = err
	reply.Config = config
}

func (sc *ShardCtrler) applyOp(op Op) (Err, Config) {
	// Your code here.
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, Config{}
	}
	sc.mu.Lock()
	ch := make(chan Config, 1)
	sc.replyChan[op.ReqestId] = ch
	sc.mu.Unlock()
	select {
	case res := <-ch:
		return OK, res
	case <-time.After(500 * time.Millisecond):
		return ErrTimeout, Config{}
	case <-sc.killCh:
		return ErrKilled, Config{}
	}
}

func (sc *ShardCtrler) applyLoop() {
	for !sc.Killed() {
		select {
		case <-sc.killCh:
			return
		case msg := <-sc.applyCh:
			if msg.SnapshotValid {
				panic("applyLoop should not receive Snapshot")
			}
			op := msg.Command.(Op)
			sc.mu.Lock()
			configNum := 0
			switch op.Operation {
			case "Join":
				sc.handleJoin(op)
			case "Leave":
				sc.handleLeave(op)
			case "Move":
				sc.handleMove(op)
			case "Query":
				if op.Num == -1 || op.Num >= len(sc.configs) {
					configNum = len(sc.configs) - 1
				} else {
					configNum = op.Num
				}
			default:
				panic("Unknown operation")
			}
			if reqId, ok := sc.lastApplied[op.ClientId]; ok {
				sc.lastApplied[op.ClientId] = Max(reqId, op.MsgId)
			} else {
				sc.lastApplied[op.ClientId] = op.MsgId
			}
			if ch, ok := sc.replyChan[op.ReqestId]; ok {
				shards, groups := sc.configs[configNum].deepCopy()
				returnedConfig := Config{
					Num:    configNum,
					Shards: shards,
					Groups: groups,
				}
				DPrintf("[Sc %v] Reply %v with config: %v\n", sc.me, op.Num, returnedConfig)
				ch <- returnedConfig
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) handleJoin(op Op) {
	if sc.isRepeated(op.ClientId, op.MsgId) {
		return
	}
	DPrintf("[Sc %v] Join, original config: %v\n", sc.me, sc.configs[len(sc.configs)-1])
	shards, groups := sc.configs[len(sc.configs)-1].deepCopy()

	// first add groups
	for gid, servers := range op.Servers {
		if _, ok := groups[gid]; ok {
			panic("Group already exists")
		}
		serversCopy := make([]string, len(servers))
		copy(serversCopy, servers)
		groups[gid] = serversCopy
	}

	// no add
	if len(groups) == len(sc.configs[len(sc.configs)-1].Groups) {
		return
	}

	originalGIDs := make([]int, 0)
	addedGIDs := make([]int, 0)
	for gid, _ := range groups {
		if _, ok := op.Servers[gid]; ok {
			addedGIDs = append(addedGIDs, gid)
		} else {
			originalGIDs = append(originalGIDs, gid)
		}
	}
	sort.Ints(originalGIDs)
	sort.Ints(addedGIDs)

	// shortcut
	if len(originalGIDs) == 0 {
		// 0 -> x
		for i := 0; i < NShards; i++ {
			shards[i] = addedGIDs[i%len(addedGIDs)]
		}

		new_config := Config{
			Num:    len(sc.configs),
			Shards: shards,
			Groups: groups,
		}
		sc.configs = append(sc.configs, new_config)
		DPrintf("[Sc %v] Join shortcut, new config: %v\n", sc.me, sc.configs)
		return
	}

	originalPartition := make(map[int][]int)
	for i := 0; i < NShards; i++ {
		gid := shards[i]
		originalPartition[gid] = append(originalPartition[gid], i)
	}

	minShardNum := NShards / len(groups)
	for _, gid := range addedGIDs {
		for i := 0; i < minShardNum; i++ {
			maxShardNum := 0
			maxGID := InvalidGID
			for _, gid2 := range originalGIDs {
				if len(originalPartition[gid2]) > maxShardNum {
					maxShardNum = len(originalPartition[gid2])
					maxGID = gid2
				}
			}
			if maxGID == InvalidGID {
				panic("Can not find max GID")
			}
			assignedShard := originalPartition[maxGID][0]
			originalPartition[maxGID] = originalPartition[maxGID][1:]
			shards[assignedShard] = gid
		}
	}

	new_config := Config{
		Num:    len(sc.configs),
		Shards: shards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, new_config)
	DPrintf("[Sc %v] Join, new config: %v\n", sc.me, sc.configs)
}

func (sc *ShardCtrler) handleLeave(op Op) {
	if sc.isRepeated(op.ClientId, op.MsgId) {
		return
	}
	shards, groups := sc.configs[len(sc.configs)-1].deepCopy()
	// first remove groups
	for _, gid := range op.GIDs {
		delete(groups, gid)
	}

	// shortcut
	if len(groups) == 0 {
		for i := 0; i < NShards; i++ {
			shards[i] = InvalidGID
		}
		new_config := Config{
			Num:    len(sc.configs),
			Shards: shards,
			Groups: groups,
		}
		sc.configs = append(sc.configs, new_config)
		return
	}

	// reassign shards
	sortedRemainGIDs := make([]int, 0)
	for gid := range groups {
		sortedRemainGIDs = append(sortedRemainGIDs, gid)
	}
	sort.Ints(sortedRemainGIDs)

	groupShardCount := make(map[int]int)
	for i := 0; i < NShards; i++ {
		gid := shards[i]
		if _, ok := groups[gid]; ok {
			groupShardCount[gid]++
		}
	}

	// assign remained shards in a round robin way
	for i := 0; i < NShards; i++ {
		if _, ok := groups[shards[i]]; ok {
			continue
		}

		minSize := NShards + 1
		minGID := InvalidGID
		for _, gid := range sortedRemainGIDs {
			if groupShardCount[gid] < minSize {
				minSize = groupShardCount[gid]
				minGID = gid
			}
		}

		if minGID == InvalidGID {
			panic("No valid GID")
		}
		shards[i] = minGID
		groupShardCount[minGID]++
	}

	new_config := Config{
		Num:    len(sc.configs),
		Shards: shards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, new_config)
}

func (sc *ShardCtrler) handleMove(op Op) {
	if sc.isRepeated(op.ClientId, op.MsgId) {
		return
	}
	shards, groups := sc.configs[len(sc.configs)-1].deepCopy()
	shards[op.Shard] = op.GID
	new_config := Config{
		Num:    len(sc.configs),
		Shards: shards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, new_config)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
	notify(sc.killCh)
}

func (sc *ShardCtrler) Killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	// Init config 0
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = InvalidGID
	}
	sc.configs[0].Num = 0

	sc.dead = 0
	sc.replyChan = make(map[int64]chan Config)
	sc.lastApplied = make(map[int64]int64)
	sc.killCh = make(chan struct{})

	go sc.applyLoop()

	return sc
}
