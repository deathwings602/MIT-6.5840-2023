package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// util functions
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // "Put",  "Append", "Get", "Sync" or "Migrate"
	ShardId   int
	ConfigNum int

	// for "Put", "Append" and "Get"
	Key       string
	Value     string
	ClientId  int64
	MsgId     int64
	RequestId int64

	// for "Sync"
	Data        map[string]string
	LastApplied map[int64]int64

	// for "Migrate"
	ActiveConfig shardctrler.Config
	NewConfig    shardctrler.Config
}

type ApplyMsg struct {
	Value string
	Error Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead      int32
	persister *raft.Persister
	mck       *shardctrler.Clerk
	opChan    map[int64]chan ApplyMsg
	killCh    chan struct{}

	// persistent state
	Db               [shardctrler.NShards]map[string]string
	LastApplied      map[int64]int64
	ServingShards    map[int]struct{}
	WaitingShards    map[int][]string
	CurrentConfigNum int
}

func (kv *ShardKV) isRepeated(clientId int64, msgId int64) bool {
	if lastMsgId, ok := kv.LastApplied[clientId]; ok {
		return lastMsgId >= msgId
	}
	return false
}

func isNormalOp(op string) bool {
	return op == "Put" || op == "Append" || op == "Get"
}

func (kv *ShardKV) getData(shardId int, key string) (err Err, value string) {
	if v, ok := kv.Db[shardId][key]; ok {
		err = OK
		value = v
		return
	} else {
		err = ErrNoKey
		value = ""
		return
	}
}

func (kv *ShardKV) decodeSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.Db) != nil ||
		d.Decode(&kv.LastApplied) != nil ||
		d.Decode(&kv.ServingShards) != nil ||
		d.Decode(&kv.WaitingShards) != nil ||
		d.Decode(&kv.CurrentConfigNum) != nil {
		panic("decode snapshot failed")
	}
}

func (kv *ShardKV) doSnapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.Db) != nil ||
		e.Encode(kv.LastApplied) != nil ||
		e.Encode(kv.ServingShards) != nil ||
		e.Encode(kv.WaitingShards) != nil ||
		e.Encode(kv.CurrentConfigNum) != nil {
		panic("encode snapshot failed")
	}
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Operation: "Get",
		ShardId:   args.ShardId,
		ConfigNum: args.ConfigNum,
		Key:       args.Key,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		RequestId: nrand(),
	}
	res := kv.executeNormalOp(op)
	reply.Err = res.Error
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation: args.Op,
		ShardId:   args.ShardId,
		ConfigNum: args.ConfidNum,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		RequestId: nrand(),
	}
	res := kv.executeNormalOp(op)
	reply.Err = res.Error
}

func (kv *ShardKV) Sync(args *SyncArgs, reply *SyncReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	kv.mu.Lock()
	if args.ConfigNum > kv.CurrentConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// copy data
	reply.Data = make(map[string]string)
	for k, v := range kv.Db[args.ShardId] {
		reply.Data[k] = v
	}
	reply.LastApplied = make(map[int64]int64)
	for k, v := range kv.LastApplied {
		reply.LastApplied[k] = v
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) executeNormalOp(op Op) (res ApplyMsg) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Error = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan ApplyMsg, 1)
	kv.opChan[op.ClientId] = ch
	kv.mu.Unlock()

	select {
	case res = <-ch:
		return
	case <-time.After(500 * time.Millisecond):
		res.Error = ErrTimeout
		return
	case <-kv.killCh:
		res.Error = ErrShutDown
		return
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	notify(kv.killCh)
}

func (kv *ShardKV) Killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) pullConfig() {
	for !kv.Killed() {
		select {
		case <-kv.killCh:
			return
		default:
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if isLeader && len(kv.WaitingShards) == 0 {
				currentConfigNum := kv.CurrentConfigNum
				kv.mu.Unlock()
				newConfig := kv.mck.Query(currentConfigNum + 1)
				oldConfig := kv.mck.Query(currentConfigNum)
				if newConfig.Num == oldConfig.Num+1 {
					kv.rf.Start(Op{
						Operation:    "Migrate",
						NewConfig:    newConfig.Copy(),
						ActiveConfig: oldConfig.Copy(),
					})
				}
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applyLoop() {
	for !kv.Killed() {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				kv.decodeSnapshot(msg.Snapshot)
				continue
			}
			op := msg.Command.(Op)
			kv.mu.Lock()
			applyMsg := ApplyMsg{}
			if isNormalOp(op.Operation) {
				applyMsg = kv.applyNormalOp(op)
			} else if op.Operation == "Sync" {
				applyMsg.Error = kv.applySync(op)
			} else if op.Operation == "Migrate" {
				applyMsg.Error = kv.applyMigrate(op)
			} else {
				panic("unknown operation")
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				go kv.doSnapshot(msg.CommandIndex)
			}
			if ch, ok := kv.opChan[op.RequestId]; ok {
				ch <- applyMsg
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) applyNormalOp(op Op) (res ApplyMsg) {
	if kv.CurrentConfigNum == 0 || op.ConfigNum != kv.CurrentConfigNum {
		res.Error = ErrWrongGroup
		return
	}
	if _, ok := kv.ServingShards[op.ShardId]; !ok {
		res.Error = ErrWrongGroup
		return
	}
	if _, ok := kv.WaitingShards[op.ShardId]; ok {
		res.Error = ErrWrongGroup
		return
	}
	err, value := kv.getData(op.ShardId, op.Key)
	if op.Operation == "Get" {
		res.Error = err
		res.Value = value
		return
	}
	if !kv.isRepeated(op.ClientId, op.MsgId) {
		if op.Operation == "Put" {
			kv.Db[op.ShardId][op.Key] = op.Value
		} else if op.Operation == "Append" {
			kv.Db[op.ShardId][op.Key] = value + op.Value
		} else {
			panic("unknown operation")
		}
		kv.LastApplied[op.ClientId] = op.MsgId
	}
	res.Error = OK
	return
}

func (kv *ShardKV) applySync(op Op) Err {
	if op.ConfigNum != kv.CurrentConfigNum {
		return ErrWrongGroup
	}
	if _, ok := kv.WaitingShards[op.ShardId]; !ok {
		return ErrWrongGroup
	}
	for k, v := range op.Data {
		kv.Db[op.ShardId][k] = v
	}
	for k, v := range op.LastApplied {
		if lastMsgId, ok := kv.LastApplied[k]; ok {
			kv.LastApplied[k] = Max(lastMsgId, v)
		} else {
			kv.LastApplied[k] = v
		}
	}
	delete(kv.WaitingShards, op.ShardId)
	return OK
}

func (kv *ShardKV) applyMigrate(op Op) Err {
	if kv.CurrentConfigNum+1 != op.ConfigNum {
		return ErrWrongGroup
	}
	if len(kv.WaitingShards) > 0 {
		return ErrMigrating
	}

	newConfig := op.NewConfig
	oldConfig := op.ActiveConfig
	kv.WaitingShards = make(map[int][]string)
	kv.ServingShards = make(map[int]struct{})

	for i := 0; i < shardctrler.NShards; i++ {
		if newConfig.Shards[i] == kv.gid {
			kv.ServingShards[i] = struct{}{}
			if kv.CurrentConfigNum != 0 && oldConfig.Shards[i] != kv.gid {
				kv.WaitingShards[i] = oldConfig.Groups[oldConfig.Shards[i]]
			}
		}
	}
	kv.CurrentConfigNum = newConfig.Num
	return OK
}

func (kv *ShardKV) sendSyncRequest(servers []string, shardId int, configNum int) {
	args := SyncArgs{
		ConfigNum: configNum,
		ShardId:   shardId,
	}
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply SyncReply
		ok := srv.Call("ShardKV.Sync", &args, &reply)
		if ok && reply.Err == OK {
			kv.mu.Lock()
			if _, ok := kv.WaitingShards[shardId]; !ok || kv.CurrentConfigNum != configNum {
				kv.mu.Unlock()
				return
			}

			kv.mu.Unlock()
			replyCopy := reply.Copy()
			kv.rf.Start(Op{
				Operation:   "Sync",
				ShardId:     shardId,
				ConfigNum:   configNum,
				Data:        replyCopy.Data,
				LastApplied: replyCopy.LastApplied,
			})
			return
		}
	}
}

func (kv *ShardKV) syncLoop() {
	for !kv.Killed() {
		select {
		case <-kv.killCh:
			return
		default:
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if isLeader && len(kv.WaitingShards) > 0 {
				configNum := kv.CurrentConfigNum
				var wg sync.WaitGroup
				for shardId, servers := range kv.WaitingShards {
					wg.Add(1)
					go func(shardId int, servers []string) {
						defer wg.Done()
						kv.sendSyncRequest(servers, shardId, configNum)
					}(shardId, servers)
				}
				kv.mu.Unlock()
				wg.Wait()
			} else {
				kv.mu.Unlock()
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.dead = 0
	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.opChan = make(map[int64]chan ApplyMsg)
	kv.killCh = make(chan struct{})

	kv.Db = [shardctrler.NShards]map[string]string{}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.Db[i] = make(map[string]string)
	}
	kv.LastApplied = make(map[int64]int64)
	kv.ServingShards = make(map[int]struct{})
	kv.WaitingShards = make(map[int][]string)
	kv.CurrentConfigNum = 0

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.decodeSnapshot(snapshot)
	}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyLoop()
	go kv.pullConfig()
	go kv.syncLoop()

	return kv
}
