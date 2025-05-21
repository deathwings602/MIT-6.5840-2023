package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"

	ErrTimeout   = "ErrTimeout"
	ErrShutDown  = "ErrShutDown"
	ErrMigrating = "ErrMigrating"
)

type Err string

type SyncArgs struct {
	ConfigNum int
	ShardId   int
}

type SyncReply struct {
	Err         Err
	Data        map[string]string
	LastApplied map[int64]int64
}

func (reply *SyncReply) Copy() SyncReply {
	data := make(map[string]string)
	for k, v := range reply.Data {
		data[k] = v
	}
	lastApplied := make(map[int64]int64)
	for k, v := range reply.LastApplied {
		lastApplied[k] = v
	}
	return SyncReply{
		Err:         reply.Err,
		Data:        data,
		LastApplied: lastApplied,
	}
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	MsgId     int64
	ShardId   int
	ConfidNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	MsgId     int64
	ShardId   int
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

const Debug = false

func DPrint(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
