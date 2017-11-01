package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	//"fmt"
	"fmt"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind string //"Put" or "Append" "Get"
	Key string
	Value string
	Id int64
	ReqId int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	db			map[string]string
	ack 		map[int64]int // To store transactionID, and make sure no duplicates.
	result	map[int]chan Op // Channel for communication when applyMsg occurs.
	// Your definitions here.
}

// When folowers receives the logs from leaders,
// they needs to do update DB.
func (kv *RaftKV) Apply(args Op) {
	fmt.Printf("Inside Apply me: %v key = %v, value = %v, op: %v, clientId: %v, reqId: %v\n", kv.me, args.Key,
		args.Value, args.Kind, args.Id, args.ReqId)
	kv.mu.Lock()
	switch args.Kind {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.ack[args.Id] = args.ReqId
	kv.mu.Unlock()
}


// if duplicate, return true; reqID id upcomint transaction id, and should be bigger than
// ack current id.
// otherwise, return false;
func (kv *RaftKV) ifDuplicate(id int64,reqid int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v,ok := kv.ack[id]
	if ok {
		if reqid <= v {
			return true;
		}
	}
	return false
}

func (kv *RaftKV) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		fmt.Printf("op: %v . this kv is not a leader.\n", entry)
		return false
	}

	kv.mu.Lock()
	ch,ok := kv.result[index]
	if !ok {
		ch = make(chan Op,1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op != entry {
			fmt.Printf("op and entry are different.\nd")
		}
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		fmt.Printf("timeout\n")
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.`
	reply.WrongLeader = true
	if kv.rf.IsLeader() == false {
		return;
	}

	entry := Op{Kind:"Get",Key:args.Key,Id:args.Id,ReqId:args.ReqID}
	fmt.Printf("GET api...me: %v current kv key: %v, db value = %v client_Id: %v, reqId: %v\n",
		kv.me, args.Key, kv.db, args.Id, args.ReqID)


	reply.WrongLeader = false
	if kv.ifDuplicate(args.Id, args.ReqID) == true {
		fmt.Printf("Duplicate happens\n")
	}

	ok := kv.AppendEntryToLog(entry)
	if !ok {
		fmt.Printf("Error happens when trying getting an entry.\n")
		return
	}

	kv.mu.Lock()
	reply.Value = kv.db[args.Key]
	kv.ack[args.Id] = args.ReqID
	kv.mu.Unlock()
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//kv.mu.Lock()
	reply.WrongLeader = true
	if kv.rf.IsLeader() == false {
		return;
	}

	entry := Op{Kind:args.Op,Key:args.Key,Value:args.Value,Id:args.Id,ReqId:args.ReqID}
	fmt.Printf("Inside PutAppend me: %v current kv db key = %v, value = %v, op: %v, clientId: %v, reqId: %v\n", kv.me, args.Key,
		args.Value, args.Op, args.Id, args.ReqID)

	reply.WrongLeader = false
	if kv.ifDuplicate(args.Id, args.ReqID) == true {
		fmt.Printf("Duplicate happens\n")
		return ;
	}

	ok := kv.AppendEntryToLog(entry)
	if !ok {
		fmt.Printf("Error happens when trying append an entry.\n")
		return
	}
	//kv.mu.Unlock()
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.result = make(map[int]chan Op)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg,100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int)

	// You may need initialization code here.
	go func() {
		for {
			msg := <- kv.applyCh
			op := msg.Command.(Op)
			fmt.Printf(" reveiving messages index %v command %v client: %v reqid: %v\n", msg.Index, msg.Command, op.Id, op.ReqId)

			if !kv.ifDuplicate(op.Id,op.ReqId) {
				fmt.Printf("current top ack req id: %v at server: %v\n", kv.ack[op.Id], me)
				kv.Apply(op)
			}

			ch,ok := kv.result[msg.Index]
			if ok {
				select {
				case <-kv.result[msg.Index]:
				default:
				}
				ch <- op
			} else {
				kv.result[msg.Index] = make(chan Op, 1)
			}

		}
	}()
	return kv
}
