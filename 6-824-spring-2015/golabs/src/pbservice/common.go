package pbservice

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrBackupFailed = "ErrBackupFailed"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string
	// You'll have to add definitions here.
	Id       int64
	IsBackup bool
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type TransferArgs struct {
	KeyValueMap        map[string]string
	IdToGetReply       map[int64]GetReply
	IdToPutAppendReply map[int64]PutAppendReply
}

type TransferReply struct {
	Err Err
}
