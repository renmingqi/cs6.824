package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

const (
    Put            = "Put"
    Append         = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	// You'll have to add definitions here.
    Client string
    Op     string
    Opid   int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type SynArgs struct {
    Data map[string]string
}

type SynReply struct {
    Err Err
}
