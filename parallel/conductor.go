package parallel

import (
	"io"
)

// WaitGroup isn't quite what we want, the parallel processes (goroutines) need to retain their state but have completed use of the provided buffer.

type SyncUpdate uint8

const (
	Complete SyncUpdate = iota
	Error
	ReaderBuffer
	WriterBuffer
)

type SyncedByte struct {
	Buffer []byte
	State  SyncUpdate
}

type SyncedWorker interface {
	// 		Receive then Send.
	Worker(<-chan SyncedByte, chan<- SyncedByte)
	//	The worker MUST implement this contract:
	//	Emit an initial return of 'WriterBuffer' to convey that it does not presently hold* (plan to use) a copy of the buffer. (This synchronizes the state machines.)
	//	Then:
	//		Recieve ReaderBuffer // A buffer to process, ReadOnly
	//		Return  WriterBuffer // Done processing
	//	  OR
	//		Recieve Complete // No further buffers
	//		Return  Complete + any result []byte (or empty)
	//	If Error is the state passed in the buffer MUST NOT be used, any prior worker state is invalid.
}

// Returns a JSON fragment ( "ex1": [1,2,3], "ex2": "something" ... )
// It is self-generated so that worker functions may return complex nested JSON for inclusion.
// Workers MUST return either nil OR a valid 'JSON fragment' in the form of: "key":...
func ConductorReaderWorker(f io.Reader, jobs []SyncedWorker, bufsize uint64) []byte {
	b0 := make([]byte, bufsize)
	b1 := make([]byte, bufsize)
	var sbi, sbo SyncedByte // SyncedByte{Buffer: nil, State: nil}

	// Setup workers
	iLen := len(jobs)
	workers := make([]chan SyncedByte, iLen)
	results := make([]chan SyncedByte, iLen)
	for ii, val := range jobs {
		csb := make(chan SyncedByte, 1)
		csr := make(chan SyncedByte)
		workers[ii] = csb
		results[ii] = csr
		go val.Worker(csb, csr)
	}

	updateWorkers := func(sbo SyncedByte) {
		for _, r := range results {
			sbi = <-r
			if sbi.State != WriterBuffer {
				panic("fileWorker: Incorrect state returned from a worker thread, corrupted output was likely.")
			}
		}
		for _, w := range workers {
			w <- sbo
		}
	}

	bufferPass := func(b []byte) bool {
		l, err := f.Read(b)

		// nil and EOF are the only error states expected
		switch err {
		case nil, io.EOF:
		default:
			panic(err)
		}

		if l > 0 {
			sbo.State = ReaderBuffer
			if int(bufsize) == l {
				sbo.Buffer = b // Avoiding slicing is about 10% faster for my go test on my test system.
			} else {
				sbo.Buffer = b[0:l]
			}
			updateWorkers(sbo)
		}
		if io.EOF == err {
			sbo.Buffer = nil
			sbo.State = Complete
			updateWorkers(sbo)
			return false // means: trigger break
		}
		return true
	}

	for {
		if !bufferPass(b0) {
			break
		}
		if !bufferPass(b1) {
			break
		}
	}

	rets := make([]byte, 0)

	var r SyncedByte
	for ii, w := range results {
		r = <-w
		if r.State != Complete {
			panic(r.Buffer)
		}
		if ii > 0 {
			rets = append(rets, []byte(",\n")...)
		}
		rets = append(rets, r.Buffer...)
	}

	return rets
}
