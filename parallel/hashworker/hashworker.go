// Producing a series of hashings / producing sums to check is often an IO bound process.  The logical solution is thus to read once, and perform the operations on multiple cores in parallel, allowing the work to be sharded and processed.

// This implicitly describes a single writer, multiple reader process.  Better, double-buffer: write (read from storage) to one buffer while other threads read from the other, then switch.
package hashworker

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	// "crypto/sha3" //
	"golang.org/x/crypto/sha3"
	"hash"
	"io"
	"log"
	"os"
	"github.com/mjevans/go/parallel"
)

// HashWorker wraps a generic has implementation with a worker method
// :: hw := HashWorker{hash: sha1.New(), name: "sha1"}
type HashWorker struct {
	hash hash.Hash
	name string // I think if this didn't have the string I could just decorate hash.Hash with the worker method (function)...
}

func (this HashWorker) Worker(ic <-chan parallel.SyncedByte, oc chan<- parallel.SyncedByte) {
	if nil == this.hash {
		panic("HashWorker was not initiated with a valid hash, unable to continue.")
	}
	sb := parallel.SyncedByte{Buffer: nil, State: parallel.WriterBuffer}
	// Declare ready for owrk
	oc <- sb
	// init hash State
	// this.Reset() // Unnecessary, this is in the constructor.  Left as a reminder.
	ll := 0

HashWorkerExit:
	for {
		sb = <-ic
		switch sb.State {
		case parallel.ReaderBuffer:
			// work
			ll += len(sb.Buffer)
			this.hash.Write(sb.Buffer)
			sb.State = parallel.WriterBuffer
			oc <- sb
		case parallel.Complete:
			oc <- parallel.SyncedByte{
				Buffer: []byte("\n\"" + this.name + "\":\"" + hex.EncodeToString(this.hash.Sum(nil)) + "\""),
				State:  parallel.Complete}
			// Work complete, resuls delivered
			break HashWorkerExit
		default:
			// Abort, silently
			break HashWorkerExit
		}
	}
}

// HashSegsWorker is similar to HashWorker, but computes an array of hashes for each segment of rollover size
// :: hsw = HashSegsWorker{hash: sha1.New(), rollover: 4*1024*1024, name: "sha1B2"}
type HashSegsWorker struct {
	hash     hash.Hash
	rollover uint64
	name     string
}

func (this HashSegsWorker) Worker(ic <-chan parallel.SyncedByte, oc chan<- parallel.SyncedByte) {
	if nil == this.hash || 0 == len(this.name) || 0 == this.rollover {
		panic("HashSegsWorker was not initiated completely with a valid hash or name or rollover value,  unable to continue.")
	}
	sb := parallel.SyncedByte{Buffer: nil, State: parallel.WriterBuffer}
	// Declare ready for owrk
	oc <- sb
	var ll, lb, lseg uint64 = 0, 0, 0
	hashSegs := map[uint64]string{}
	// this.Reset() // Unnecessary, this is in the constructor.  Left as a reminder.

HashSegsWorkerExit:
	for {
		sb = <-ic
		switch sb.State {
		case parallel.ReaderBuffer:
			// work
			lb = uint64(len(sb.Buffer))
			// Note: This //completely// does not handle the case of rollover being smaller than bufsize!
			if ((ll + lb) / this.rollover) == (ll / this.rollover) {
				this.hash.Write(sb.Buffer)
			} else {
				remainder := this.rollover - (ll % this.rollover)
				this.hash.Write(sb.Buffer[0:remainder])
				hashSegs[lseg] = hex.EncodeToString(this.hash.Sum(nil))
				lseg++
				this.hash.Reset()
				this.hash.Write(sb.Buffer[remainder:])
			}
			ll += lb
			sb.State = parallel.WriterBuffer
			oc <- sb
		case parallel.Complete:
			// prepare string for return
			hashSegs[lseg] = hex.EncodeToString(this.hash.Sum(nil))

			// Note: https://github.com/golang/go/issues/18990  There still isn't a good way of doing this:
			// My own mostly uninformed 2 cents are that somehow telling the compiler the allocated []byte will /eventually/ be frozen in to a string; otherwise anything could grab a reference to the bytes and discard or disregard any channels/syncs/guards...

			rs := make([]byte, 2+len(this.name)+2+2+(int(lseg)+1)*(this.hash.Size()*2+3))
			rsp := copy(rs, []byte("\n\""+this.name+"\":"))
			rsp += copy(rs[rsp:], []byte("[\n\""))
			rsp += copy(rs[rsp:], hashSegs[0])
			rsp += copy(rs[rsp:], []byte("\""))
			for ii := uint64(1); ii <= lseg; ii++ {
				rsp += copy(rs[rsp:], []byte(",\""))
				rsp += copy(rs[rsp:], hashSegs[ii])
				rsp += copy(rs[rsp:], []byte("\""))
			}
			_ = copy(rs[rsp:], []byte("]\n"))

			oc <- parallel.SyncedByte{
				Buffer: rs, // See above note
				State:  parallel.Complete}
			// Work complete, resuls delivered
			break HashSegsWorkerExit
		default:
			// Abort, silently
			break HashSegsWorkerExit
		}
	}
}


// Notes:
// Amazon S3 == 5GB (5...0? or 5 binary?) Max; MD5 integrity optional
// Backblaze B2 == 5GB (5...0 decimal) Max; SHA1 required.
// Google Cloud Platform == ?? ; Looks like MD5 as well.
func SumReader(f io.Reader, rollover uint64) []byte {
	//jobs = append(jobs, extra...)
	// I've made a /guess/ about the runtime required for each thread, and sorted them in the /HOPE/ that golang will wake the low indexed goroutines first...
	// If I blocked the writer thread, handed back a buffered channel wakeup to it, and then continued I think I could FORCE sync... but I'm not sure that would be better, and it sounds WAY more complex.
	jobs := make([]parallel.SyncedWorker, 0)
	jobs = append(jobs, HashWorker{hash: sha3.New512(), name: "sha3-512"})
	jobs = append(jobs, HashWorker{hash: sha3.New256(), name: "sha3-256"})
	jobs = append(jobs, HashWorker{hash: sha256.New(), name: "sha256"})
	jobs = append(jobs, HashSegsWorker{hash: sha1.New(), rollover: rollover, name: "sha1segs"})
	jobs = append(jobs, HashWorker{hash: sha1.New(), name: "sha1"})
	jobs = append(jobs, HashWorker{hash: md5.New(), name: "md5"})
	jobs = append(jobs, HashSegsWorker{hash: md5.New(), rollover: rollover, name: "md5segs"})
	//const bufsize = 32 * 1024

	return parallel.ConductorReaderWorker(f, jobs, 32*1024)
}

func Sum(fname string, rollover uint64) []byte {
	f, err := os.Open(fname)
	if err != nil {
		log.Fatal(err)
		//panic(e)
		return []byte{}
	}
	defer f.Close()

	return SumReader(f, rollover)
}
