package shellworker

import (
	"github.com/mjevans/go/parallel"
	"io"
	"os"
	"os/exec"
	"bytes"
)

func PipeService(w io.Writer, r io.Reader, sync chan<- int){
	ioBuf := make([]byte, 64 * 1024 * 1024)
	var ll, kk, kp int
	var errIn, errOut error
PipeServiceExit:
	for {
		ll, errIn = r.Read(ioBuf)
		kp = 0
		for (ll - kp) > 0 && nil == errOut {
			kk, errOut = w.Write(ioBuf[kp:ll])
			kp += kk
		}
		switch errIn {
		case io.EOF, os.ErrClosed:
			break PipeServiceExit
		case nil:
		default:
			panic(errIn)
		}
		switch errOut {
		case io.EOF, os.ErrClosed:
			break PipeServiceExit
		case nil:
		default:
			panic(errOut)
		}
	}
	sync <- int(0)
}


type ShellWorker struct {
	Cmd	string
	Args	[]string
	LogFile string
	ErrFile string
}

type fakeBytesBufferCloser struct {
	bytes.Buffer
}

func (this fakeBytesBufferCloser) Close() error {
	return nil
}

func (this ShellWorker) Worker(ic <-chan parallel.SyncedByte, oc chan<- parallel.SyncedByte) {
	if "" == this.Cmd {
		panic("")
	}

	nilOrPanic := func(e error){
		if nil != e {
			panic(e)
		}
	}

	var err error
	var logFile, errFile io.ReadWriteCloser
	if "" != this.LogFile {
		logFile, err = os.Create(this.LogFile)
		nilOrPanic(err)
		defer logFile.Close()
	} else {
		logFile = new(fakeBytesBufferCloser)
	}

	if "" != this.ErrFile {
		errFile, err = os.Create(this.ErrFile)
		nilOrPanic(err)
		defer errFile.Close()
	} else {
		errFile = new(fakeBytesBufferCloser)
	}

	cmd := exec.Command(this.Cmd, this.Args...)
	cIn,  err := cmd.StdinPipe()
	nilOrPanic(err)
	cOut, err := cmd.StdoutPipe()
	nilOrPanic(err)
	cErr, err := cmd.StderrPipe()
	nilOrPanic(err)
	err = cmd.Start()
	nilOrPanic(err)
	
	cSync := make(chan int)
	
	go PipeService(logFile, cOut, cSync)
	go PipeService(errFile, cErr, cSync)

	sb := parallel.SyncedByte{Buffer: nil, State: parallel.WriterBuffer}
	// Declare ready for work
	oc <- sb

ShellWorkerExit:
	for {
		sb = <-ic
		switch sb.State {
		case parallel.ReaderBuffer:
			// work
			cIn.Write(sb.Buffer)
			sb.State = parallel.WriterBuffer
			oc <- sb
		case parallel.Complete:
			cIn.Close()
			<-cSync // Wait for the two output pipes to drain.
			<-cSync
			cmd.Wait()
			temp := new(bytes.Buffer)
			if "" == this.ErrFile {
				temp.ReadFrom(errFile)
			}
			if "" == this.LogFile {
				temp.ReadFrom(logFile)
			}
			oc <- parallel.SyncedByte{
				Buffer: temp.Bytes(),
				State:  parallel.Complete}
			// Work complete, resuls delivered
			break ShellWorkerExit
		default:
			// Abort, silently
			break ShellWorkerExit
		}
	}
}
