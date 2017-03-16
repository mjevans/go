package shellworker

import (
	"testing"
	"github.com/mjevans/go/parallel"
	"bytes"
	"io"
)

func Test001(T *testing.T) {
	jobs := make([]parallel.SyncedWorker, 0)
	jobs = append(jobs, ShellWorker{Cmd: "cat"})
	jobs = append(jobs, ShellWorker{Cmd: "/bin/sh", Args: []string{0: "-c", 1: "cat", 2:">&2"}})
	
	refString := "This test is basic."
	br := bytes.NewReader([]byte(refString))
	lr := io.LimitReader(br, int64(br.Len()))
	
	res := parallel.ConductorReaderWorker(lr, jobs, 1024)
	T.Logf("\n---\n%s\n---\n", string(res))
	if 0 != bytes.Compare(res, []byte(refString + ",\n" + refString)) {
		T.Errorf("Basic reference string not returned as expected, got:\n---\n%s\n---\n", res)
	}
}
