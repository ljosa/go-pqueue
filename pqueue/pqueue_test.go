package pqueue

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
)

func ensureExist(t *testing.T, filename string) {
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			t.Fatal(filename, err)
		} else {
			t.Fatal("stat", filename)
		}
	}
}

func TestOpenQueue(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "test_open_queue_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := OpenQueue(dir)
	if err != nil {
		t.Fatal("OpenQueue", err)
	}
	if q.basedir != dir {
		t.Fatal("basedir", q.basedir, "- expected", dir)
	}
	ensureExist(t, path.Join(dir, "new"))
	ensureExist(t, path.Join(dir, "cur"))
	ensureExist(t, path.Join(dir, "cur", strconv.Itoa(os.Getpid())))
	ensureExist(t, path.Join(dir, "tmp"))
}

func TestOpenCreateSubmitTakeFinish(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "test_open_create_submit_take_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := OpenQueue(dir)

	j, err := q.CreateJob("foo")
	if err != nil {
		t.Fatal("failed to create job:", err)
	}
	if !strings.Contains(j.dir, "/tmp/") {
		t.Fatal("created job does not have /tmp/ in its dir", j)
	}

	err = j.Submit()
	if err != nil {
		t.Fatal("failed to submit job:", err)
	}
	if !strings.Contains(j.dir, "/new/") {
		t.Fatal("created job does not have /new/ in its dir", j)
	}

	j2, err := q.Take()
	if err != nil {
		t.Fatal("failed to take job:", err)
	}
	if !strings.Contains(j.dir, "/new/") {
		t.Fatal("taken job does not have /cur/ in its dir", j)
	}
	if j2.Basename != j.Basename {
		t.Fatal("didn't get the same job back", j2, j)
	}

	err = j2.Finish()
	if err != nil {
		t.Fatal("failed to finish job:", err)
	}
	if !strings.Contains(j2.dir, "/done/") {
		t.Fatal("finished job does not have /done/ in its dir", j)
	}
}

func TestTakeNone(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "test_open_create_submit_take_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := OpenQueue(dir)
	j, err := q.Take()
	if err != nil {
		t.Fatal("failed to take job:", err)
	}
	if j != nil {
		t.Fatal("got a job when there aren't any")
	}
}

func TestRescueDeadJobs(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "test_open_create_submit_take_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := OpenQueue(dir)
	smypid := strconv.Itoa(os.Getpid())
	if err := os.Mkdir(path.Join(q.basedir, "cur", "424242"), 0755); err != nil {
		t.Fatal("Mkdir", err)
	}
	if err := os.Mkdir(path.Join(q.basedir, "cur", "424242", "foo"), 0755); err != nil {
		t.Fatal("Mkdir", err)
	}
	if err := os.Mkdir(path.Join(q.basedir, "cur", smypid, "bar"), 0755); err != nil {
		t.Fatal("Mkdir", err)
	}
	q.RescueDeadJobs()
	if _, err := os.Stat(path.Join(q.basedir, "new", "foo")); os.IsNotExist(err) {
		t.Fatal("Job is not in new location")
	}
	if _, err := os.Stat(path.Join(q.basedir, "cur", "424242", "foo")); err == nil {
		t.Fatal("Job is still in old location")
	}
	if _, err := os.Stat(path.Join(q.basedir, "cur", "424242")); err == nil {
		t.Fatal("Directory of dead process is still there")
	}
	if _, err := os.Stat(path.Join(q.basedir, "cur", smypid, "bar")); os.IsNotExist(err) {
		t.Fatal("Job of still-alive process was moved")
	}
}

func TestProcessExistsFalse(t *testing.T) {
	exists, err := processExists(424242)
	if err != nil {
		t.Fatal("ProcessExists:", err)
	}
	if exists {
		t.Fatal("Didn't expect process 424242 to exist")
	}
}

func TestSignalExistingProcess(t *testing.T) {
	exists, err := processExists(os.Getpid())
	if err != nil {
		t.Fatal("ProcessExists:", err)
	}
	if !exists {
		t.Fatal("I'm not dead yet!")
	}
}

func TestSetGet(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "test_open_queue_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := OpenQueue(dir)
	if err != nil {
		t.Fatal("OpenQueue", err)
	}
	j, err := q.CreateJob("foo")
	if err != nil {
		t.Fatal("failed to create job:", err)
	}
	err = j.Set("foo", []byte("foo"))
	if err != nil {
		t.Fatal("failed to set value:", err)
	}
	bytes, err := j.Get("foo")
	if err != nil {
		t.Fatal("failed to get value:", err)
	}
	if string(bytes) != "foo" {
		t.Fatal("got the wrong value:", bytes)
	}
}
