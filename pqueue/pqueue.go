// Package pqueue provides privitives for processing a simple
// persistent queue backed by the local filesystem. In many ways, the
// approach is similar to Maildir. Submitting jobs can be done easily
// from any language by creating a directory and moving it atomically
// into a directory.
//
// A pqueue is a directory with several subdirectories:
//
// - New files and directories are created in the `tmp` subdirectory
// before being moved atomically to elsewhere in the directory
// structure.
//
// - The `new` subdirectory contains jobs that workers should process
// (see the `Take` method).
//
// - The `cur` subdirectory contains a subdirectory for each worker
// (named by its process ID) where jobs are placed while the worker
// processes them.
//
// - When jobs fail or finish successfully, they are moved to the
// `failed` or `done` subdirectories, respectively. See the `Fail` and
// `Finish` methods.
//
// Jobs have state in the form of properties, which are really just
// files inside the job's directory. The `Get` and `Set` methods read
// these properties and set them atomically. Properties are set both
// by the process submitting a job (to specify the work that is to be
// done) and by workers (to checkpoint its progress so the job can
// continue if interrupted).
package pqueue

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"strconv"
	"syscall"
)

type Queue struct {
	basedir  string
	mycurdir string
}

type Job struct {
	Basename string
	dir      string
	q        *Queue
}

var mycur = path.Join("cur", strconv.Itoa(os.Getpid()))

// Open a pqueue. The directory `dir` must already exist. The
// subdirectories (`new`, `cur`, etc.) will be created if they are
// missing.
func OpenQueue(dir string) (*Queue, error) {
	var q Queue
	q.basedir = dir
	for _, d := range []string{"tmp", "new", "cur", "done", "failed", mycur} {
		err := ensuredir(q.basedir, d)
		if err != nil {
			return nil, err
		}
	}
	return &q, nil
}

// Create a job in the `tmp` directory of the queue. After you finish
// preparing the job with `Set`, call the `Submit` method to make the
// job available to workers.
func (q *Queue) CreateJob(prefix string) (*Job, error) {
	tmp, err := ioutil.TempDir(path.Join(q.basedir, "tmp"), prefix)
	if err != nil {
		return nil, err
	}
	var job Job
	job.Basename = path.Base(tmp)
	job.dir = tmp
	job.q = q
	return &job, nil
}

// Move a job (created by `CreateJob` in the `tmp` subdirectory) to
// the `new` subdirectory so it becomes available to workers.
func (job *Job) Submit() error {
	d := path.Join(job.q.basedir, "new", job.Basename)
	err := os.Rename(job.dir, d)
	if err != nil {
		return err
	}
	job.dir = d
	return nil
}

// Find an available job (in the `new` subdirectory) and move it to
// the `cur` subdirectory for this worker process. Returns `nil` if
// there are no available jobs.
func (q *Queue) Take() (*Job, error) {
	for {
		names, err := readdirnames(path.Join(q.basedir, "new"))
		if err != nil {
			return nil, err
		}
		if len(names) == 0 {
			return nil, nil
		}
		basename := names[rand.Intn(len(names))]
		d := path.Join(q.basedir, mycur, basename)
		err = os.Rename(path.Join(q.basedir, "new", basename), d)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			} else {
				return nil, err
			}
		}
		var job Job
		job.Basename = basename
		job.dir = d
		job.q = q
		return &job, nil
	}
}

// Move the job ot the `done` subdirectory
func (job *Job) Finish() error {
	d := path.Join(job.q.basedir, "done", job.Basename)
	err := os.Rename(job.dir, d)
	if err != nil {
		return err
	}
	job.dir = d
	return nil
}

// Move the job to the `failed` subdirectory.
func (job *Job) Fail() error {
	d := path.Join(job.q.basedir, "failed", job.Basename)
	err := os.Rename(job.dir, d)
	if err != nil {
		return err
	}
	job.dir = d
	return nil
}

func (q *Queue) getNewDir() string {
	return path.Join(q.basedir, "new")
}

func (q *Queue) getCurDir() string {
	return path.Join(q.basedir, "cur")
}

func (q *Queue) getWorkerDir(pid int) string {
	return path.Join(q.basedir, "cur", strconv.Itoa(pid))
}

// Go through the `cur` subdirectory, determine which workers are no
// longer alove, and resubmit the jobs they were processing when they
// died.
func (q *Queue) RescueDeadJobs() error {
	curdir := q.getCurDir()
	names, err := readdirnames(curdir)
	if err != nil {
		log.Println("Could not rescue dead jobs: failed to read contents of", curdir)
		return err
	}
	for _, s := range names {
		pid, err := strconv.Atoi(s)
		if err != nil {
			log.Println("Does not look like a PID:", s, "- error:", err)
			continue
		}
		exists, err := processExists(pid)
		if err != nil {
			log.Println("Kill failed for PID", pid, "- error:", err)
			continue
		}
		if !exists {
			log.Println("Process", pid, "has gone away")
			q.rescueDeadJobsFrom(pid)
			if err := os.Remove(q.getWorkerDir(pid)); err != nil {
				log.Printf("Failed to rmdir %s: %s", q.getWorkerDir(pid), err)
			}
		}
	}
	return nil
}

func (q *Queue) rescueDeadJobsFrom(pid int) {
	dir := q.getWorkerDir(pid)
	names, err := readdirnames(dir)
	if err != nil {
		log.Printf("Could not rescue dead jobs from pid %d: failed to read contents of %s\n", pid, dir)
	}
	for _, s := range names {
		if err := os.Rename(path.Join(dir, s), path.Join(q.getNewDir(), s)); err != nil {
			log.Println("Failed to reschedule job", s, "from process", pid, "- error:", err)
		} else {
			log.Println("Rescueduled job", s, "from process", pid)
		}
	}
}

// Read a property of the job. This simply reads afile inside the
// job's directory.
func (j *Job) Get(name string) ([]byte, error) {
	return ioutil.ReadFile(path.Join(j.dir, name))
}

// Set a property of the job. This simply creates a file inside the
// job's directory atomically.
func (j *Job) Set(name string, data []byte) error {
	q := j.q
	f, err := ioutil.TempFile(path.Join(q.basedir, "tmp"), name)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	fn := f.Name()
	err = f.Close()
	if err != nil {
		return err
	}
	newfn := path.Join(j.dir, name)
	if err := os.Rename(fn, newfn); err != nil {
		log.Printf("Failed to rename %s to %s: %s\n", fn, newfn)
		return err
	}
	return nil
}

func ensuredir(parts ...string) error {
	err := os.Mkdir(path.Join(parts...), 0755)
	if os.IsExist(err) {
		return nil
	}
	return err
}

func readdirnames(dir string) ([]string, error) {
	d, err := os.Open(dir)
	if err != nil {
		return []string{}, err
	}
	return d.Readdirnames(-1)
}

func processExists(pid int) (bool, error) {
	if err := syscall.Kill(pid, 0); err != nil {
		if err == syscall.ESRCH {
			return false, nil
		} else {
			log.Println("Kill failed for PID", pid, "- error:", err)
			return false, err
		}
	} else {
		return true, nil
	}
}
