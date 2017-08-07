// +build ignore

// Run tests for all the remotes
//
// Run with go run test_all.go
package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/ncw/rclone/fs"
	_ "github.com/ncw/rclone/fs/all" // import all fs
	"github.com/ncw/rclone/fstest"
)

type remoteConfig struct {
	Name     string
	SubDir   bool
	FastList bool
}

var (
	remotes = []remoteConfig{
		{
			Name:     "TestAmazonCloudDrive:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestB2:",
			SubDir:   true,
			FastList: true,
		},
		{
			Name:     "TestCryptDrive:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestCryptSwift:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestDrive:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestDropbox:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestGoogleCloudStorage:",
			SubDir:   true,
			FastList: true,
		},
		{
			Name:     "TestHubic:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestOneDrive:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestS3:",
			SubDir:   true,
			FastList: true,
		},
		{
			Name:     "TestSftp:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestSwift:",
			SubDir:   true,
			FastList: true,
		},
		{
			Name:     "TestYandex:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestFTP:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestBox:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestQingStor:",
			SubDir:   false,
			FastList: false,
		},
		{
			Name:     "TestAzureBlob:",
			SubDir:   true,
			FastList: true,
		},
	}
	binary = "fs.test"
	// Flags
	maxTries = flag.Int("maxtries", 5, "Number of times to try each test")
	runTests = flag.String("remotes", "", "Comma separated list of remotes to test, eg 'TestSwift:,TestS3'")
	clean    = flag.Bool("clean", false, "Instead of testing, clean all left over test directories")
	runOnly  = flag.String("run-only", "", "Run only those tests matching the regexp supplied")
)

// test holds info about a running test
type test struct {
	remote      string
	subdir      bool
	cmdLine     []string
	cmdString   string
	try         int
	err         error
	output      []byte
	failedTests []string
	runFlag     string
}

// newTest creates a new test
func newTest(remote string, subdir bool, fastlist bool) *test {
	t := &test{
		remote:  remote,
		subdir:  subdir,
		cmdLine: []string{"./" + binary, "-remote", remote},
		try:     1,
	}
	if *fstest.Verbose {
		t.cmdLine = append(t.cmdLine, "-test.v")
		fs.Config.LogLevel = fs.LogLevelDebug
	}
	if *runOnly != "" {
		t.cmdLine = append(t.cmdLine, "-test.run", *runOnly)
	}
	if subdir {
		t.cmdLine = append(t.cmdLine, "-subdir")
	}
	if fastlist {
		t.cmdLine = append(t.cmdLine, "-fast-list")
	}
	t.cmdString = toShell(t.cmdLine)
	return t
}

// dumpOutput prints the error output
func (t *test) dumpOutput() {
	log.Println("------------------------------------------------------------")
	log.Printf("---- %q ----", t.cmdString)
	log.Println(string(t.output))
	log.Println("------------------------------------------------------------")
}

var failRe = regexp.MustCompile(`(?m)^--- FAIL: (Test\w*) \(`)

// findFailures looks for all the tests which failed
func (t *test) findFailures() {
	oldFailedTests := t.failedTests
	t.failedTests = nil
	for _, matches := range failRe.FindAllSubmatch(t.output, -1) {
		t.failedTests = append(t.failedTests, string(matches[1]))
	}
	if len(t.failedTests) != 0 {
		t.runFlag = "^(" + strings.Join(t.failedTests, "|") + ")$"
	} else {
		t.runFlag = ""
	}
	if t.passed() && len(t.failedTests) != 0 {
		log.Printf("%q - Expecting no errors but got: %v", t.cmdString, t.failedTests)
		t.dumpOutput()
	} else if !t.passed() && len(t.failedTests) == 0 {
		log.Printf("%q - Expecting errors but got none: %v", t.cmdString, t.failedTests)
		t.dumpOutput()
		t.failedTests = oldFailedTests
	}
}

// nextCmdLine returns the next command line
func (t *test) nextCmdLine() []string {
	cmdLine := t.cmdLine[:]
	if t.runFlag != "" {
		cmdLine = append(cmdLine, "-test.run", t.runFlag)
	}
	return cmdLine
}

// if matches then is definitely OK in the shell
var shellOK = regexp.MustCompile("^[A-Za-z0-9./_:-]+$")

// converts a argv style input into a shell command
func toShell(args []string) (result string) {
	for _, arg := range args {
		if result != "" {
			result += " "
		}
		if shellOK.MatchString(arg) {
			result += arg
		} else {
			result += "'" + arg + "'"
		}
	}
	return result
}

// trial runs a single test
func (t *test) trial() {
	cmdLine := t.nextCmdLine()
	cmdString := toShell(cmdLine)
	log.Printf("%q - Starting (try %d/%d)", cmdString, t.try, *maxTries)
	cmd := exec.Command(cmdLine[0], cmdLine[1:]...)
	start := time.Now()
	t.output, t.err = cmd.CombinedOutput()
	duration := time.Since(start)
	t.findFailures()
	if t.passed() {
		log.Printf("%q - Finished OK in %v (try %d/%d)", cmdString, duration, t.try, *maxTries)
	} else {
		log.Printf("%q - Finished ERROR in %v (try %d/%d): %v: Failed %v", cmdString, duration, t.try, *maxTries, t.err, t.failedTests)
	}
}

// cleanFs runs a single clean fs for left over directories
func (t *test) cleanFs() error {
	f, err := fs.NewFs(t.remote)
	if err != nil {
		return err
	}
	entries, err := fs.ListDirSorted(f, true, "")
	if err != nil {
		return err
	}
	return entries.ForDirError(func(dir fs.Directory) error {
		remote := dir.Remote()
		if fstest.MatchTestRemote.MatchString(remote) {
			log.Printf("Purging %s%s", t.remote, remote)
			dir, err := fs.NewFs(t.remote + remote)
			if err != nil {
				return err
			}
			return fs.Purge(dir)
		}
		return nil
	})
}

// clean runs a single clean on a fs for left over directories
func (t *test) clean() {
	log.Printf("%q - Starting clean (try %d/%d)", t.remote, t.try, *maxTries)
	start := time.Now()
	t.err = t.cleanFs()
	if t.err != nil {
		log.Printf("%q - Failed to purge %v", t.remote, t.err)
	}
	duration := time.Since(start)
	if t.passed() {
		log.Printf("%q - Finished OK in %v (try %d/%d)", t.cmdString, duration, t.try, *maxTries)
	} else {
		log.Printf("%q - Finished ERROR in %v (try %d/%d): %v", t.cmdString, duration, t.try, *maxTries, t.err)
	}
}

// passed returns true if the test passed
func (t *test) passed() bool {
	return t.err == nil
}

// run runs all the trials for this test
func (t *test) run(result chan<- *test) {
	for t.try = 1; t.try <= *maxTries; t.try++ {
		if *clean {
			if !t.subdir {
				t.clean()
			}
		} else {
			t.trial()
		}
		if t.passed() {
			break
		}
	}
	if !t.passed() {
		t.dumpOutput()
	}
	result <- t
}

// makeTestBinary makes the binary we will run
func makeTestBinary() {
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	log.Printf("Making test binary %q", binary)
	err := exec.Command("go", "test", "-c", "-o", binary).Run()
	if err != nil {
		log.Fatalf("Failed to make test binary: %v", err)
	}
	if _, err := os.Stat(binary); err != nil {
		log.Fatalf("Couldn't find test binary %q", binary)
	}
}

// removeTestBinary removes the binary made in makeTestBinary
func removeTestBinary() {
	err := os.Remove(binary) // Delete the binary when finished
	if err != nil {
		log.Printf("Error removing test binary %q: %v", binary, err)
	}
}

func main() {
	flag.Parse()
	if *runTests != "" {
		newRemotes := []remoteConfig{}
		for _, name := range strings.Split(*runTests, ",") {
			for i := range remotes {
				if remotes[i].Name == name {
					newRemotes = append(newRemotes, remotes[i])
					goto found
				}
			}
			log.Printf("Remote %q not found - inserting with default flags", name)
			newRemotes = append(newRemotes, remoteConfig{Name: name})
		found:
		}
		remotes = newRemotes
	}
	var names []string
	for _, remote := range remotes {
		names = append(names, remote.Name)
	}
	log.Printf("Testing remotes: %s", strings.Join(names, ", "))

	start := time.Now()
	if *clean {
		fs.LoadConfig()
	} else {
		makeTestBinary()
		defer removeTestBinary()
	}

	// start the tests
	results := make(chan *test, 8)
	awaiting := 0
	for _, remote := range remotes {
		for _, subdir := range []bool{false, true} {
			for _, fastlist := range []bool{false, true} {
				if (!subdir || subdir && remote.SubDir) && (!fastlist || fastlist && remote.FastList) {
					go newTest(remote.Name, subdir, fastlist).run(results)
					awaiting++
				}
			}
		}
	}

	// Wait for the tests to finish
	var failed []*test
	for ; awaiting > 0; awaiting-- {
		t := <-results
		if !t.passed() {
			failed = append(failed, t)
		}
	}
	duration := time.Since(start)

	// Summarise results
	if len(failed) == 0 {
		log.Printf("PASS: All tests finished OK in %v", duration)
	} else {
		log.Printf("FAIL: %d tests failed in %v", len(failed), duration)
		for _, t := range failed {
			log.Printf("  * %s", toShell(t.nextCmdLine()))
			log.Printf("    * Failed tests: %v", t.failedTests)
		}
		os.Exit(1)
	}
}
