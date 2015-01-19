package dynamicmq

import (
	"fmt"
	"io/ioutil"
	"os"
)

func PrintVersion() {
	fmt.Println("dynamicmq-go version", Version)
}

const (
	defaultUser  = "nobody"
	defaultGroup = "nobody"
)

// Init create pid file, set working dir
func ProcessInit(dir, pidFile string) error {
	// change working dir
	if err := os.Chdir(dir); err != nil {
		return err
	}
	// create pid file
	if err := ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
		return err
	}
	return nil
}
