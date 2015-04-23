package dynamicmq

import (
	"fmt"
	"github.com/op/go-logging"
	"hash"
	"io/ioutil"
	"os"
	"regexp"
)

var LogLevelMap = map[string]logging.Level{
	"CRITICAL": logging.CRITICAL,
	"ERROR":    logging.ERROR,
	"WARNING":  logging.WARNING,
	"NOTICE":   logging.NOTICE,
	"INFO":     logging.INFO,
	"DEBUG":    logging.DEBUG,
}

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

// extract clientid and attribute name of subscription
// e.g. extract 5528c41448a90c1c73000015 and zcoord from
// "/sub/attr/5528c41448a90c1c73000015/zcoord"
func ExtractInfoFromSubKey(subkey string) (string, string) {
	regStr := fmt.Sprintf("^%s$", GetSubAttrKey("([0-9a-f]{24})", "([\\S]+)"))
	regex := regexp.MustCompile(regStr)

	match := regex.FindStringSubmatch(subkey)
	if len(match) != 3 {
		return "", ""
	} else {
		return match[1], match[2]
	}
}

// extract clientid and attribute name
// the attribute name could be empty if the client delete all its subscription
// attribute from etcd directly.
func ExtractInfoFromDelKey(delkey string) (string, string) {
	regStr1 := fmt.Sprintf("^%s$", GetSubAttrKey("([0-9a-f]{24})", "([\\S]+)"))
	regex1 := regexp.MustCompile(regStr1)

	regStr2 := fmt.Sprintf("^%s$", GetSubAttrCliBase("([0-9a-f]{24})"))
	regex2 := regexp.MustCompile(regStr2)

	match := regex1.FindStringSubmatch(delkey)
	if len(match) != 3 {
		match2 := regex2.FindStringSubmatch(delkey)
		if len(match2) != 2 {
			return "", ""
		} else {
			return match2[1], ""
		}
	} else {
		return match[1], match[2]
	}
}

func GenHash(origin []byte, hashfunc func() hash.Hash) []byte {
	hasher := hashfunc()
	hasher.Write(origin)
	return hasher.Sum(nil)
}
