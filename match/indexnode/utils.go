package main

import ()

func genHash(origin []byte) []byte {
	hasher := Config.HashFunc()
	hasher.Write(origin)
	return hasher.Sum(nil)
}
