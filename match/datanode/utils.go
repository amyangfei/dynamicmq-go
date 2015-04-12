package main

import (
	"fmt"
)

func AttrNameCombine(xattr, yattr string) string {
	if xattr < yattr {
		return fmt.Sprintf("%s-%s", xattr, yattr)
	} else {
		return fmt.Sprintf("%s-%s", yattr, xattr)
	}
}
