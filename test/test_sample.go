package test

import (
	"testing"

	list "github.com/brown-csci1270/db/pkg/list"
)

func TestSample(t *testing.T) {
	l := list.NewList()
	if l.PeekHead() != nil || l.PeekTail() != nil {
		t.Fatal("bad list initialization")
	}
}