package list

import (
	"errors"
	"fmt"
	"strconv"
	//"io"
	"strings"

	repl "github.com/brown-csci1270/db/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	//panic("function not yet implemented");
	return new(List)
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	//panic("function not yet implemented");
	return list.head
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	//panic("function not yet implemented");
	return list.tail
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	//panic("function not yet implemented");
	node := &Link{}
	node.value = value
	node.list = list
	if list.tail == nil {
		list.tail = node
	} else {
		list.head.prev = node
		node.next = list.head
	}
	list.head = node
	return node
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	//panic("function not yet implemented");
	node := &Link{}
	node.value = value
	node.list = list
	if list.head == nil {
		list.head = node
	} else {
		list.tail.next = node
		node.prev = list.tail
	}
	list.tail = node
	return node
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	//panic("function not yet implemented");
	ptr := list.head
	for ; ptr != nil; {
		if f(ptr) {
			return ptr
		}
		ptr = ptr.next
	}
	return nil
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	//panic("function not yet implemented");
	ptr := list.head
	for ; ptr != nil; {
		f(ptr)
		ptr = ptr.next
	}
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	//panic("function not yet implemented");
	return link.list
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	//panic("function not yet implemented");
	if link.value != nil {
		return link.value
	}
	return nil
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	//panic("function not yet implemented");
	link.value = value
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	//panic("function not yet implemented");
	if link.prev != nil {
		return link.prev
	}
	return nil
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	//panic("function not yet implemented");
	if link.next != nil {
		return link.next
	}
	return nil
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	//panic("function not yet implemented");
	switch {
	case link.next != nil:
		link.next.prev = link.prev
		if link.prev == nil {
			link.list.head = link.next
		} else {
			link.prev.next = link.next
		}
	case link.prev != nil:
		link.prev.next = link.next
		if link.next == nil{
			link.list.tail = link.prev
		}else {
			link.next.prev = link.prev
		}
	case link.prev == nil && link.next == nil:
		link.list.head = nil
		link.list.tail = nil
	}
}

const (
	CmdListHelp = ".help"
	CmdListContains = "list_contains"
	CmdListPrint = "list_print"
	CmdListPushHead = "list_push_head"
	CmdListPushTail = "list_push_tail"
	CmdListRemove = "list_remove"
	

	HelpListContains = "Checks if an element exists int the list, usage: list_contains <elt>"
	HelpListPrint = "Prints out the elements of the list. usage: list_print"
	HelpListPushHead = "Add an element to the head of the list. usage: list_push_head <elt>"
	HelpListPushTail = "Add an element to the tail of the list. usage: list_push_tail <elt>"
	HelpListRemove = "Remove an element with the given value from the list. usage: list_remove <elf>"

)

func (list *List) list_contains(s string, config *repl.REPLConfig) error {
	tokens := strings.Split(s, " ")
	if tokens[0] != CmdListContains || len(tokens) != 2 {
		fmt.Print("wrong command\n")
		return errors.New("wrong command")
	}
	//fmt.Println("tokens: ", tokens)
	v, err := strconv.ParseInt(tokens[1], 10, 0)
	if err != nil {
		return err
	}
	ptr := list.head
	for ; ptr != nil; {
		if ptr.value == v {
			fmt.Println("found!")
			return nil
		}
		ptr = ptr.next
	}
	fmt.Println("not found")
	return nil
}

func (list *List) list_print(s string, config *repl.REPLConfig) error {
	if s != CmdListPrint {
		fmt.Print("wrong command\n")
		return errors.New("wrong command")
	}
	ptr := list.head
	for ; ptr != nil; {
		fmt.Print(ptr.value)
		ptr = ptr.next
	}
	fmt.Println("")
	return nil
}

func (list *List) list_pushhead(s string, config *repl.REPLConfig) error {
	tokens := strings.Split(s, " ")
	if tokens[0] != CmdListPushHead || len(tokens) != 2 {
		fmt.Print("wrong command\n")
		return errors.New("wrong command")
	}
	v, err := strconv.ParseInt(tokens[1], 10, 0)
	if err != nil {
		return err
	}
	list.PushHead(v)
	return nil
}


func (list *List) list_pushtail(s string, config *repl.REPLConfig) error {
	tokens := strings.Split(s, " ")
	if tokens[0] != CmdListPushTail || len(tokens) != 2 {
		fmt.Print("wrong command\n")
		return errors.New("wrong command")
	}
	v, err := strconv.ParseInt(tokens[1], 10, 0)
	if err != nil {
		return err
	}
	list.PushTail(v)
	return nil
}

func (list *List) list_remove(s string, config *repl.REPLConfig) error {
	tokens := strings.Split(s, " ")
	if tokens[0] != CmdListRemove || len(tokens) != 2 {
		fmt.Print("wrong command\n")
		return errors.New("wrong command")
	}
	v, err := strconv.ParseInt(tokens[1], 10, 0)
	if err != nil {
		return err
	}
	ptr := list.head
	for ; ptr != nil; {
		if ptr.value == v {
			ptr.PopSelf()
			return nil
		}
		ptr = ptr.next
	}
	return nil
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	//panic("function not yet implemented listrepl");
	//fmt.Println("enter ListRepl")
	repl := repl.NewRepl()
	//list.PushHead(1)
	
	repl.AddCommand(CmdListContains, list.list_contains, HelpListContains)
	repl.AddCommand(CmdListPrint, list.list_print, HelpListPrint)
	repl.AddCommand(CmdListPushHead, list.list_pushhead, HelpListPushHead)
	repl.AddCommand(CmdListPushTail, list.list_pushtail, HelpListPushTail)
	repl.AddCommand(CmdListRemove, list.list_remove, HelpListRemove)
	return repl
	
}
