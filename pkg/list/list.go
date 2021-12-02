package list

import (
	"errors"
	"fmt"
	"strings"

	repl "github.com/brown-csci1270/db/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// NewList Create a new list.
func NewList() *List {
	return new(List)
}

// PeekHead Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	if list == nil {
		return nil
	}
	return list.head
}

// PeekTail Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	if list == nil {
		return nil
	}
	return list.tail
}

// PushHead Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	if list == nil {
		return nil
	}

	newNode := new(Link)

	newNode.list = list
	newNode.value = value
	newNode.next = list.head

	if list.head == nil {
		list.head = newNode
		list.tail = newNode
	} else {
		list.head.prev = newNode
		list.head = newNode
	}

	return newNode
}

// PushTail Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	if list == nil {
		return nil
	}

	newNode := new(Link)

	newNode.list = list
	newNode.value = value
	newNode.prev = list.tail

	if list.head == nil {
		list.head = newNode
		list.tail = newNode
	} else {
		list.tail.next = newNode
		list.tail = newNode
	}

	return newNode
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	if list == nil {
		return nil
	}

	var temp *Link = list.head
	for temp != nil {
		if f(temp) {
			return temp
		}

		temp = temp.next
	}
	return nil
}

// Map Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	if list == nil {
		return
	}
	var temp *Link = list.head
	for temp != nil {
		// apply the function to node
		f(temp)
		temp = temp.next
	}
}

func (list *List) printList(command string, config *repl.REPLConfig) error {
	node := list.head
	for node != nil {
		if node == list.head {
			fmt.Print(node.value)
		} else {
			fmt.Printf(",%s", node.value)
		}
		node = node.next
	}
	fmt.Print("\n")
	return nil
}

func (list *List) pushHead(command string, config *repl.REPLConfig) error {
	args := strings.Split(command, " ")
	if len(args) < 1 {
		return errors.New("invalid command")
	}

	value := args[1]

	if list.PushHead(value) == nil {
		return errors.New("push list head error")
	}
	return nil
}

func (list *List) pushTail(command string, config *repl.REPLConfig) error {
	args := strings.Split(command, " ")
	if len(args) < 1 {
		return errors.New("invalid command")
	}

	value := args[1]

	if list.PushTail(value) == nil {
		return errors.New("push list tail error")
	}
	return nil
}

func (list *List) remove(command string, config *repl.REPLConfig) error {
	args := strings.Split(command, " ")
	if len(args) < 1 {
		return errors.New("invalid command")
	}

	value := args[1]

	tempNode := new(Link)
	tempNode.SetKey(value)

	iter := list.head
	var found *Link = nil

	for iter != nil {
		if iter.isEqual(tempNode) {
			found = iter
			break
		}
		iter = iter.next
	}
	if found == nil {
		return errors.New("value not found")
	}

	found.PopSelf()
	return nil
}

func (list *List) contains(command string, config *repl.REPLConfig) error {
	args := strings.Split(command, " ")
	if len(args) < 1 {
		return errors.New("invalid command")
	}

	value := args[1]

	tempNode := new(Link)
	tempNode.SetKey(value)

	iter := list.head
	found := false
	for iter != nil {
		if iter.isEqual(tempNode) {
			found = true
			break
		}
		iter = iter.next
	}
	if found {
		fmt.Println("found!")
	} else {
		fmt.Println("not found")
	}
	return nil
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// GetList Get the list that this link is a part of.
func (link *Link) GetList() *List {
	if link == nil {
		return nil
	}
	return link.list
}

// GetKey Get the link's value.
func (link *Link) GetKey() interface{} {
	if link == nil {
		return nil
	}

	return link.value
}

// SetKey Set the link's value.
func (link *Link) SetKey(value interface{}) {
	if link == nil {
		return
	}
	link.value = value
}

// GetPrev Get the link's prev.
func (link *Link) GetPrev() *Link {
	if link == nil {
		return nil
	}

	return link.prev
}

// GetNext Get the link's next.
func (link *Link) GetNext() *Link {
	if link == nil {
		return nil
	}

	return link.next
}

// PopSelf Remove this link from its list.
func (link *Link) PopSelf() {
	if link == nil {
		return
	}

	var prev *Link = link.prev
	var next *Link = link.next
	var list *List = link.list
	if prev == nil && next == nil {
		// link is the only node in the list
		list.head = nil
		list.tail = nil
	} else if prev == nil {
		// link is the first node of its list
		next.prev = nil
		list.head = next
	} else if next == nil {
		// link is the last node of its list
		prev.next = nil
		list.tail = prev
	} else {
		prev.next = link.next
		next.prev = link.prev
	}
	// remove the link from the list
	link.prev = nil
	link.next = nil
}

func (link *Link) isEqual(other *Link) bool {
	return link.value == other.value
}

// ListRepl List REPL.
func ListRepl(list *List) *repl.REPL {
	r := repl.NewRepl()
	linkedList := NewList()

	r.AddCommand("list_print", linkedList.printList, "Prints out all of the elements in the list in order, separated by commas (e.g. \"0, 1, 2\")")
	r.AddCommand("list_push_head", linkedList.pushHead, "Inserts the given element to the List as a string.")
	r.AddCommand("list_push_tail", linkedList.pushTail, "Inserts the given element to the end of the List as a string.")
	r.AddCommand("list_remove", linkedList.remove, "Removes the given element from the list.")
	r.AddCommand("list_contains", linkedList.contains, "Prints \"found!\" if the element is in the list, prints \"not found\" otherwise.")

	return r
}
