// Main executable for bumblebase.
package main

import (
	//"flag"
	//"fmt"
	
	config "github.com/brown-csci1270/db/pkg/config"
	list "github.com/brown-csci1270/db/pkg/list"
	//repl "github.com/brown-csci1270/db/pkg/repl"
	uuid "github.com/google/uuid"
)

// Start the database.
func main() {
	//fmt.Println("test list")
	myList := list.NewList()
	listrepl := list.ListRepl(myList)
	listrepl.Run(nil, uuid.New(), config.GetPrompt(true))
	//fmt.Println("End")
	//panic("function not yet implemented main")
}
