package repl

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	uuid "github.com/google/uuid"
)

// REPL struct.
type REPL struct {
	commands map[string]func(string, *REPLConfig) error
	help     map[string]string
}

// REPL Config struct.
type REPLConfig struct {
	writer   io.Writer
	clientId uuid.UUID
}

const (
	CmdHelp         = ".help"
	CmdListContains = "list_contains"
	CmdListPrint    = "list_print"
	CmdListPushHead = "list_push_head"
	CmdListPushTail = "list_push_tail"
	CmdListRemove   = "list_remove"
)

// Get writer.
func (replConfig *REPLConfig) GetWriter() io.Writer {
	return replConfig.writer
}

// Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// Construct an empty REPL.
func NewRepl() *REPL {
	//panic("function not yet implemented new")
	r := &REPL{}
	r.help = make(map[string]string)
	r.commands = make(map[string]func(string, *REPLConfig) error)
	return r
}

// Combines a slice of REPLs.
func CombineRepls(repls []*REPL) (*REPL, error) {
	//panic("function not yet implemented combine")
	r := NewRepl()
	fmt.Println("len is ", len(repls))
	if repls == nil || len(repls) == 0{
		return r, errors.New("nil repls")
	}
	if len(repls) == 1 {
		r.commands = repls[0].commands
		r.help = repls[0].help
		return r, nil
	}
	
	for _, repl := range repls {
		if repl == nil {
			continue
		}
		for t, v := range repl.commands {
			_, ok := r.commands[t]
			if ok {
				return nil, errors.New("repls overlapping")
			} else {
				r.commands[t] = v
				r.help[t] = repl.help[t]
			}
		}
	}
	return r, nil
}

// Get commands.
func (r *REPL) GetCommands() map[string]func(string, *REPLConfig) error {
	return r.commands
}

// Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// Add a command, along with its help string, to the set of commands.
func (r *REPL) AddCommand(trigger string, action func(string, *REPLConfig) error, help string) {
	//panic("function not yet implemented add")
	r.help[trigger] = help
	r.commands[trigger] = action
}

// Return all REPL usage information as a string.
func (r *REPL) HelpString() string {
	//panic("function not yet implemented help")
	s := ""
	for name, desc := range r.help {
		s += (name + ": \n \t" + desc + "\n")
	}
	return s
}

// Run the REPL.
func (r *REPL) Run(c net.Conn, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	var reader io.Reader
	var writer io.Writer
	if c == nil {
		reader = os.Stdin
		writer = os.Stdout
	} else {
		reader = c
		writer = c
	}
	scanner := bufio.NewScanner((reader))
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	
	// Begin the repl loop!
	//panic("function not yet implemented run")
	
	for {
		io.WriteString(writer, prompt)
		scanned := scanner.Scan()
		if !scanned {
			return
		}
		line := cleanInput(scanner.Text())
		var err error
		tokens := strings.Split(line, " ")
		if tokens[0] == ".help" {
			io.WriteString(writer, r.HelpString())
			continue
		}
		_, ok := r.commands[tokens[0]]
		if !ok {
			io.WriteString(writer, "wrong command \n")
			continue
		}
		r.commands[tokens[0]](line, replConfig)
		
		if err != nil {
			io.WriteString(writer, ("error: " + err.Error() + "\n"))
		}
	}
	
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	//panic("function not yet implemented clean")
	return text
}
