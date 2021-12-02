package repl

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
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

// REPLConfig REPL Config struct.
type REPLConfig struct {
	writer   io.Writer
	clientId uuid.UUID
}

// GetWriter Get writer.
func (replConfig *REPLConfig) GetWriter() io.Writer {
	return replConfig.writer
}

// GetAddr Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// NewRepl Construct an empty REPL.
func NewRepl() *REPL {
	r := new(REPL)
	r.help = make(map[string]string)
	r.commands = make(map[string]func(string, *REPLConfig) error)

	return r
}

// CombineRepls Combines a slice of REPLs.
func CombineRepls(repls []*REPL) (*REPL, error) {
	if repls == nil || len(repls) == 0 {
		return NewRepl(), nil
	}

	combinedRepl := NewRepl()

	var i int
	for i = 0; i < len(repls); i++ {
		for trigger := range repls[i].help {
			_, present := combinedRepl.help[trigger]
			if present {
				return nil, errors.New("overlapping triggers detected")
			}

			combinedRepl.help[trigger] = repls[i].help[trigger]
			combinedRepl.commands[trigger] = repls[i].commands[trigger]
		}
	}
	return combinedRepl, nil
}

// GetCommands Get commands.
func (r *REPL) GetCommands() map[string]func(string, *REPLConfig) error {
	return r.commands
}

// GetHelp Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// AddCommand Add a command, along with its help string, to the set of commands.
func (r *REPL) AddCommand(trigger string, action func(string, *REPLConfig) error, help string) {
	if r == nil {
		return
	}
	if strings.HasPrefix(trigger, ".") {
		fmt.Printf("Attempts to overwrite meta command is illegal!")
		return
	}
	r.commands[trigger] = action
	r.help[trigger] = help
}

// HelpString Return all REPL usage information as a string.
func (r *REPL) HelpString() string {
	if r == nil {
		return ""
	}
	usage := ""
	for tri := range r.help {
		usage += tri + ": " + r.help[tri] + "\n"
	}
	return usage
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
	scanner := bufio.NewScanner(reader)
	replConfig := &REPLConfig{writer: writer, clientId: clientId}

	// print the prompt
	fmt.Print(prompt)
	// Begin the repl loop!
	for scanner.Scan() {
		// read from the scanner
		command := cleanInput(scanner.Text())
		inputCommand := strings.Split(command, " ")

		if inputCommand[0] == ".help" {
			r.metaHelp()
		} else {
			action, present := r.commands[inputCommand[0]]
			if present {
				err := action(command, replConfig)
				if err != nil {
					log.Print(err)
				}
			}
		}
		// print the prompt
		fmt.Print(prompt)
	}
}

// RunChan Run the REPL.
func (r *REPL) RunChan(c chan string, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	writer := os.Stdout
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// Begin the repl loop!
	io.WriteString(writer, prompt)
	for payload := range c {
		// Emit the payload for debugging purposes.
		io.WriteString(writer, payload+"\n")
		// Parse the payload.
		fields := strings.Fields(payload)
		if len(fields) == 0 {
			io.WriteString(writer, prompt)
			continue
		}
		trigger := cleanInput(fields[0])
		// Check for a meta-command.
		if trigger == ".help" {
			io.WriteString(writer, r.HelpString())
			io.WriteString(writer, prompt)
			continue
		}
		// Else, check user commands.
		if command, exists := r.commands[trigger]; exists {
			// Call a hardcoded function.
			err := command(payload, replConfig)
			if err != nil {
				io.WriteString(writer, fmt.Sprintf("%v\n", err))
			}
		} else {
			io.WriteString(writer, "command not found\n")
		}
		io.WriteString(writer, prompt)
	}
	// Print an additional line if we encountered an EOF character.
	io.WriteString(writer, "\n")
}

func (r *REPL) metaHelp() {
	for trigger := range r.commands {
		fmt.Println(trigger + ": " + r.help[trigger])
	}

	return
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	text = strings.Trim(text, " ")
	splitText := strings.Split(text, " ")

	var cleanedText []string
	for _, section := range splitText {
		if len(section) > 0 {
			cleanedText = append(cleanedText, section)
		}
	}
	return strings.Join(cleanedText, " ")
}
