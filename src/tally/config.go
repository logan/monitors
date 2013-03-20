package tally

import (
    "bufio"
    "bytes"
    "errors"
    "fmt"
    "io"
    "os"
    "strconv"
    "strings"
    "time"
)

type SyntaxError struct {
    msg string
    path string
    line int
}

func (e *SyntaxError) Error() string {
    return fmt.Sprintf("%s:%d: %s", e.path, e.line, e.msg)
}

// Config is a two layer map of sections of variables to values.
type Config map[string] map[string] string

// HasSection returns true if the given section has been added to the config.
func (config Config) HasSection(section string) bool {
    _, ok := config[section]
    return ok
}

// AddSection initializes a section in the config. Variables can't be set under
// a section until it's been added.
func (config Config) AddSection(section string) error {
    if _, ok := config[section]; ok {
        return errors.New(fmt.Sprintf("section %#v already exists", section))
    }
    config[section] = make(map[string] string)
    return nil
}

// HasValue returns true if the given section exists and contains a value under
// the given name.
func (config Config) HasValue(section string, name string) bool {
    _, ok := config[section]; if !ok { return false }
    _, ok = config[section][name]
    return ok
}

// GetValue fetches a value stored under the given section and name. Returns an
// error if the value (or section) doesn't exist.
func (config Config) GetValue(section string, name string) (value string, err error) {
    if sectionValues, ok := config[section]; !ok {
        err = errors.New(fmt.Sprintf("no section %#v", section))
    } else if value, ok = sectionValues[name]; !ok {
        err = errors.New(fmt.Sprintf("no option %#v in section %#v", name, section))
    }
    return
}

// GetInt is GetValue passed through strings.Atoi.
func (config Config) GetInt(section string, name string) (value int, err error) {
    strValue, err := config.GetValue(section, name)
    if err != nil { return }
    return strconv.Atoi(strValue)
}

// GetBoolean is GetValue converted to a bool. Values of "1", "yes", "true", and
// "on are true. Values of "0", "no", "false", and "off" are false. Any other
// values are an error.
func (config Config) GetBoolean(section string, name string) (result bool, err error) {
    value, err := config.GetValue(section, name)
    if err != nil { return }
    switch strings.ToLower(value) {
    case "1", "yes", "true", "on":
        result = true
    case "0", "no", "false", "off":
        result = false
    default:
        err = errors.New(fmt.Sprintf("invalid boolean value: %#v", value))
    }
    return
}

// GetSeconds is GetValue converted to a time.Duration. The implied unit for the
// value is seconds.
func (config Config) GetSeconds(section string, name string) (duration time.Duration, err error) {
    // TODO: parse as float
    seconds, err := config.GetInt(section, name)
    if err != nil { return }
    duration = time.Duration(seconds) * time.Second
    return
}

// Parse reads a config file at the given path and merges it with this config.
func (config Config) Parse(path string) (err error) {
    parser, err := NewConfigParser(&config, path)
    if err != nil { return err }
    for {
        if err = parser.ReadAndParseLine(); err != nil {
            break
        }
    }
    if err == io.EOF { return nil }
    return
}

type ConfigParser struct {
    config *Config
    path string
    currentLine int
    reader *bufio.Reader
    section string
}

func NewConfigParser(config *Config, path string) (parser *ConfigParser, err error) {
    file, err := os.Open(path)
    if err != nil { return }
    parser = &ConfigParser{
        config: config,
        path: path,
        currentLine: 0,
        reader: bufio.NewReader(file),
    }
    return
}

func (parser *ConfigParser) Error(msg string) error {
    return &SyntaxError{msg: msg, path: parser.path, line: parser.currentLine}
}

func (parser *ConfigParser) ReadAndParseLine() error {
    var buffer bytes.Buffer
    parser.currentLine += 1
    for {
        line_bytes, isPrefix, err := parser.reader.ReadLine()
        if err != nil { return err }
        buffer.Write(line_bytes)
        if !isPrefix { break }
    }
    return parser.ParseLine(buffer.String())
}

func (parser *ConfigParser) ParseLine(line string) (err error) {
    if len(line) == 0 { return }
    if line[0] == '[' {
        line = strings.TrimSpace(line)
        if len(line) < 3 || line[len(line) - 1] != ']' {
            return parser.Error("invalid section header")
        }
        parser.section = line[1:len(line) - 1]
        if parser.config.HasSection(parser.section) {
            return parser.Error("section repeated: " + parser.section)
        }
        return parser.config.AddSection(parser.section)
    }
    line = strings.TrimSpace(line)
    if len(line) > 0 && line[0] != '#' {
        return parser.ParseAssignment(line)
    }
    return nil
}

func (parser *ConfigParser) ParseAssignment(line string) error {
    if parser.section == "" {
        return parser.Error("config must begin with section header")
    }
    parts := strings.SplitN(line, "=", 2)
    if len(parts) != 2 {
        return parser.Error("assignment required")
    }
    name := strings.TrimSpace(parts[0])
    if len(name) == 0 {
        return parser.Error("name required for assignment")
    }
    value := strings.TrimSpace(parts[1])
    (*parser.config)[parser.section][name] = value
    return nil
}
