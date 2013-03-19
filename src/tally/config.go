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

type Config struct {
    sections map[string] map[string] string
}

func NewConfig() *Config {
    return &Config{sections: make(map[string] map[string] string)}
}

func (config *Config) HasSection(section string) (present bool) {
    _, present = config.sections[section]
    return
}

func (config *Config) HasValue(section string, name string) (present bool) {
    present = config.HasSection(section)
    if present {
        _, present = config.sections[section][name]
    }
    return
}

func (config *Config) GetValue(section string, name string) (value string, err error) {
    sectionValues, present := config.sections[section]
    if !present {
        err = errors.New(fmt.Sprintf("no section %#v", section))
        return
    }
    value, present = sectionValues[name]
    if !present {
        err = errors.New(fmt.Sprintf("no option %#v in section %#v", name, section))
    }
    return
}

func (config *Config) GetInt(section string, name string) (value int, err error) {
    var strValue string
    strValue, err = config.GetValue(section, name)
    if err == nil {
        value, err = strconv.Atoi(strValue)
    }
    return
}

func (config *Config) GetBoolean(section string, name string) (result bool, err error) {
    var value string
    if value, err = config.GetValue(section, name); err != nil { return }
    switch strings.ToLower(value) {
        case "1": result = true
        case "yes": result = true
        case "true": result = true
        case "on": result = true
        case "0": result = false
        case "no": result = false
        case "false": result = false
        case "off": result = false
        default:
             err = errors.New(fmt.Sprintf("invalid boolean value: %#v", value))
    }
    return
}

func (config *Config) GetSeconds(section string, name string) (duration time.Duration, err error) {
    var seconds int
    seconds, err = config.GetInt(section, name)
    if err == nil {
        duration = time.Duration(seconds) * time.Second
    }
    return
}

func (config *Config) Parse(path string) (err error) {
    var parser *ConfigParser
    if parser, err = NewConfigParser(config, path); err != nil { return err }
    for {
        if err = parser.ReadAndParseLine(); err != nil {
            break
        }
    }
    if err == io.EOF {
        err = nil
    }
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
    if err == nil {
        parser = &ConfigParser{
            config: config,
            path: path,
            currentLine: 0,
            reader: bufio.NewReader(file),
        }
    }
    return
}

func (parser *ConfigParser) Error(msg string) error {
    return &SyntaxError{msg: msg, path: parser.path, line: parser.currentLine}
}

func (parser *ConfigParser) ReadAndParseLine() (err error) {
    parser.currentLine += 1
    buffer := new(bytes.Buffer)
    for {
        var line_bytes []byte
        var isPrefix bool
        line_bytes, isPrefix, err = parser.reader.ReadLine()
        if err != nil { return }
        buffer.Write(line_bytes)
        if !isPrefix { break }
    }
    return parser.ParseLine(buffer.String())
}

func (parser *ConfigParser) ParseLine(line string) (err error) {
    if strings.HasPrefix(line, "[") {
        line = strings.TrimSpace(line)
        if len(line) < 3 || !strings.HasSuffix(line, "]") {
            return parser.Error("invalid section header")
        }
        parser.section = line[1:len(line) - 1]
        if parser.config.HasSection(parser.section) {
            return parser.Error("section repeated: " + parser.section)
        }
        parser.config.sections[parser.section] = make(map[string] string)
    } else {
        err = parser.ParseAssignment(line)
    }
    return
}

func (parser *ConfigParser) ParseAssignment(line string) error {
    line = strings.TrimSpace(line)
    if len(line) > 0 && !strings.HasPrefix(line, "#") {
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
        if !parser.config.HasSection(parser.section) {
            parser.config.sections[parser.section] = make(map[string] string)
        }
        parser.config.sections[parser.section][name] = value
    }
    return nil
}
