package tally

import "testing"

func AssertSyntaxError(t *testing.T, err error) {
    if err == nil {
        t.Error("expected syntax error")
    }
}

func (config *Config) AssertEmpty(t *testing.T) {
	if len(config.sections) != 0 {
		t.Error("expected config to remain empty")
	}
}

func (config *Config) AssertAssigned(t *testing.T, section string, name string,
	    expected string) {
	if value := config.sections[section][name]; value != expected {
		t.Errorf("expected %s.%s == %#v, got %#v", section, name, expected,
			value)
	}
}

func (config *Config) AssertHasSection(t *testing.T, section string,
        expected bool) {
    if expected != config.HasSection(section) {
        t.Errorf("expected HasSection(%#v) to be %#v", section, expected)
    }
}

func (config *Config) AssertHasValue(t *testing.T, section string, name string,
        expected bool) {
    if expected != config.HasValue(section, name) {
        t.Errorf("expected HasValue(%#v, %#v) to be %#v", section, name,
                expected)
    }
}

func (config *Config) AssertGetValue(t *testing.T, section string, name string,
        expected string) {
    if value, _ := config.GetValue(section, name); expected != value {
        t.Errorf("expected GetValue(%#v, %#v) to be %#v, got %#v", section,
                name, expected, value)
    }
}

func TestHasSection(t *testing.T) {
    config := NewConfig()
    config.AssertHasSection(t, "test", false)
    config.sections["test"] = make(map[string] string)
    config.AssertHasSection(t, "test", true)
}

func TestHasValue(t *testing.T) {
    config := NewConfig()
    config.AssertHasValue(t, "test", "x", false)
    config.sections["test"] = make(map[string] string)
    config.AssertHasValue(t, "test", "x", false)
    config.sections["test"]["x"] = "1"
    config.AssertHasValue(t, "test", "x", true)
}

func TestGetValue(t *testing.T) {
    config := NewConfig()
    config.AssertGetValue(t, "test", "x", "")
    config.sections["test"] = make(map[string] string)
    config.sections["test"]["x"] = "1"
    config.AssertGetValue(t, "test", "x", "1")
}

func (parser *ConfigParser) AssertParseAssignment(t *testing.T, line string) {
	if err := parser.ParseAssignment(line); err != nil {
		t.Error(err)
	}
}

func (parser *ConfigParser) AssertParseLine(t *testing.T, line string) {
    if err := parser.ParseLine(line); err != nil {
        t.Error(err)
    }
}

func (parser *ConfigParser) AssertSection(t *testing.T, expected string) {
    if expected != parser.section {
        t.Errorf("expected section == %#v, got %#v", expected, parser.section)
    }
}

func (parser *ConfigParser) AssertEmpty(t *testing.T) {
    parser.config.AssertEmpty(t)
}

func (parser *ConfigParser) AssertAssigned(t *testing.T, section string,
        name string, expected string) {
    parser.config.AssertAssigned(t, section, name, expected)
}

func TestParseAssignment(t *testing.T) {
    parser := ConfigParser{
        config: NewConfig(),
    }
    parser.AssertParseAssignment(t, "")
    parser.AssertParseAssignment(t, "    ")
    AssertSyntaxError(t, parser.ParseAssignment("x=1"))
	parser.AssertEmpty(t)

    parser.section = "test"
    AssertSyntaxError(t, parser.ParseAssignment("=1"))
	parser.AssertParseAssignment(t, "x=1")
	parser.AssertAssigned(t, "test", "x", "1")
	parser.AssertParseAssignment(t, "   x =  2 = 3   ")
	parser.AssertAssigned(t, "test", "x", "2 = 3")
	parser.AssertParseAssignment(t, "y =     ")
	parser.AssertAssigned(t, "test", "y", "")

}

func TestParseLine(t *testing.T) {
    parser := ConfigParser{
        config: NewConfig(),
    }

    parser.AssertParseLine(t, "")
    parser.AssertParseLine(t, "   ")
    parser.AssertParseLine(t, "   # this is a comment   ")
    parser.AssertEmpty(t)
    parser.AssertSection(t, "")

    parser.AssertParseLine(t, "[test]   ")
    parser.AssertSection(t, "test")

    AssertSyntaxError(t, parser.ParseLine("[test]"))
    AssertSyntaxError(t, parser.ParseLine("[]"))
    AssertSyntaxError(t, parser.ParseLine("[hmm"))

    parser.AssertParseLine(t, "x=1")
	parser.AssertAssigned(t, "test", "x", "1")
}

