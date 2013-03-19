package tally

import (
    "errors"
    "strconv"
    "strings"
)

type SampleType int
const (
    COUNTER SampleType = iota
    TIMER
)

type Sample struct {
    key string
    value float64
    valueType SampleType
    sampleRate float64
}

type Statgram []Sample

func Parse(text string) (statgram Statgram) {
    previous := ""
    lines := strings.Split(text, "\n")
    statgrams := make([]Statgram, 0, len(lines))
    total := 0
    for _, value := range(lines) {
        if len(value) > 2 && value[0] == '^' {
            prefixLength, err := strconv.ParseInt(value[1:3], 16, 0)
            if err == nil && int(prefixLength) < len(previous) {
                value = previous[:prefixLength] + value[3:]
            }
        }
        previous = value
        subsamples, _ := ParseStatgramLine(value)
        statgrams = append(statgrams, subsamples)
        total += len(subsamples)
    }
    statgram = make([]Sample, 0, total)
    for _, subsamples := range(statgrams) {
        for _, sample := range(subsamples) {
            statgram = append(statgram, sample)
        }
    }
    return
}

func ParseStatgramLine(text string) (statgram Statgram, err error) {
    parts := strings.Split(text, ":")
    if len(parts) == 0 {
        return
    }
    key := parts[0]
    statgram = make([]Sample, 0, len(parts) - 1)
    for _, part := range(parts[1:]) {
        var sample Sample
        sample, err = ParseSample(key, part)
        if err != nil {
            return
        }
        statgram = append(statgram, sample)
    }
    return
}

func ParseSample(key string, part string) (sample Sample, err error) {
    fields := strings.Split(part, "|")
    if len(fields) != 2 {
        err = errors.New("sample field should contain exactly one '|'")
        return
    }
    var value float64
    if value, err = strconv.ParseFloat(fields[0], 64); err != nil {
        return
    }
    sample = Sample{key: key, value: value, sampleRate: 1.0}
    if strings.Contains(fields[1], "@") {
        f1Parts := strings.SplitN(fields[1], "@", 2)
        fields[1] = f1Parts[0]
        sample.sampleRate, err = strconv.ParseFloat(f1Parts[1], 64)
        if err != nil {
            return
        }
    }
    if fields[1] == "ms" {
        sample.valueType = TIMER
    } else {
        sample.valueType = COUNTER
    }
    return
}
