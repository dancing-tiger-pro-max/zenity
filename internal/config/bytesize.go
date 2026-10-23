package config

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// ByteSize is a size written like "512MB" or "2GB" in config.yaml.
type ByteSize int64

var units = []struct {
	suffix string
	mult   int64
}{
	{"GB", 1 << 30},
	{"MB", 1 << 20},
	{"KB", 1 << 10},
	{"B", 1},
}

func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	s := strings.ToUpper(strings.TrimSpace(n.Value))
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			v, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
			if err != nil {
				return fmt.Errorf("line %d: invalid size %q", n.Line, n.Value)
			}
			*b = ByteSize(v * u.mult)
			return nil
		}
	}
	return fmt.Errorf("line %d: size %q needs a unit (KB, MB, GB)", n.Line, n.Value)
}

func (b ByteSize) String() string {
	for _, u := range units {
		if int64(b) >= u.mult && int64(b)%u.mult == 0 {
			return fmt.Sprintf("%d%s", int64(b)/u.mult, u.suffix)
		}
	}
	return fmt.Sprintf("%dB", int64(b))
}
