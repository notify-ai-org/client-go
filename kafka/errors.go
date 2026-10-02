package kafka

import "github.com/twmb/franz-go/pkg/kerr"

func kerrFor(code int16) error { return kerr.ErrorForCode(code) }
