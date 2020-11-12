package idgenerator

import (
	"github.com/oklog/ulid/v2"
	"math/rand"
	"strings"
	"time"
)

//go:generate mockgen -source=id_generator.go -destination=../../mocks/id_generator_mock.go -package=mocks
type IdGenerator interface {
	Generate() string
}

type ulIdGenerator struct {
	entropy *ulid.MonotonicEntropy
}

func New() IdGenerator {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	return &ulIdGenerator{entropy: entropy}
}

func (generator *ulIdGenerator) Generate() string {
	return strings.ToLower(ulid.MustNew(ulid.Timestamp(time.Now()), generator.entropy).String())
}
