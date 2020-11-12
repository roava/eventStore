package idgenerator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUlIdGenerator_Generate(t *testing.T) {
	idGen := New()
	id := idGen.Generate()
	assert.NotEmpty(t, id)
	assert.Equal(t, len(id), 26)
}
