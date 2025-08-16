package token

import (
	"maps"
	"math/rand/v2"
)

// todo: should be u128 to reduce chances of collision (i think)
type Value uint64

func Zero() Value {
	return Value(0)
}

func Rand() Value {
	return Value(rand.Uint64())
}

func (t Value) Merge(other Value) Value {
	return Value(t ^ other)
}

// Split a token into `count` tokens such that those tokens, when merged,
// will result in the original value.
func (v Value) Split(count int) []Value {
	if count <= 0 {
		panic("must split into at least one token")
	}
	var values []Value
	for range count - 1 {
		nv := Rand()
		v = v.Merge(nv)
		values = append(values, nv)
	}
	// We've merged all the other tokens together to get a value that when merged with those tokens will cause the result to be the original token.
	// (eg. imagine if t was 0 - we xor a bunch of tokens, then that xor itself will be zero)
	values = append(values, v)
	return values
}

func (t Value) IsZero() bool {
	return t == 0
}

func (t Value) Equal(other Value) bool {
	return t == other
}

// Example: We have an array of transcript sentences and want to batch them into groups of five.
// - We create a token for each sentence.
// - We are going to send one message per batch of sentences using a "scatter" stage.
// - That batch will have five tokens - the ID will be the ID of the sentence, and the value
//   will be the split portion of the original sentence token. (We split each sentence token
//   into the number of batches that sentence will appear in.)
// - The batch processing stage will emit messages each of which is related to zero or more
//   of the sentence tokens.
// - The "gather" stage will group each message's data by its token id, with a map from token
//   id to slice of data. When the a token is complete (ie. value zero), we send that group.
//   The same message might have multiple token IDs, so will get grouped into multiple lists.
// - For an unknown number of tokens, we can send an initial message mixing in an additional
//   token value as a "starter", and then send a "completion" message with nil data but that
//   same starter as its token value. This will cause the token result to finally become zero.

// Map from token id to token value
type Tokens map[string]Value

// Returns a new Tokens representing `other` merged into `t`.
// Go's zero value initialization means this works for keys
// in `other` that were not present in `t`.
func (t Tokens) Merge(other Tokens) Tokens {
	new := make(Tokens)
	maps.Copy(new, t)
	for k, v := range other {
		new[k] = new[k].Merge(v)
	}
	return new
}
