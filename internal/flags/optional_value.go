// Copyright 2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flags

import (
	"strconv"

	"github.com/spf13/pflag"
)

// optionalValue is a flag value that stays unset until the flag is given: it then points
// the target at the parsed value. It tells a flag left out apart from one given with its
// default, which a plain flag value cannot do.
type optionalValue[T any] struct {
	target **T
	// def is the default shown in the help: the server default that applies when the
	// flag is left out. It is never sent.
	def    T
	parse  func(string) (T, error)
	format func(T) string
	typ    string
}

var _ pflag.Value = (*optionalValue[int])(nil)

// String returns the value the flag is set to, or the default shown in the help.
func (v *optionalValue[T]) String() string {
	if *v.target != nil {
		return v.format(**v.target)
	}

	return v.format(v.def)
}

// Set parses s and points the target at the result.
func (v *optionalValue[T]) Set(s string) error {
	parsed, err := v.parse(s)
	if err != nil {
		return err
	}

	*v.target = &parsed

	return nil
}

// Type names the value type the way pflag names its own, so that the help reads the
// same as for a plain flag.
func (v *optionalValue[T]) Type() string {
	return v.typ
}

// optionalBoolVar defines a bool flag that sets *target only when it is given. As for a
// plain bool flag, giving it without a value sets it to true.
func optionalBoolVar(fs *pflag.FlagSet, target **bool, name string, def bool, usage string) {
	fs.Var(&optionalValue[bool]{
		target: target,
		def:    def,
		parse:  strconv.ParseBool,
		format: strconv.FormatBool,
		typ:    "bool",
	}, name, usage)

	fs.Lookup(name).NoOptDefVal = "true"
}

// optionalIntVar defines an int flag that sets *target only when it is given.
func optionalIntVar(fs *pflag.FlagSet, target **int, name string, def int, usage string) {
	fs.Var(&optionalValue[int]{
		target: target,
		def:    def,
		parse: func(s string) (int, error) {
			// pflag names the flag and the value in the error already.
			v, err := strconv.ParseInt(s, 0, strconv.IntSize)

			return int(v), err
		},
		format: strconv.Itoa,
		typ:    "int",
	}, name, usage)
}

// optionalFloat64Var defines a float64 flag that sets *target only when it is given.
func optionalFloat64Var(fs *pflag.FlagSet, target **float64, name string, def float64, usage string) {
	fs.Var(&optionalValue[float64]{
		target: target,
		def:    def,
		parse: func(s string) (float64, error) {
			return strconv.ParseFloat(s, 64)
		},
		format: func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) },
		typ:    "float64",
	}, name, usage)
}
