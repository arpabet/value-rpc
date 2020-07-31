/*
 *
 * Copyright 2020-present Arpabet Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package valueserver

import "github.com/consensusdb/value"

/**
@author Alex Shvid
*/

type Function func(args value.Value) (value.Value, error)
type OutgoingStream func(args value.Value) (<-chan value.Value, error)
type IncomingStream func(args value.Value, inC <-chan value.Value) error
type Chat func(args value.Value, inC <-chan value.Value) (<-chan value.Value, error)

type Server interface {
	AddFunction(name string, args TypeDef, res TypeDef, cb Function) error

	// GET for client
	AddOutgoingStream(name string, args TypeDef, cb OutgoingStream) error

	// PUT for client
	AddIncomingStream(name string, args TypeDef, cb IncomingStream) error

	// Dual channel chat
	AddChat(name string, args TypeDef, cb Chat) error

	Run() error

	Close() error
}

type TypeDef interface {
	UserTypeDef()
}

type AnyDef struct {
}

func (t AnyDef) UserTypeDef() {
}

var Any = AnyDef{}

type VoidDef struct {
}

func (t VoidDef) UserTypeDef() {
}

var Void = VoidDef{}

type ArgsDef struct {
	List []ArgDef
}

func (t ArgsDef) UserTypeDef() {
}

func List(args ...ArgDef) ArgsDef {
	return ArgsDef{args}
}

type ParamsDef struct {
	Map []ParamDef
}

func (t ParamsDef) UserTypeDef() {
}

func Map(params ...ParamDef) ParamsDef {
	return ParamsDef{params}
}

type ArgDef struct {
	Kind     value.Kind
	Required bool
}

func (t ArgDef) UserTypeDef() {
}

func Arg(kind value.Kind, required bool) ArgDef {
	return ArgDef{kind, required}
}

type ParamDef struct {
	Name     string
	Kind     value.Kind
	Required bool
}

func Param(name string, kind value.Kind, required bool) ParamDef {
	return ParamDef{name, kind, required}
}
