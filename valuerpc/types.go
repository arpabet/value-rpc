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

package valuerpc

import "github.com/consensusdb/value"

/**
@author Alex Shvid
*/


type TypeDef interface {
	UserTypeDef()
}

type AnyDef struct {
}

func (t AnyDef) UserTypeDef() {
}

type VoidDef struct {
}

func (t VoidDef) UserTypeDef() {
}

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

var (

	Any = AnyDef{}
    Void = VoidDef{}

	Bool = Arg(value.BOOL, true)
	BoolOpt = Arg(value.BOOL, false)

	Number = Arg(value.NUMBER, true)
	NumberOpt = Arg(value.NUMBER, false)

	String = Arg(value.STRING, true)
	StringOpt = Arg(value.STRING, false)

)
