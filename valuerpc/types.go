/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valuerpc

import "go.arpabet.com/value"


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
