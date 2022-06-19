/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
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
