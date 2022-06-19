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

func Verify(args value.Value, def TypeDef) bool {
	if def == Any {
		return true
	}
	if def == Void {
		if args == nil {
			return true
		}
		switch args.Kind() {
		case value.LIST:
			list := args.(value.List)
			return list.Len() == 0
		case value.MAP:
			m := args.(value.Map)
			return m.Len() == 0
		default:
			return false
		}
	}
	if argDef, ok := def.(ArgDef); ok {
		return VerifyArg(args, argDef)
	}
	if argsDef, ok := def.(ArgsDef); ok {
		return VerifyArgs(args, argsDef)
	}
	if paramsDef, ok := def.(ParamsDef); ok {
		return VerifyParams(args, paramsDef)
	}
	return false
}

func VerifyArgs(args value.Value, argsDef ArgsDef) bool {
	if args == nil {
		return len(argsDef.List) == 0
	}
	if args.Kind() != value.LIST {
		return false
	}
	list := args.(value.List)
	if list.Len() != len(argsDef.List) {
		return false
	}
	for i, def := range argsDef.List {
		if !VerifyArg(list.GetAt(i), def) {
			return false
		}
	}
	return true
}

func VerifyParams(args value.Value, paramsDef ParamsDef) bool {
	if args == nil {
		return len(paramsDef.Map) == 0
	}
	if args.Kind() != value.MAP {
		return false
	}
	cache := args.(value.Map)
	for _, paramDef := range paramsDef.Map {
		if val, ok := cache.Get(paramDef.Name); ok {
			if !VerifyParam(val, paramDef) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func VerifyArg(arg value.Value, def ArgDef) bool {
	if arg == nil {
		return !def.Required
	}
	return arg.Kind() == def.Kind
}

func VerifyParam(value value.Value, def ParamDef) bool {
	if value == nil {
		return !def.Required
	}
	return value.Kind() == def.Kind
}
