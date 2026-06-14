/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valuerpc

import "go.arpabet.com/value"

func Verify(args value.Value, def TypeDef) bool {
	if def == Any {
		return true
	}
	if def == Void {
		// BUG-2 fix: nil args cross the wire as value.Null, so Void must accept
		// Null as well as Go nil and empty collections.
		if args == nil || args.Kind() == value.NULL {
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
	if args == nil || args.Kind() == value.NULL {
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
	if args == nil || args.Kind() == value.NULL {
		return len(paramsDef.Map) == 0
	}
	if args.Kind() != value.MAP {
		return false
	}
	cache := args.(value.Map)
	for _, paramDef := range paramsDef.Map {
		val := cache.Get(paramDef.Name)
		if val == value.Null {
			// absent: only an error when the param is required
			if paramDef.Required {
				return false
			}
			continue
		}
		if !VerifyParam(val, paramDef) {
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
