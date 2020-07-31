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
