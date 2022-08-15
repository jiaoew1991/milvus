// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package paramtable

import (
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/config"
)

type ParamItem struct {
	Key          string // which should be named as "A.B.C"
	Version      string
	Doc          string
	DefaultValue string
	Refreshable  bool
	PanicIfEmpty bool

	valueGuard sync.RWMutex
	value      string
	deleted    bool // if value deleted, return DefaultValue

	GetFunc func(originValue string) string
}

func (pi *ParamItem) Init(manager *config.Manager) {
	v, err := manager.GetConfig(pi.Key)
	if err == nil {
		pi.updateValue(v, false)
	} else if pi.PanicIfEmpty {
		panic(err)
	} else {
		pi.updateValue("", true)
	}
	if pi.Refreshable {
		manager.Dispatcher.Register(pi.Key, pi)
	}
}

// Get original value with error
func (pi *ParamItem) GetValue() (string, error) {
	pi.valueGuard.RLock()
	defer pi.valueGuard.RUnlock()
	var ret string
	var err error
	// if item marked as deleted then return DefaultValue
	if pi.deleted {
		ret = pi.DefaultValue
		err = errors.New("Param not found " + pi.Key)
	} else {
		ret = pi.value
		err = nil
	}
	if pi.GetFunc == nil {
		return ret, err
	}
	return pi.GetFunc(ret), err
}

func (pi *ParamItem) GetAsString() string {
	v, _ := pi.GetValue()
	return v
}

func (pi *ParamItem) GetAsStrings() []string {
	return getAndConvert(pi, func(value string) ([]string, error) {
		return strings.Split(value, ","), nil
	}, []string{})
}

func (pi *ParamItem) GetAsBool() bool {
	return getAndConvert(pi, strconv.ParseBool, false)
}

func (pi *ParamItem) GetAsInt() int {
	return getAndConvert(pi, strconv.Atoi, 0)
}

func (pi *ParamItem) OnEvent(e *config.Event) {
	switch e.EventType {
	case config.CreateType, config.UpdateType:
		pi.updateValue(e.Value, false)
	case config.DeleteType:
		pi.updateValue("", true)
	}
}

func (pi *ParamItem) updateValue(value string, deleted bool) {
	pi.valueGuard.Lock()
	defer pi.valueGuard.Unlock()
	pi.value = value
	pi.deleted = deleted
}

func (r *ParamItem) GetName() string {
	return "Default"
}

type CompositeParamItem struct {
	Items  []*ParamItem
	Format func(map[string]string) string
}

func (cpi *CompositeParamItem) GetValue() string {
	kvs := make(map[string]string, len(cpi.Items))
	for _, v := range cpi.Items {
		kvs[v.Key] = v.GetAsString()
	}
	return cpi.Format(kvs)
}

func getAndConvert[T any](pi *ParamItem, converter func(input string) (T, error), defaultValue T) T {
	v, _ := pi.GetValue()
	t, err := converter(v)
	if err != nil {
		return defaultValue
	}
	return t
}
