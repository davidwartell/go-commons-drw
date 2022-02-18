/*
 * Copyright (c) 2022 by David Wartell. All Rights Reserved.
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

package profiler

import (
	"github.com/davidwartell/go-commons-drw/logger"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

type Profiler struct {
	cpuProfilingStarted    bool
	cpuMutex               sync.RWMutex
	cpuFile                *os.File
	cpuFileName            string
	memoryProfilingStarted bool
	memoryMutex            sync.RWMutex
	memoryFile             *os.File
	memoryFileName         string
}

var instance *Profiler
var profilerInstanceSync sync.Once

func Instance() *Profiler {
	profilerInstanceSync.Do(func() {
		instance = &Profiler{}
	})
	return instance
}

func (p *Profiler) StopCpuProfiling() {
	p.cpuMutex.Lock()
	defer p.cpuMutex.Unlock()
	if !p.cpuProfilingStarted {
		return
	}
	pprof.StopCPUProfile()
	err := p.cpuFile.Close()
	if err != nil {
		logger.Instance().ErrorfUnstruct("error closing cpu profiling file=%s", p.cpuFileName)
	}
	p.cpuProfilingStarted = false
	logger.Instance().InfofUnstruct("cpu profiling stopped - output=%s", p.cpuFileName)
}

func (p *Profiler) StartCpuProfiling() {
	p.cpuMutex.Lock()
	defer p.cpuMutex.Unlock()
	if p.cpuProfilingStarted {
		return
	}

	var exPath string
	var err error
	exPath, err = os.Executable()
	if err != nil {
		logger.Instance().ErrorfUnstruct("error starting cpu profiler: %s", err)
		return
	}
	workingDir := filepath.Dir(exPath)

	file, err := ioutil.TempFile(workingDir, "remotescan-cpu-"+time.Now().Format("2006-01-02T150405")+"-*.prof")
	if err != nil {
		logger.Instance().ErrorfUnstruct("error starting cpu profiler: %s", err)
		return
	}

	if err := pprof.StartCPUProfile(file); err != nil {
		logger.Instance().ErrorfUnstruct("error starting cpu profiler: %s", err)
		return
	}

	p.cpuProfilingStarted = true
	p.cpuFile = file
	p.cpuFileName = file.Name()
	logger.Instance().InfofUnstruct("cpu profiling started - output=%s", file.Name())
}

func (p *Profiler) StartMemoryProfiling() {
	p.memoryMutex.Lock()
	defer p.memoryMutex.Unlock()
	if p.memoryProfilingStarted {
		return
	}

	workingDir, err := os.Getwd()
	if err != nil {
		logger.Instance().ErrorfUnstruct("error starting memory profiler: %s", err)
		return
	}

	file, err := ioutil.TempFile(workingDir, "remotescan-memory-"+time.Now().Format("2006-01-02T150405")+"-*.prof")
	if err != nil {
		logger.Instance().ErrorfUnstruct("error starting memory profiler: %s", err)
		return
	}

	p.memoryProfilingStarted = true
	p.memoryFile = file
	p.memoryFileName = file.Name()
	logger.Instance().InfofUnstruct("memory profiling started - output=%s", file.Name())
}

func (p *Profiler) StopMemoryProfiling() {
	p.memoryMutex.Lock()
	defer p.memoryMutex.Unlock()
	if !p.memoryProfilingStarted {
		return
	}

	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(p.memoryFile); err != nil {
		logger.Instance().ErrorfUnstruct("error writing heap memory profile: %s", err)
	}
	err := p.memoryFile.Close()
	if err != nil {
		logger.Instance().ErrorfUnstruct("error closing memory profiling file=%s", p.memoryFileName)
	}
	p.memoryProfilingStarted = false
	logger.Instance().InfofUnstruct("memory profiling stopped - output=%s", p.memoryFileName)
}
