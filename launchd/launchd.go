///
// Copyright (c) 2021. StealthMode Inc. All Rights Reserved
///

package launchd

import (
	"github.com/pkg/errors"
	"howett.net/plist"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
)

var ServiceNotFoundError = errors.New("service not found")

type ServiceKeyValue map[string]string

// Service see man launchd.plist
type Service struct {
	Label                   string           `plist:"Label"`
	Disabled                bool             `plist:"Disabled"`
	UserName                bool             `plist:"UserName"`
	GroupName               bool             `plist:"GroupName"`
	Program                 string           `plist:"Program"`
	ProgramArguments        []string         `plist:"ProgramArguments"`
	EnableGlobbing          bool             `plist:"EnableGlobbing"`
	KeepAlive               bool             `plist:"KeepAlive"`
	RootDirectory           string           `plist:"RootDirectory"`
	WorkingDirectory        string           `plist:"WorkingDirectory"`
	EnvironmentVariables    ServiceKeyValue  `plist:"EnvironmentVariables"`
	Umask                   string           `plist:"Umask"`
	ExitTimeOut             int64            `plist:"ExitTimeOut"`
	StandardInPath          string           `plist:"StandardInPath"`
	StandardOutPath         string           `plist:"StandardOutPath"`
	StandardErrorPath       string           `plist:"StandardErrorPath"`
	SoftResourceLimits      map[string]int64 `plist:"SoftResourceLimits"`
	HardResourceLimits      map[string]int64 `plist:"HardResourceLimits"`
	Nice                    int64            `plist:"Nice"`
	ProcessType             string           `plist:"ProcessType"`
	AbandonProcessGroup     bool             `plist:"AbandonProcessGroup"`
	LowPriorityIO           bool             `plist:"LowPriorityIO"`
	LowPriorityBackgroundIO bool             `plist:"LowPriorityBackgroundIO"`
	LaunchOnlyOnce          bool             `plist:"LaunchOnlyOnce"`
}

func launchDaemonsDir() string {
	return filepath.Join("/", "Library", "LaunchDaemons")
}

func FindService(name string) (srvc *Service, err error) {
	launchFile := filepath.Join(launchDaemonsDir(), name+".plist")
	if _, derr := os.Stat(launchFile); os.IsNotExist(derr) {
		err = ServiceNotFoundError
		return
	}

	var plistData []byte
	plistData, err = ioutil.ReadFile(launchFile)
	if err != nil {
		err = errors.Errorf("error reading file %s: %v", launchFile, err)
		return
	}
	srvc = &Service{}
	_, err = plist.Unmarshal(plistData, srvc)
	if err != nil {
		err2 := errors.Errorf("error decoding plist file %s: %v", launchFile, err)
		err = err2
		return
	}
	return
}

func (s *Service) WritePlistToArray() (plistBytes []byte, err error) {
	return plist.MarshalIndent(s, plist.XMLFormat, "\t")
}

// Equal compares two Service structs for equality
func (s *Service) Equal(x *Service) bool {
	if s.EnvironmentVariables == nil {
		s.EnvironmentVariables = make(ServiceKeyValue)
	}
	if x.EnvironmentVariables == nil {
		s.EnvironmentVariables = make(ServiceKeyValue)
	}

	// Program & ProgramArguments require special handling because Program ends of being the first argument in
	// ProgramArguments and reading a real world launchd.plist will yield Program == "" and ProgramArguments[0] contains the Program value
	sProgram := s.Program
	if sProgram == "" && len(s.ProgramArguments) > 0 {
		sProgram = s.ProgramArguments[0]
	}
	xProgram := x.Program
	if xProgram == "" && len(x.ProgramArguments) > 0 {
		xProgram = x.ProgramArguments[0]
	}
	if sProgram != xProgram {
		return false
	}

	if s.Program != "" {
		if len(s.ProgramArguments) == 0 {
			s.ProgramArguments = []string{s.Program}
		} else if s.ProgramArguments[0] != s.Program {
			s.ProgramArguments = append(make([]string, 1), s.ProgramArguments...)
			s.ProgramArguments[0] = s.Program
		}
	} else if len(s.ProgramArguments) > 0 {
		s.Program = s.ProgramArguments[0]
	}

	if x.Program != "" {
		if len(x.ProgramArguments) == 0 {
			x.ProgramArguments = []string{x.Program}
		} else if x.ProgramArguments[0] != x.Program {
			x.ProgramArguments = append(make([]string, 1), x.ProgramArguments...)
			x.ProgramArguments[0] = x.Program
		}
	} else if len(x.ProgramArguments) > 0 {
		x.Program = x.ProgramArguments[0]
	}

	if s.Program != x.Program {
		return false
	}
	if len(s.ProgramArguments) != len(x.ProgramArguments) {
		return false
	}
	for i := range s.ProgramArguments {
		if s.ProgramArguments[i] != x.ProgramArguments[i] {
			return false
		}
	}

	// reflect.DeepEqual should work with Service because there are no pointers in the data structure
	if !reflect.DeepEqual(s, x) {
		return false
	}

	return true
}
