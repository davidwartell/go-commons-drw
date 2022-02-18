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

package systemservice

import (
	"github.com/davidwartell/go-commons-drw/launchd"
	"github.com/davidwartell/go-commons-drw/logger"
	"github.com/google/go-cmp/cmp"
	"github.com/stealthmodesoft/service"
	"path/filepath"
)

func (s *SystemService) setupSystemService(workingDir string, execPath string) (err error) {
	var launchdService *launchd.Service
	launchdService, err = launchd.FindService(s.params.SystemServiceName)
	if err != nil && err != launchd.ServiceNotFoundError {
		return err
	}
	if err != nil && err == launchd.ServiceNotFoundError {
		// service does not exist
		logger.Instance().InfofUnstruct("%s service does not exist - creating", s.params.ServiceCommonName)
		return s.installSystemService(workingDir, execPath)
	} else {
		// service exists see if it has correct properties
		logger.Instance().InfofUnstruct("%s service found with executable %s", s.params.ServiceCommonName, launchdService.Program)
		desiredPlist := s.newLaunchdPlist(workingDir, execPath)
		if !desiredPlist.Equal(launchdService) {
			logger.Instance().InfofUnstruct("%s service found with different configuration: %s", s.params.ServiceCommonName, cmp.Diff(launchdService, desiredPlist))
			logger.Instance().InfofUnstruct("updating %s service", s.params.ServiceCommonName)
			err = s.uninstallSystemService()
			if err != nil {
				logger.Instance().ErrorUnstruct(err)
			}
			err = s.installSystemService(workingDir, execPath)
			if err != nil {
				logger.Instance().ErrorUnstruct(err)
			}
		} else {
			logger.Instance().InfofUnstruct("%s service found with desired configuration - making sure started", s.params.ServiceCommonName)
			err = s.startSystemService()
			if err != nil {
				return
			}
		}
	}
	return
}

func (s *SystemService) newLaunchdPlist(workingDir string, execPath string) *launchd.Service {
	softLimits := make(map[string]int64)
	softLimits["NumberOfFiles"] = 65536

	hardLimits := make(map[string]int64)
	hardLimits["NumberOfFiles"] = 65536

	programArgs := []string{execPath}
	programArgs = append(programArgs, s.params.ExecutableArguments...)

	launchdPlist := &launchd.Service{
		Label:                   s.params.SystemServiceName,
		Disabled:                false,
		Program:                 execPath,
		ProgramArguments:        programArgs,
		KeepAlive:               true,
		WorkingDirectory:        workingDir,
		Umask:                   s.params.DarwinParams.Umask,
		ExitTimeOut:             s.params.DarwinParams.ExitTimeOut,
		SoftResourceLimits:      softLimits,
		HardResourceLimits:      hardLimits,
		Nice:                    s.params.DarwinParams.Nice,
		ProcessType:             s.params.DarwinParams.ProcessType,
		LowPriorityIO:           s.params.DarwinParams.LowPriorityIO,
		LowPriorityBackgroundIO: s.params.DarwinParams.LowPriorityBackgroundIO,
		StandardOutPath:         filepath.Join(workingDir, s.params.DarwinParams.StandardOutFileName),
		StandardErrorPath:       filepath.Join(workingDir, s.params.DarwinParams.StandardErrorFileName),
	}

	return launchdPlist
}

func (s *SystemService) installSystemService(workingDir string, execPath string) (err error) {
	launchdPlist := s.newLaunchdPlist(workingDir, execPath)

	svcConfig := &service.Config{
		Name: s.params.SystemServiceName,
	}

	svcConfig.DarwinLaunchdPlist, err = launchdPlist.WritePlistToArray()
	if err != nil {
		return
	}

	srvc, err := service.New(s.serviceInterface, svcConfig)
	if err != nil {
		return
	}

	err = srvc.Install()
	if err != nil {
		return
	}
	logger.Instance().InfofUnstruct("%s installed", s.params.ServiceCommonName)

	err = srvc.Start()
	if err != nil {
		return
	}
	logger.Instance().InfofUnstruct("%s started", s.params.ServiceCommonName)

	return
}
