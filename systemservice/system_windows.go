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
	"github.com/google/go-cmp/cmp"
	"github.com/stealthmodesoft/service"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc/mgr"
	"reflect"
	"strings"
)

func (s *SystemService) setupSystemService(workingDir string, execPath string) (err error) {
	//time.Sleep(20 * time.Second)

	var serviceManager *mgr.Mgr
	serviceManager, err = mgr.Connect()
	if err != nil {
		return err
	}

	var existingService *mgr.Service
	existingService, err = serviceManager.OpenService(s.params.SystemServiceName)
	if err == nil {
		// service exists
		var existingWindowsServiceConfig mgr.Config
		existingWindowsServiceConfig, err = existingService.Config()
		if err != nil {
			logger.Instance().ErrorUnstruct(err)
			errClose := serviceManager.Disconnect()
			if errClose != nil {
				logger.Instance().ErrorUnstruct(errClose)
			}
			return
		} else {
			errClose := existingService.Close()
			if errClose != nil {
				logger.Instance().ErrorUnstruct(errClose)
			}
			errClose = serviceManager.Disconnect()
			if errClose != nil {
				logger.Instance().ErrorUnstruct(errClose)
			}
		}

		desiredWindowsServiceConfig := s.serviceConfigToWindowsServiceConfig(s.newServiceConfig(workingDir, execPath))
		logger.Instance().InfofUnstruct("%s service found with executable %s", s.params.ServiceCommonName, existingWindowsServiceConfig.BinaryPathName)

		if !reflect.DeepEqual(existingWindowsServiceConfig, desiredWindowsServiceConfig) {
			logger.Instance().InfofUnstruct("%s service found with different configuration: %s", s.params.ServiceCommonName, cmp.Diff(existingWindowsServiceConfig, desiredWindowsServiceConfig))
			logger.Instance().InfofUnstruct("updating %s service", s.params.ServiceCommonName)
			err = s.uninstallSystemService()
			if err != nil {
				logger.Instance().ErrorUnstruct(err)
			}
			err = s.installSystemService(workingDir, execPath)
			if err != nil {
				logger.Instance().ErrorUnstruct(err)
			}
			return
		} else {
			logger.Instance().InfofUnstruct("%s service found with desired configuration - making sure started", s.params.ServiceCommonName)
			err = s.startSystemService()
			if err != nil {
				return
			}
			return
		}
	} else {
		// service does not exists
		errClose := serviceManager.Disconnect()
		if errClose != nil {
			logger.Instance().ErrorUnstruct(errClose)
		}

		logger.Instance().InfofUnstruct("%s service does not exist - creating", s.params.ServiceCommonName)
		err = s.installSystemService(workingDir, execPath)
		return
	}
}

func (s *SystemService) serviceConfigToWindowsServiceConfig(svcConfig *service.Config) (mgrConfig mgr.Config) {
	var binaryPathName strings.Builder
	binaryPathName.WriteString(svcConfig.Executable)
	for _, arg := range svcConfig.Arguments {
		binaryPathName.WriteString(" ")
		binaryPathName.WriteString(arg)
	}

	return mgr.Config{
		BinaryPathName:   binaryPathName.String(),
		DisplayName:      svcConfig.DisplayName,
		Description:      svcConfig.Description,
		StartType:        uint32(mgr.StartAutomatic),
		ServiceStartName: svcConfig.UserName,
		Password:         "",
		Dependencies:     svcConfig.Dependencies,
		DelayedAutoStart: false,
		ServiceType:      uint32(windows.SERVICE_WIN32_OWN_PROCESS),
	}
}

func (s *SystemService) newServiceConfig(workingDir string, execPath string) (svcConfig *service.Config) {
	programArgs := []string{execPath}
	programArgs = append(programArgs, s.params.ExecutableArguments...)
	svcConfig = &service.Config{
		Name:             s.params.SystemServiceName,
		DisplayName:      s.params.SystemServiceDisplayName,
		Description:      s.params.SystemServiceDescription,
		Arguments:        programArgs,
		Executable:       execPath,
		WorkingDirectory: workingDir,
		Dependencies:     s.params.WindowsParams.Dependencies,
		UserName:         s.params.WindowsParams.UserName,
	}
	return
}

func (s *SystemService) installSystemService(workingDir string, execPath string) (err error) {
	svcConfig := s.newServiceConfig(workingDir, execPath)

	var srvc service.Service
	srvc, err = service.New(s.serviceInterface, svcConfig)
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
