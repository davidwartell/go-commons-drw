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
	"fmt"
	"github.com/davidwartell/go-commons-drw"
	"github.com/davidwartell/go-logger-facade/logger"
	"github.com/pkg/errors"
	"github.com/stealthmodesoft/service"
	"os"
	"path/filepath"
	"sync"
)

type SystemService struct {
	sync.Mutex
	params           *Params
	serviceInterface service.Interface
}

type DarwinParams struct {
	StandardOutFileName     string
	StandardErrorFileName   string
	Umask                   string
	ExitTimeOut             int64
	Nice                    int64
	ProcessType             string
	LowPriorityIO           bool
	LowPriorityBackgroundIO bool
}

type WindowsParams struct {
	Dependencies []string
	UserName     string
}

type Params struct {
	ServiceCommonName        string
	SystemServiceName        string
	SystemServiceDisplayName string
	SystemServiceDescription string
	ExecutableFileName       string
	ExecutableArguments      []string
	DarwinParams             DarwinParams
	WindowsParams            WindowsParams
}

//goland:noinspection GoUnusedExportedFunction
func NewSystemService(params *Params, serviceInterface service.Interface) *SystemService {
	return &SystemService{
		params:           params,
		serviceInterface: serviceInterface,
	}
}

func (s *SystemService) Status() (status string, err error) {
	s.Lock()
	defer s.Unlock()

	status, err = s.systemServiceStatus()
	if err != nil {
		return
	}
	return
}

func (s *SystemService) Install() error {
	return s.InstallWithExtraArguments()
}

func (s *SystemService) InstallWithExtraArguments(args ...string) (err error) {
	s.Lock()
	defer s.Unlock()

	var oldArguments []string
	copy(oldArguments, s.params.ExecutableArguments)

	s.params.ExecutableArguments = append(s.params.ExecutableArguments, args...)

	var workingDir, executablePath string
	workingDir, executablePath, err = s.execPath()

	err = s.setupSystemService(workingDir, executablePath)
	s.params.ExecutableArguments = oldArguments
	return
}

func (s *SystemService) Uninstall() error {
	s.Lock()
	defer s.Unlock()

	err := s.uninstallSystemService()
	if err != nil {
		return err
	}
	return nil
}

func (s *SystemService) Start() error {
	s.Lock()
	defer s.Unlock()

	err := s.startSystemService()
	if err != nil {
		return err
	}
	return nil
}

func (s *SystemService) Stop() error {
	s.Lock()
	defer s.Unlock()

	err := s.stopSystemService()
	if err != nil {
		return err
	}
	return nil
}

func (s *SystemService) Setup() error {
	s.Lock()
	defer s.Unlock()
	var err error
	var workingDir, executablePath string
	workingDir, executablePath, err = s.execPath()
	if err != nil {
		return err
	}
	return s.setupSystemService(workingDir, executablePath)
}

func (s *SystemService) CommonName() string {
	return s.params.ServiceCommonName
}

func (s *SystemService) stopSystemService() (err error) {
	svcConfig := &service.Config{
		Name: s.params.SystemServiceName,
	}

	srvc, err := service.New(s.serviceInterface, svcConfig)
	if err != nil {
		return
	}

	var status service.Status
	status, err = srvc.Status()
	if err != nil {
		return
	}

	if status == service.StatusRunning {
		_ = srvc.Stop()
		if err != nil {
			return
		}
		logger.Instance().Info(fmt.Sprintf("%s stopped", s.params.ServiceCommonName), logger.String("service", s.params.ServiceCommonName))
	} else {
		logger.Instance().Info(fmt.Sprintf("%s already stopped", s.params.ServiceCommonName), logger.String("service", s.params.ServiceCommonName))
	}

	return
}

func (s *SystemService) uninstallSystemService() (err error) {
	svcConfig := &service.Config{
		Name: s.params.SystemServiceName,
	}

	srvc, err := service.New(s.serviceInterface, svcConfig)
	if err != nil {
		return
	}

	// on windows have to stop first or the delete does not actually take place until its stopped
	_ = srvc.Stop()
	logger.Instance().Info(fmt.Sprintf("%s stopped", s.params.ServiceCommonName), logger.String("service", s.params.ServiceCommonName))

	err = srvc.Uninstall()
	if err != nil {
		return
	}
	logger.Instance().Info(fmt.Sprintf("%s uninstalled", s.params.ServiceCommonName), logger.String("service", s.params.ServiceCommonName))
	return
}

func (s *SystemService) startSystemService() (err error) {
	svcConfig := &service.Config{
		Name: s.params.SystemServiceName,
	}

	srvc, err := service.New(s.serviceInterface, svcConfig)
	if err != nil {
		return
	}

	var status service.Status
	status, err = srvc.Status()
	if err != nil {
		return
	}

	if status == service.StatusStopped {
		_ = srvc.Start()
		if err != nil {
			return
		}
		logger.Instance().Info(fmt.Sprintf("%s started", s.params.ServiceCommonName), logger.String("service", s.params.ServiceCommonName))
	} else {
		logger.Instance().Info(fmt.Sprintf("%s already started", s.params.ServiceCommonName), logger.String("service", s.params.ServiceCommonName))
	}

	return
}

func (s *SystemService) execPath() (workingDir string, executablePath string, err error) {
	var exPath string
	exPath, err = os.Executable()
	if err != nil {
		err2 := errors.Wrapf(err, "error starting %s failed to get current working directory: %v", s.params.ServiceCommonName, err)
		err = err2
		return
	}
	workingDir = filepath.Dir(exPath)
	executablePath = filepath.Join(workingDir, s.params.ExecutableFileName)

	if !commons.FileExists(executablePath) {
		logger.Instance().Error(fmt.Sprintf(
			"error starting service %s executable file not found", s.params.ServiceCommonName),
			logger.String("service", s.params.ServiceCommonName),
			logger.String("executablePath", executablePath),
		)
		err = errors.Errorf("error starting %s file (%s) not found", s.params.ServiceCommonName, executablePath)
	}

	return
}

func (s *SystemService) systemServiceStatus() (statusStr string, err error) {
	svcConfig := &service.Config{
		Name: s.params.SystemServiceName,
	}

	var srvc service.Service
	srvc, err = service.New(s.serviceInterface, svcConfig)
	if err != nil {
		err2 := errors.Wrapf(err, "error getting status of %s", s.params.ServiceCommonName)
		err = err2
		return
	}

	var status service.Status
	status, err = srvc.Status()
	if err != nil {
		err2 := errors.Wrapf(err, "error getting status of %s", s.params.ServiceCommonName)
		err = err2
		return
	}
	statusStr = status.String()
	return
}
