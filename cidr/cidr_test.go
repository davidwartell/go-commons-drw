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

package cidr

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

func TestCIDRFirstAndLast(t *testing.T) {
	_, ipNet, err := net.ParseCIDR("192.168.1.1/24")
	require.Nil(t, err)
	first, last := AddressRange(ipNet)
	assert.Equal(t, "192.168.1.0", first.String())
	assert.Equal(t, "192.168.1.255", last.String())

	_, ipNet, err = net.ParseCIDR("192.168.1.255/24")
	require.Nil(t, err)
	first, last = AddressRange(ipNet)
	assert.Equal(t, "192.168.1.0", first.String())
	assert.Equal(t, "192.168.1.255", last.String())

	_, ipNet, err = net.ParseCIDR("192.168.2.32/22")
	require.Nil(t, err)
	first, last = AddressRange(ipNet)
	assert.Equal(t, "192.168.0.0", first.String())
	assert.Equal(t, "192.168.3.255", last.String())

	_, ipNet, err = net.ParseCIDR("fe80:6::cb3:9253:5864:450c/64")
	require.Nil(t, err)
	first, last = AddressRange(ipNet)
	assert.Equal(t, "fe80:6::", first.String())
	assert.Equal(t, "fe80:6::ffff:ffff:ffff:ffff", last.String())
}
