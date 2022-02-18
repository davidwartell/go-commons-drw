///
// Copyright (c) 2021. StealthMode Inc. All Rights Reserved
///

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
