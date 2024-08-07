// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/compression"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestLargeMessageHandle4Compression(t *testing.T) {
	t.Parallel()

	largeMessageHandle := NewDefaultLargeMessageHandleConfig()

	// unsupported compression, return error
	largeMessageHandle.LargeMessageHandleCompression = "zstd"

	err := largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, false)
	require.ErrorIs(t, err, cerror.ErrInvalidReplicaConfig)

	largeMessageHandle.LargeMessageHandleCompression = compression.LZ4
	err = largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, false)
	require.NoError(t, err)

	largeMessageHandle.LargeMessageHandleCompression = compression.Snappy
	err = largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, false)
	require.NoError(t, err)

	largeMessageHandle.LargeMessageHandleCompression = compression.None
	err = largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, false)
	require.NoError(t, err)
}

func TestLargeMessageHandle4NotSupportedProtocol(t *testing.T) {
	t.Parallel()

	largeMessageHandle := NewDefaultLargeMessageHandleConfig()

	err := largeMessageHandle.AdjustAndValidate(ProtocolCanal, true)
	require.NoError(t, err)

	largeMessageHandle.LargeMessageHandleOption = LargeMessageHandleOptionHandleKeyOnly
	err = largeMessageHandle.AdjustAndValidate(ProtocolCanal, true)
	require.ErrorIs(t, err, cerror.ErrInvalidReplicaConfig)
}

func TestHandleKeyOnly4CanalJSON(t *testing.T) {
	t.Parallel()

	// large-message-handle not set, always no error
	largeMessageHandle := NewDefaultLargeMessageHandleConfig()

	err := largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, false)
	require.NoError(t, err)
	require.True(t, largeMessageHandle.Disabled())

	largeMessageHandle.LargeMessageHandleOption = LargeMessageHandleOptionHandleKeyOnly

	// `enable-tidb-extension` is false, return error
	err = largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, false)
	require.ErrorIs(t, err, cerror.ErrInvalidReplicaConfig)

	// `enable-tidb-extension` is true, no error
	err = largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, true)
	require.NoError(t, err)
	require.Equal(t, LargeMessageHandleOptionHandleKeyOnly, largeMessageHandle.LargeMessageHandleOption)
}

func TestClaimCheck4CanalJSON(t *testing.T) {
	t.Parallel()

	// large-message-handle not set, always no error
	largeMessageHandle := NewDefaultLargeMessageHandleConfig()

	err := largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, false)
	require.NoError(t, err)
	require.True(t, largeMessageHandle.Disabled())

	largeMessageHandle.LargeMessageHandleOption = LargeMessageHandleOptionClaimCheck
	largeMessageHandle.ClaimCheckStorageURI = "file:///tmp/claim-check"

	for _, rawValue := range []bool{false, true} {
		largeMessageHandle.ClaimCheckRawValue = rawValue
		// `enable-tidb-extension` is false, return error
		err = largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, false)
		require.ErrorIs(t, err, cerror.ErrInvalidReplicaConfig)

		// `enable-tidb-extension` is true, no error
		err = largeMessageHandle.AdjustAndValidate(ProtocolCanalJSON, true)
		require.NoError(t, err)
		require.Equal(t, LargeMessageHandleOptionClaimCheck, largeMessageHandle.LargeMessageHandleOption)
	}
}

func TestHandleKeyOnly4OpenProtocol(t *testing.T) {
	t.Parallel()

	// large-message-handle not set, always no error
	largeMessageHandle := NewDefaultLargeMessageHandleConfig()

	err := largeMessageHandle.AdjustAndValidate(ProtocolOpen, false)
	require.NoError(t, err)
	require.True(t, largeMessageHandle.Disabled())

	largeMessageHandle.LargeMessageHandleOption = LargeMessageHandleOptionHandleKeyOnly
	// `enable-tidb-extension` is false, return error
	err = largeMessageHandle.AdjustAndValidate(ProtocolOpen, false)
	require.NoError(t, err)

	// `enable-tidb-extension` is true, no error
	err = largeMessageHandle.AdjustAndValidate(ProtocolOpen, true)
	require.NoError(t, err)
	require.Equal(t, LargeMessageHandleOptionHandleKeyOnly, largeMessageHandle.LargeMessageHandleOption)
}

func TestClaimCheck4OpenProtocol(t *testing.T) {
	t.Parallel()

	// large-message-handle not set, always no error
	largeMessageHandle := NewDefaultLargeMessageHandleConfig()

	err := largeMessageHandle.AdjustAndValidate(ProtocolOpen, false)
	require.NoError(t, err)
	require.True(t, largeMessageHandle.Disabled())

	largeMessageHandle.LargeMessageHandleOption = LargeMessageHandleOptionClaimCheck
	largeMessageHandle.ClaimCheckStorageURI = "file:///tmp/claim-check"

	// `enable-tidb-extension` is false, return error
	err = largeMessageHandle.AdjustAndValidate(ProtocolOpen, false)
	require.NoError(t, err)

	// `enable-tidb-extension` is true, no error
	err = largeMessageHandle.AdjustAndValidate(ProtocolOpen, true)
	require.NoError(t, err)
	require.Equal(t, LargeMessageHandleOptionClaimCheck, largeMessageHandle.LargeMessageHandleOption)

	largeMessageHandle.ClaimCheckRawValue = true
	err = largeMessageHandle.AdjustAndValidate(ProtocolOpen, true)
	require.ErrorIs(t, err, cerror.ErrInvalidReplicaConfig)
}

func TestHandleKeyOnly4SimpleProtocol(t *testing.T) {
	t.Parallel()

	// large-message-handle not set, always no error
	largeMessageHandle := NewDefaultLargeMessageHandleConfig()

	err := largeMessageHandle.AdjustAndValidate(ProtocolSimple, false)
	require.NoError(t, err)
	require.True(t, largeMessageHandle.Disabled())

	largeMessageHandle.LargeMessageHandleOption = LargeMessageHandleOptionHandleKeyOnly
	// `enable-tidb-extension` is false, return error
	err = largeMessageHandle.AdjustAndValidate(ProtocolSimple, false)
	require.NoError(t, err)

	// `enable-tidb-extension` is true, no error
	err = largeMessageHandle.AdjustAndValidate(ProtocolSimple, true)
	require.NoError(t, err)
	require.Equal(t, LargeMessageHandleOptionHandleKeyOnly, largeMessageHandle.LargeMessageHandleOption)
}

func TestClaimCheck4SimpleProtocol(t *testing.T) {
	t.Parallel()

	// large-message-handle not set, always no error
	largeMessageHandle := NewDefaultLargeMessageHandleConfig()

	err := largeMessageHandle.AdjustAndValidate(ProtocolSimple, false)
	require.NoError(t, err)
	require.True(t, largeMessageHandle.Disabled())

	largeMessageHandle.LargeMessageHandleOption = LargeMessageHandleOptionClaimCheck
	largeMessageHandle.ClaimCheckStorageURI = "file:///tmp/claim-check"

	// `enable-tidb-extension` is false, return error
	err = largeMessageHandle.AdjustAndValidate(ProtocolSimple, false)
	require.NoError(t, err)

	// `enable-tidb-extension` is true, no error
	err = largeMessageHandle.AdjustAndValidate(ProtocolSimple, true)
	require.NoError(t, err)
	require.Equal(t, LargeMessageHandleOptionClaimCheck, largeMessageHandle.LargeMessageHandleOption)

	largeMessageHandle.ClaimCheckRawValue = true
	err = largeMessageHandle.AdjustAndValidate(ProtocolSimple, true)
	require.NoError(t, err)
}
