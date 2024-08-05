// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/spf13/cobra"
)

// newCmdSafePoint creates the `cli safepoint` command.
func newCmdSafePoint(f factory.Factory) *cobra.Command {
	command := &cobra.Command{
		Use:   "safepoint",
		Short: "Manage safepoint",
	}

	command.AddCommand(newCmdQuerySafePoint(f))
	command.AddCommand(newCmdSetSafePoint(f))
	command.AddCommand(newCmdDeleteSafePoint(f))

	return command
}
