// Copyright 2025 PingCAP, Inc.
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

package schemastore

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestBuildDDLEventForNewTableDDL_CreateTableLikeBlockedTableNames(t *testing.T) {
	cases := []struct {
		name     string
		query    string
		expected []commonEvent.SchemaTableName
	}{
		{
			name:  "default schema",
			query: "CREATE TABLE `b` LIKE `a`",
			expected: []commonEvent.SchemaTableName{
				{SchemaName: "test", TableName: "a"},
			},
		},
		{
			name:  "explicit schema",
			query: "CREATE TABLE `b` LIKE `other`.`a`",
			expected: []commonEvent.SchemaTableName{
				{SchemaName: "other", TableName: "a"},
			},
		},
	}

	for _, tc := range cases {
		rawEvent := &PersistedDDLEvent{
			Type:       byte(model.ActionCreateTable),
			SchemaID:   1,
			TableID:    2,
			SchemaName: "test",
			TableName:  "b",
			Query:      tc.query,
			TableInfo:  &model.TableInfo{},
		}

		ddlEvent, ok, err := buildDDLEventForNewTableDDL(rawEvent, nil, 0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, tc.expected, ddlEvent.BlockedTableNames)
	}
}
