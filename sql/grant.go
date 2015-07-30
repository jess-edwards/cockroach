// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// Grant gives new privileges to users.
// Current status:
// - Target: DATABASE X only
// - Privileges: ALL or ony of READ, WRITE.
// TODO(marc): open questions:
// - should we allow ALL,READ? (ie: should ALL always be by itself?)
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
func (p *planner) Grant(n *parser.Grant) (planNode, error) {
	if n.Target == "" {
		return nil, errEmptyDatabaseName
	}

	// Parse privileges.
	isAll, isRead, isWrite := false, false, false
	for _, priv := range n.Privileges {
		switch strings.ToLower(priv) {
		case "all":
			isAll = true
		case "read":
			isRead = true
		case "write":
			isWrite = true
		default:
			return nil, fmt.Errorf("grant: unknown privilege %q", priv)
		}
	}

	if isAll && (isRead || isWrite) {
		return nil, fmt.Errorf("grant: privilege ALL should not be combined with others")
	}

	// Lookup the database descriptor.
	dbDesc, err := p.getDatabaseDesc(n.Target.String())
	if err != nil {
		return nil, err
	}

	// Let's just build a map of users->permissions and turn it back into two lists.
	readers := map[string]struct{}{}
	writers := map[string]struct{}{}
	for _, readUser := range dbDesc.Read {
		readers[readUser] = struct{}{}
	}
	for _, writeUser := range dbDesc.Write {
		writers[writeUser] = struct{}{}
	}

	// Now set the grantees.
	for _, grantee := range n.Grantees {
		if grantee == security.RootUser {
			return nil, fmt.Errorf("grant: cannot modify permissions for %s", security.RootUser)
		}
		if isAll || isRead {
			readers[grantee] = struct{}{}
		}
		if isAll || isWrite {
			writers[grantee] = struct{}{}
		}
	}

	// Clear the permissions and re-build from the map.
	newDesc := *dbDesc
	newDesc.Read, newDesc.Write = []string{}, []string{}
	for user := range readers {
		newDesc.Read = append(newDesc.Read, user)
	}

	for user := range writers {
		newDesc.Write = append(newDesc.Write, user)
	}

	// Now update the descriptor.
	descKey := keys.MakeDescMetadataKey(newDesc.GetID())
	if err := p.db.CPut(descKey, &newDesc, dbDesc); err != nil {
		return nil, err
	}

	return &valuesNode{}, nil
}
