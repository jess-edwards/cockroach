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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

// Delete deletes rows from a table.
func (p *planner) Delete(n *parser.Delete) (planNode, error) {
	tableDesc, err := p.getAliasedTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	// TODO(tamird,pmattis): avoid going through Select to avoid encoding
	// and decoding keys. Also, avoiding Select may provide more
	// convenient access to index keys which we are not currently
	// deleting.
	node, err := p.Select(&parser.Select{
		Exprs: parser.SelectExprs{
			&parser.StarExpr{TableName: &parser.QualifiedName{Base: parser.Name(tableDesc.Name)}},
		},
		From:  parser.TableExprs{n.Table},
		Where: n.Where,
	})
	if err != nil {
		return nil, err
	}

	colMap := map[uint32]int{}
	for i, name := range node.Columns() {
		c, err := tableDesc.FindColumnByName(name)
		if err != nil {
			return nil, err
		}
		colMap[c.ID] = i
	}

	primaryIndex := tableDesc.Indexes[0]
	primaryIndexKeyPrefix := encodeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	b := client.Batch{}

	for node.Next() {
		if err := node.Err(); err != nil {
			return nil, err
		}
		values := node.Values()

		primaryIndexKeySuffix, err := encodeIndexKey(primaryIndex, colMap, values, nil)
		if err != nil {
			return nil, err
		}
		primaryIndexKey := bytes.Join([][]byte{primaryIndexKeyPrefix, primaryIndexKeySuffix}, nil)

		// Delete the secondary indexes.
		secondaryIndexEntries, err := encodeSecondaryIndexes(tableDesc, colMap, values, primaryIndexKeySuffix)
		if err != nil {
			return nil, err
		}

		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("Del %q", secondaryIndexEntry.key)
			}
			b.Del(secondaryIndexEntry.key)
		}

		// Delete the row.
		rowStartKey := proto.Key(primaryIndexKey)
		rowEndKey := rowStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %q - %q", rowStartKey, rowEndKey)
		}
		b.DelRange(rowStartKey, rowEndKey)
	}

	if err := p.db.Run(&b); err != nil {
		return nil, err
	}

	// TODO(tamird/pmattis): return the number of affected rows
	return &valuesNode{}, nil
}
