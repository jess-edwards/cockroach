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
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

// Update updates columns for a selection of rows from a table.
// TODO(vivek): Update columns that are a part of an index.
func (p *planner) Update(n *parser.Update) (planNode, error) {
	tableDesc, err := p.getAliasedTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	// Determine which columns we're inserting into.
	var names parser.QualifiedNames
	for _, expr := range n.Exprs {
		names = append(names, expr.Name)
	}
	cols, err := p.processColumns(tableDesc, names)
	if err != nil {
		return nil, err
	}

	// TODO(vivek): Avoid going through Select
	node, err := p.Select(&parser.Select{
		Exprs: parser.SelectExprs{
			&parser.StarExpr{TableName: parser.Name(tableDesc.Name)},
		},
		From:  parser.TableExprs{n.Table},
		Where: n.Where,
	})
	if err != nil {
		return nil, err
	}

	// Create a map of all the columns
	colMap := map[uint32]int{}
	for i, name := range node.Columns() {
		c, err := tableDesc.FindColumnByName(name)
		if err != nil {
			return nil, err
		}
		colMap[c.ID] = i
	}

	index := tableDesc.Indexes[0]
	indexKey := encodeIndexKeyPrefix(tableDesc.ID, index.ID)

	b := client.Batch{}

	for node.Next() {
		if err := node.Err(); err != nil {
			return nil, err
		}

		// TODO(tamird/pmattis): update the secondary indexes too
		primaryKey, err := encodeIndexKey(index, colMap, node.Values(), indexKey)
		if err != nil {
			return nil, err
		}
		// Update the columns
		for i, expr := range n.Exprs {
			key := encodeColumnKey(cols[i], primaryKey)
			val, err := parser.EvalExpr(expr.Expr, nil)
			if err != nil {
				return nil, err
			}
			if log.V(2) {
				log.Infof("Put %q -> %v", key, val)
			}

			// TODO(pmattis): Need to convert the value type to the column type.
			switch t := val.(type) {
			case parser.DBool:
				b.Put(key, bool(t))
			case parser.DInt:
				b.Put(key, int64(t))
			case parser.DFloat:
				b.Put(key, float64(t))
			case parser.DString:
				b.Put(key, string(t))
			default:
				return nil, fmt.Errorf("Unsupported type: %t", t)
			}
		}
	}

	if err := p.db.Run(&b); err != nil {
		return nil, err
	}

	// TODO(tamird/pmattis): return the number of affected rows
	return &valuesNode{}, nil
}
