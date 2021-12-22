// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlserver

import (
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type spannerData struct {
	table string
	cols  []string
	vals  []interface{}
}

// Basic smoke test of ProcessDataRow. The core part of this code path
// (ConvertData) is tested in TestConvertData.
func TestProcessDataRow(t *testing.T) {
	tableName := "testtable"
	cols := []string{"a", "b", "c"}
	conv := buildConv(
		ddl.CreateTable{
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]ddl.ColumnDef{
				"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Float64}},
				"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}},
				"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			}},
		schema.Table{
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]schema.Column{
				"a": schema.Column{Name: "a", Type: schema.Type{Name: "float"}},
				"b": schema.Column{Name: "b", Type: schema.Type{Name: "int"}},
				"c": schema.Column{Name: "c", Type: schema.Type{Name: "text"}},
			}})
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(func(table string, cols []string, vals []interface{}) {
		rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
	})
	ProcessDataRow(conv, tableName, cols, conv.SrcSchema[tableName], tableName, cols, conv.SpSchema[tableName], []string{"4.2", "6", "prisoner zero"})
	assert.Equal(t, []spannerData{spannerData{table: tableName, cols: cols, vals: []interface{}{float64(4.2), int64(6), "prisoner zero"}}}, rows)
}

func TestConvertData(t *testing.T) {
	singleColTests := []struct {
		name  string
		ty    ddl.Type
		srcTy string      // Source DB type (Used by e.g. timestamp conversions).
		in    string      // Input value for conversion.
		e     interface{} // Expected result.
	}{
		{"bit 0", ddl.Type{Name: ddl.Bool}, "", "0", false},
		{"bit 1", ddl.Type{Name: ddl.Bool}, "", "1", true},
		{"bytes", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, "", string([]byte{137, 80}), []byte{0x89, 0x50}}, // need some other approach to testblob type
		{"date", ddl.Type{Name: ddl.Date}, "", "2019-10-29", getDate("2019-10-29")},
		{"float64", ddl.Type{Name: ddl.Float64}, "", "42.6", float64(42.6)},
		{"int64", ddl.Type{Name: ddl.Int64}, "", "42", int64(42)},
		{"string", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, "", "eh", "eh"},
		{"datetime", ddl.Type{Name: ddl.Timestamp}, "datetime", "2019-10-29 05:30:00", getTimeWithoutTimezone(t, "2019-10-29 05:30:00")},
	}
	tableName := "testtable"
	for _, tc := range singleColTests {
		col := "a"
		conv := buildConv(
			ddl.CreateTable{
				Name:     tableName,
				ColNames: []string{col},
				ColDefs:  map[string]ddl.ColumnDef{col: ddl.ColumnDef{Name: col, T: tc.ty, NotNull: false}},
				Pks:      []ddl.IndexKey{}},
			schema.Table{Name: tableName, ColNames: []string{col}, ColDefs: map[string]schema.Column{col: schema.Column{Type: schema.Type{Name: tc.srcTy}}}})
		conv.TimezoneOffset = "+05:30"
		t.Run(tc.in, func(t *testing.T) {
			at, ac, av, err := ConvertData(conv, tableName, []string{col}, conv.SrcSchema[tableName], tableName, []string{col}, conv.SpSchema[tableName], []string{tc.in})
			checkResults(t, at, ac, av, err, tableName, []string{col}, []interface{}{tc.e}, tc.name)
		})
	}
}

func buildConv(spTable ddl.CreateTable, srcTable schema.Table) *internal.Conv {
	conv := internal.MakeConv()
	conv.SpSchema[spTable.Name] = spTable
	conv.SrcSchema[srcTable.Name] = srcTable
	return conv
}

func getTime(t *testing.T, s string) time.Time {
	x, err := time.Parse(time.RFC3339, s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}

func checkResults(t *testing.T, atable string, acols []string, avals []interface{}, err error, etable string, ecols []string, evals []interface{}, name string) {
	assert.Nil(t, err, name)
	assert.Equal(t, atable, etable, name+": table mismatch")
	assert.Equal(t, ecols, acols, name+": column mismatch")
	assert.Equal(t, evals, avals, name+": value mismatch")
}

func getDate(s string) civil.Date {
	d, _ := civil.ParseDate(s)
	return d
}

func getTimeWithoutTimezone(t *testing.T, s string) time.Time {
	x, err := time.Parse("2006-01-02 15:04:05", s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}
