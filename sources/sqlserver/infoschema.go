// Copyright 2020 Google LLC
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

// INFOSCHEMA interface implementation for sql server.
// took refrence from postgress
// some method is not convert for sql server those method marked as
// [ps**]

package sqlserver

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type InfoSchemaImpl struct {
	DbName string
	Db     *sql.DB
}

// GetToDdl function below implement the common.InfoSchema interface.
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// GetTableName returns table name.
func (isi InfoSchemaImpl) GetTableName(schema string, tableName string) string {
	if schema == "public" { // Drop 'public' prefix.
		return tableName
	}
	return fmt.Sprintf("%s.%s", schema, tableName)
}

//[ps*]
// ProcessDataRows performs data conversion for source database
// 'db'. For each table, we extract data using a "SELECT *" query,
// convert the data to Spanner data (based on the source and Spanner
// schemas), and write it to Spanner.  If we can't get/process data
// for a table, we skip that table and process the remaining tables.
//
// Note that the database/sql library has a somewhat complex model for
// returning data from rows.Scan. Scalar values can be returned using
// the native value used by the underlying driver (by passing
// *interface{} to rows.Scan), or they can be converted to specific go
// types. Array values are always returned as []byte, a string
// encoding of the array values. This string encoding is
// database/driver specific. For example, for PostgreSQL, array values
// are returned in the form "{v1,v2,..,vn}", where each v1,v2,...,vn
// is a PostgreSQL encoding of the respective array value.
//
// We choose to do all type conversions explicitly ourselves so that
// we can generate more targeted error messages: hence we pass
// *interface{} parameters to row.Scan.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, srcTable string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable) error {
	rowsInterface, err := isi.GetRowsFromTable(conv, srcTable)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get data for table %s : err = %s", srcTable, err))
		return err
	}
	rows := rowsInterface.(*sql.Rows)
	defer rows.Close()
	srcCols, _ := rows.Columns()
	v, scanArgs := buildVals(len(srcCols))
	for rows.Next() {
		// get RawBytes from data.
		err := rows.Scan(scanArgs...)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't process sql data row: %s", err))
			// Scan failed, so we don't have any data to add to bad rows.
			conv.StatsAddBadRow(srcTable, conv.DataMode())
			continue
		}
		values := valsToStrings(v)
		ProcessDataRow(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, values)
	}
	return nil
}

// GetRowsFromTable returns a sql Rows object for a table.
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error) {
	q := fmt.Sprintf(`SELECT * FROM "%s"."%s";`, conv.SrcSchema[srcTable].Schema, srcTable)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, err
	}
	return rows, err
}

//[ps*]
// buildVals contructs interface{} value containers to scan row
// results into.  Returns both the underlying containers (as a slice)
// as well as an interface{} of pointers to containers to pass to
// rows.Scan.
func buildVals(n int) (v []interface{}, iv []interface{}) {
	v = make([]interface{}, n)
	for i := range v {
		iv = append(iv, &v[i])
	}
	return v, iv
}

// GetRowCount with number of rows in each table.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s";`, table.Schema, table.Name)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count int64
	if rows.Next() {
		err := rows.Scan(&count)
		return count, err
	}
	return 0, nil //Check if 0 is ok to return
}

// GetTables return list of tables in the selected database.
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	q := `
	SELECT 
		sc.name AS table_schema, 
			tbls.name AS table_name
		FROM sys.tables AS tbls
		INNER JOIN sys.schemas AS sc on sc.schema_id = tbls.schema_id
		WHERE tbls.type = 'U' AND tbls.is_tracked_by_cdc = 0
	`
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}

	defer rows.Close()
	var tableSchema, tableName string
	var tables []common.SchemaAndName
	for rows.Next() {
		rows.Scan(&tableSchema, &tableName)
		tables = append(tables, common.SchemaAndName{Schema: tableSchema, Name: tableName})
	}
	return tables, nil
}

// GetColumns returns a list of Column objects and names
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	q := `
		SELECT 
			column_name, 
			data_type, 
			is_nullable, 
			column_default, 
			character_maximum_length, 
			numeric_precision, 
			numeric_scale
		FROM information_schema.COLUMNS 
		WHERE table_schema = @p1 and table_name = @p2 
		ORDER BY ordinal_position;
	`
	cols, err := isi.Db.Query(q, table.Schema, table.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colDefs := make(map[string]schema.Column)
	var colNames []string
	var colName, dataType string
	var isNullable string
	var colDefault sql.NullString
	// elementDataType
	var charMaxLen, numericPrecision, numericScale sql.NullInt64
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
			// c can be UNIQUE, PRIMARY KEY, FOREIGN KEY,
			// or CHECK (based on msql, sql server, postgres docs).
			// We've already filtered out PRIMARY KEY.
			switch c {
			case "CHECK":
				ignored.Check = true
			case "FOREIGN KEY", "PRIMARY KEY", "UNIQUE":
				// Nothing to do here -- these are handled elsewhere.
			}
		}
		ignored.Default = colDefault.Valid
		c := schema.Column{
			Name:    colName,
			Type:    toType(dataType, charMaxLen, numericPrecision, numericScale),
			NotNull: strings.ToUpper(isNullable) == "NO",
			Ignored: ignored,
		}
		colDefs[colName] = c
		colNames = append(colNames, colName)
	}
	return colDefs, colNames, nil
}

// GetConstraints returns a list of primary keys and by-column map of
// other constraints.  Note: we need to preserve ordinal order of
// columns in primary key constraints.
// Note that foreign key constraints are handled in getForeignKeys.
func (isi InfoSchemaImpl) GetConstraints(conv *internal.Conv, table common.SchemaAndName) ([]string, map[string][]string, error) {
	q := `
		SELECT 
			k.COLUMN_NAME, 
			t.CONSTRAINT_TYPE
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
		INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
			ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
		WHERE k.TABLE_SCHEMA = @p1 AND k.TABLE_NAME = @p2 ORDER BY k.ordinal_position;
	`
	rows, err := isi.Db.Query(q, table.Schema, table.Name)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var primaryKeys []string
	var col, constraint string
	m := make(map[string][]string)
	for rows.Next() {
		err := rows.Scan(&col, &constraint)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if col == "" || constraint == "" {
			conv.Unexpected(fmt.Sprintf("Got empty col or constraint"))
			continue
		}
		switch constraint {
		case "PRIMARY KEY":
			primaryKeys = append(primaryKeys, col)
		default:
			m[col] = append(m[col], constraint)
		}
	}
	return primaryKeys, m, nil
}

// GetForeignKeys returns a list of all the foreign key constraints.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	q := `
		SELECT  
			sch.name AS [schema_name],
			tab2.name AS [referenced_table],
			col1.name AS [column],
			col2.name AS [referenced_column],
			obj.name AS FK_NAME
		FROM sys.foreign_key_columns fkc
		INNER JOIN sys.objects obj
			ON obj.object_id = fkc.constraint_object_id
		INNER JOIN sys.tables tab1
			ON tab1.object_id = fkc.parent_object_id
		INNER JOIN sys.schemas sch
			ON tab1.schema_id = sch.schema_id
		INNER JOIN sys.columns col1
			ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
		INNER JOIN sys.tables tab2
			ON tab2.object_id = fkc.referenced_object_id
		INNER JOIN sys.columns col2
			ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id and tab1.name =@p1
	`
	rows, err := isi.Db.Query(q, table.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refTable common.SchemaAndName
	var col, refCol, fKeyName string
	fKeys := make(map[string]common.FkConstraint)
	var keyNames []string
	for rows.Next() {
		err := rows.Scan(&refTable.Schema, &refTable.Name, &col, &refCol, &fKeyName)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		tableName := isi.GetTableName(refTable.Schema, refTable.Name)
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.Cols = append(fk.Cols, col)
			fk.Refcols = append(fk.Refcols, refCol)
			fKeys[fKeyName] = fk
			continue
		}
		fKeys[fKeyName] = common.FkConstraint{Name: fKeyName, Table: tableName, Refcols: []string{refCol}, Cols: []string{col}}
		keyNames = append(keyNames, fKeyName)
	}

	sort.Strings(keyNames)
	for _, k := range keyNames {
		foreignKeys = append(foreignKeys,
			schema.ForeignKey{
				Name:         fKeys[k].Name,
				Columns:      fKeys[k].Cols,
				ReferTable:   fKeys[k].Table,
				ReferColumns: fKeys[k].Refcols})
	}
	return foreignKeys, nil
}

// GetIndexes return a list of all indexes for the specified table.
// Note: Extracting index definitions from PostgreSQL information schema tables is complex.
// See https://stackoverflow.com/questions/6777456/list-all-index-names-column-names-and-its-table-name-of-a-postgresql-database/44460269#44460269
// for background.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName) ([]schema.Index, error) {
	q2 := `
		SELECT
			ix.name, 
			COL_NAME(ix.object_id, ixc.column_id) as [Column Name],
			ix.is_unique,
			ixc.is_descending_key 
		FROM sys.indexes ix 
		INNER JOIN sys.index_columns ixc 
			ON  ix.object_id = ixc.object_id AND ix.index_id = ixc.index_id
		INNER JOIN sys.tables tab 
			ON ix.object_id = tab.object_id 
		WHERE
			ix.is_primary_key = 0          
			AND ix.is_unique_constraint = 0 
			AND tab.is_ms_shipped = 0   
			AND tab.name=@p1
			ORDER BY ix.name ;
	`
	rows, err := isi.Db.Query(q2, table.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// TODO : remove sequence
	var name, column, isUnique, collation string
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	var indexes []schema.Index
	for rows.Next() {
		if err := rows.Scan(&name, &column, &isUnique, &collation); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}

		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{Name: name, Unique: (isUnique == "true")}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{Column: column, Desc: (collation == "DESC")})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		indexes = append(indexes, indexMap[k])
	}
	return indexes, nil
}

func toType(dataType string, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	// case dataType == "ARRAY":
	// 	return schema.Type{Name: elementDataType.String, ArrayBounds: []int64{-1}}
	// 	// TODO: handle error cases.
	// 	// TODO: handle case of multiple array bounds.
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	case dataType == "numeric" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "numeric" && numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	default:
		return schema.Type{Name: dataType}
	}
}

//[ps*]
func valsToStrings(vals []interface{}) []string {
	toString := func(val interface{}) string {
		if val == nil {
			return "NULL"
		}
		switch v := val.(type) {
		case *interface{}:
			val = *v
		}
		return fmt.Sprintf("%v", val)
	}
	var s []string
	for _, v := range vals {
		s = append(s, toString(v))
	}
	return s
}