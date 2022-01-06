package oracle

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
func (isi InfoSchemaImpl) GetTableName(dbName string, tableName string) string {
	return tableName
}

// GetRowsFromTable returns a sql Rows object for a table.
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error) {
	panic("unimplemented")
}

// ProcessData performs data conversion for source database.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, srcTable string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable) error {
	panic("unimplemented")
}

func ProcessDataRow(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, values []string) {
	panic("unimplemented")
}

// GetRowCount with number of rows in each table.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	panic("unimplemented")
}

// GetTables return list of tables in the selected database.
// Note that sql.DB already effectively has the dbName
// embedded within it (dbName is part of the DSN passed to sql.Open),
// but unfortunately there is no way to extract it from sql.DB.
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	// In MySQL, schema is the same as database name.
	q := fmt.Sprintf("SELECT table_name from all_tables where owner = '%s'", isi.DbName)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}
	defer rows.Close()
	var tableName string
	var tables []common.SchemaAndName
	for rows.Next() {
		rows.Scan(&tableName)
		tables = append(tables, common.SchemaAndName{Schema: isi.DbName, Name: tableName})
	}
	return tables, nil
}

// GetColumns returns a list of Column objects and names
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	q := fmt.Sprintf(`SELECT column_name, data_type, nullable, data_default, data_length, data_precision, data_scale FROM USER_TAB_COLUMNS WHERE table_name = '%s'`, table.Name)
	cols, err := isi.Db.Query(q)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colDefs := make(map[string]schema.Column)
	var colNames []string
	var colName, dataType string
	var isNullable string
	var colDefault sql.NullString
	var charMaxLen, numericPrecision, numericScale sql.NullInt64
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
		// Type of constraint definition in oracle C (check constraint on a table)
		// P (primary key), U (unique key) ,R (referential integrity), V (with check option, on a view)
		// O (with read only, on a view).
		// We've already filtered out PRIMARY KEY.
			switch c {
			case "C":
				ignored.Check = true
			case "R", "P", "U":
				// Nothing to do here -- these are handled elsewhere.
			}
		}
		ignored.Default = colDefault.Valid
		c := schema.Column{
			Name:    colName,
			Type:    toType(dataType, charMaxLen, numericPrecision, numericScale),
			NotNull: strings.ToUpper(isNullable) == "N",
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
	q := fmt.Sprintf(`SELECT k.column_name,
       t.constraint_type
	   FROM   ALL_CONSTRAINTS t
       INNER JOIN ALL_CONS_COLUMNS k
       ON ( k.constraint_name = t.constraint_name ) WHERE k.table_name = '%s'`, table.Name)
	rows, err := isi.Db.Query(q)
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
			conv.Unexpected("Got empty col or constraint")
			continue
		}
		// P (primary key) constraint in oracle
		switch constraint {
		case "P":
			primaryKeys = append(primaryKeys, col)
		default:
			m[col] = append(m[col], constraint)
		}
	}
	return primaryKeys, m, nil
}

// GetForeignKeys return list all the foreign keys constraints.
// MySQL supports cross-database foreign key constraints. We ignore
// them because HarbourBridge works database at a time (a specific run
// of HarbourBridge focuses on a specific database) and so we can't handle
// them effectively.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	q := fmt.Sprintf(`SELECT B.TABLE_NAME AS REF_TABLE, A.COLUMN_NAME AS COL_NAME,
				B.COLUMN_NAME AS REF_COL_NAME ,A.CONSTRAINT_NAME AS NAME
		FROM ALL_CONS_COLUMNS A 
		JOIN ALL_CONSTRAINTS C ON A.OWNER = C.OWNER 
			AND A.CONSTRAINT_NAME = C.CONSTRAINT_NAME
    	JOIN ALL_CONS_COLUMNS B ON B.OWNER = C.OWNER 
			AND B.CONSTRAINT_NAME = C.R_CONSTRAINT_NAME
    	WHERE A.TABLE_NAME='%s' AND A.OWNER='%s'`,table.Name,isi.DbName) 
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var col, refCol, refTable, fKeyName string
	fKeys := make(map[string]common.FkConstraint)
	var keyNames []string

	for rows.Next() {
		err := rows.Scan(&refTable, &col, &refCol, &fKeyName)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.Cols = append(fk.Cols, col)
			fk.Refcols = append(fk.Refcols, refCol)
			fKeys[fKeyName] = fk
			continue
		}
		fKeys[fKeyName] = common.FkConstraint{Name: fKeyName, Table: refTable, Refcols: []string{refCol}, Cols: []string{col}}
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
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName) ([]schema.Index, error) {
	q := fmt.Sprintf(`SELECT AI.INDEX_NAME, AIC.COLUMN_NAME, AIC.COLUMN_POSITION, AI.UNIQUENESS
				FROM ALL_INDEXES AI, ALL_IND_COLUMNS AIC
				WHERE AI.INDEX_NAME  = AIC.INDEX_NAME
					AND AI.TABLE_OWNER = AIC.TABLE_OWNER
					AND AI.TABLE_OWNER = '%s'
					AND AI.TABLE_NAME  = '%s'
				ORDER BY AI.INDEX_NAME, AIC.COLUMN_POSITION`, table.Schema, table.Name)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, column, sequence, Unique string
	var collation sql.NullString
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	var indexes []schema.Index
	for rows.Next() {
		if err := rows.Scan(&name, &column, &sequence, &Unique); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{Name: name, Unique: (Unique == "UNIQUE")}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{Column: column, Desc: (collation.Valid)})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		indexes = append(indexes, indexMap[k])
	}
	return indexes, nil
}

//Function for mysql now. Have to change later.
func toType(dataType string, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
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

