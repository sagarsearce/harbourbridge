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

package sqlserver

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ProcessDataRow converts a row of data and writes it out to Spanner.
// srcTable and srcCols are the source table and columns respectively,
// and vals contains string data to be converted to appropriate types
// to send to Spanner.  ProcessDataRow is only called in DataMode.
func ProcessDataRow(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, vals []string) {
	spTable, cvtCols, cvtVals, err := ConvertData(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, vals)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
		conv.StatsAddBadRow(srcTable, conv.DataMode())
		conv.CollectBadRow(srcTable, srcCols, vals)
	} else {
		conv.WriteRow(srcTable, spTable, cvtCols, cvtVals)
	}
}

// ConvertData maps the source DB data in vals into Spanner data,
// based on the Spanner and source DB schemas. Note that since entries
// in vals may be empty, we also return the list of columns (empty
// cols are dropped).
func ConvertData(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, vals []string) (string, []string, []interface{}, error) {
	var c []string
	var v []interface{}
	if len(spCols) != len(srcCols) || len(spCols) != len(vals) {
		return "", []string{}, []interface{}{}, fmt.Errorf("ConvertData: spCols, srcCols and vals don't all have the same lengths: len(spCols)=%d, len(srcCols)=%d, len(vals)=%d", len(spCols), len(srcCols), len(vals))
	}
	for i, spCol := range spCols {
		srcCol := srcCols[i]
		// Skip columns with 'NULL' values.
		if vals[i] == "<nil>" || vals[i] == "NULL" {
			continue
		}
		spColDef, ok1 := spSchema.ColDefs[spCol]
		srcColDef, ok2 := srcSchema.ColDefs[srcCol]
		if !ok1 || !ok2 {
			return "", []string{}, []interface{}{}, fmt.Errorf("can't find Spanner and source-db schema for col %s", spCol)
		}
		var x interface{}
		var err error
		if spColDef.T.IsArray {
			x, err = convArray(spColDef.T, srcColDef.Type.Name, vals[i])
		} else {
			x, err = convScalar(conv, spColDef.T, srcColDef.Type.Name, conv.TimezoneOffset, vals[i])
		}
		if err != nil {
			return "", []string{}, []interface{}{}, err
		}
		v = append(v, x)
		c = append(c, spCol)
	}
	if aux, ok := conv.SyntheticPKeys[spTable]; ok {
		c = append(c, aux.Col)
		v = append(v, int64(bits.Reverse64(uint64(aux.Sequence))))
		aux.Sequence++
		conv.SyntheticPKeys[spTable] = aux
	}
	return spTable, c, v, nil
}

// convScalar converts a source database string value to an
// appropriate Spanner value. It is the caller's responsibility to
// detect and handle NULL values: convScalar will return error if a
// NULL value is passed.
func convScalar(conv *internal.Conv, spannerType ddl.Type, srcTypeName string, TimezoneOffset string, val string) (interface{}, error) {
	// Whitespace within the val string is considered part of the data value.
	// Note that many of the underlying conversions functions we use (like
	// strconv.ParseFloat and strconv.ParseInt) return "invalid syntax"
	// errors if whitespace were to appear at the start or end of a string.
	// We do not expect pg_dump to generate such output.
	switch spannerType.Name {
	case ddl.Bool:
		return convBool(val)
	case ddl.Bytes:
		return convBytes(val)
	case ddl.Date:
		return convDate(val)
	case ddl.Float64:
		return convFloat64(val)
	case ddl.Int64:
		return convInt64(val)
	case ddl.Numeric:
		return convNumeric(conv, val)
	case ddl.String:
		return val, nil
	case ddl.Timestamp:
		return convTimestamp(srcTypeName, TimezoneOffset, val)
	case ddl.JSON:
		return val, nil
	default:
		return val, fmt.Errorf("data conversion not implemented for type %v", spannerType.Name)
	}
}

func convBool(val string) (bool, error) {
	b, err := strconv.ParseBool(val)
	if err != nil {
		return b, fmt.Errorf("can't convert to bool: %w", err)
	}
	return b, err
}

func convBytes(val string) ([]byte, error) {
	// convert a string to a byte slice.
	b := []byte(val)
	return b, nil
}

func convDate(val string) (civil.Date, error) {
	date := strings.Fields(val)
	d, err := civil.ParseDate(date[0])
	if err != nil {
		return d, fmt.Errorf("can't convert to date: %w", err)
	}
	return d, err
}

func convFloat64(val string) (float64, error) {
	//val will be a byte slice in the form of a string
	//convertByteslice conversts val to a proper byte slice
	if strings.HasPrefix(val, "[") {
		b := convertByteSlice(val)
		val = string(b)
	}
	float, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return float, fmt.Errorf("can't convert to float64: %w", err)
	}
	return float, err
}

func convInt64(val string) (int64, error) {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return i, fmt.Errorf("can't convert to int64: %w", err)
	}
	return i, err
}

// convNumeric maps a source database string value (representing a numeric)
// into a string representing a valid Spanner numeric.
func convNumeric(conv *internal.Conv, val string) (interface{}, error) {
	//val will be a byte slice in the form of a string
	//convertByteslice conversts val to a proper byte slice
	if strings.HasPrefix(val, "[") {
		b := convertByteSlice(val)
		val = string(b)
	}
	if conv.TargetDb == constants.TargetExperimentalPostgres {
		return spanner.PGNumeric{Numeric: val, Valid: true}, nil
	} else {
		r := new(big.Rat)
		if _, ok := r.SetString(val); !ok {
			return "", fmt.Errorf("can't convert %q to big.Rat", val)
		}
		return r, nil
	}
}

// convTimestamp maps a source DB timestamp into a go Time Spanner timestamp
// It handles both datetime and timestamp conversions.
func convTimestamp(srcTypeName string, TimezoneOffset string, val string) (t time.Time, err error) {
	//val will be a byte slice in the form of a string
	//convertByteslice conversts val to a proper byte slice
	if strings.HasPrefix(val, "[") {
		b := convertByteSlice(val)
		data := binary.BigEndian.Uint64(b)
		t := time.Unix(int64(data), 0)
		val = t.String()
	}
	if srcTypeName == "timestamp" {
		// We consider timezone for timestamp datatype.
		// If timezone is not specified, we consider UTC time.
		if TimezoneOffset == "" {
			TimezoneOffset = "+00:00"
		}
		// convert timestamp from format "2006-01-02 15:04:05" to
		// "2006-01-02T15:04:05+00:00".
		timeNew := strings.Split(val, " ")
		timeJoined := strings.Join(timeNew, "T")
		timeJoined = timeJoined + TimezoneOffset
		t, err = time.Parse(time.RFC3339, timeJoined)
	}
	if srcTypeName == "datetimeoffset" {
		// val will be in this format "2021-12-15 07:39:52.9433333 +0000 +0000"
		// we ignore the part after time
		if idx := strings.Index(val, "+"); idx != -1 {
			val = val[:idx-1]
		}
		timeNew := strings.Split(val, " ")
		timeJoined := strings.Join(timeNew, "T")
		timeJoined = timeJoined + TimezoneOffset
		t, err = time.Parse(time.RFC3339, timeJoined)
	} else {
		// datetime: data should just consist of date and time.
		// timestamp conversion should ignore timezone.
		// val will be in this format "2021-12-15 07:39:52.943 +0000 UTC"
		// we ignore the part after time
		if idx := strings.Index(val, "+"); idx != -1 {
			val = val[:idx-1]
		}
		t, err = time.Parse("2006-01-02 15:04:05", val)
	}
	if err != nil {
		return t, fmt.Errorf("can't convert to timestamp (mssql type: %s)", srcTypeName)
	}
	return t, err
}

// convArray converts a source database string value (representing an
// array) to an appropriate Spanner array value. It is the caller's
// responsibility to detect and handle the case where the entire array
// is NULL. However, convArray does handle the case where individual
// array elements are NULL. In other words, convArray handles "{1,
// NULL, 2}", but it does not handle "NULL" (it returns error).
func convArray(spannerType ddl.Type, srcTypeName string, v string) (interface{}, error) {
	v = strings.TrimSpace(v)
	// Handle empty array. Note that we use an empty NullString array
	// for all Spanner array types since this will be converted to the
	// appropriate type by the Spanner client.
	if v == "" {
		return []spanner.NullString{}, nil
	}

	a := strings.Split(v, ",")

	// The Spanner client for go does not accept []interface{} for arrays.
	// Instead it only accepts slices of a specific type eg: []string
	// Hence we have to do the following case analysis.
	switch spannerType.Name {
	case ddl.String:
		var r []spanner.NullString
		for _, s := range a {
			if s == "NULL" {
				r = append(r, spanner.NullString{Valid: false})
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return []spanner.NullString{}, err
			}
			r = append(r, spanner.NullString{StringVal: s, Valid: true})
		}
		return r, nil
	}
	return []interface{}{}, fmt.Errorf("array type conversion not implemented for type %v", spannerType.Name)
}

// processQuote returns the unquoted version of s.
// Note: The element values of PostgreSQL arrays may have double
// quotes around them.  The array output routine will put double
// quotes around element values if they are empty strings, contain
// curly braces, delimiter characters, double quotes, backslashes, or
// white space, or match the word NULL. Double quotes and backslashes
// embedded in element values will be backslash-escaped.  See section
// 8.14.6.of www.postgresql.org/docs/9.1/arrays.html.
func processQuote(s string) (string, error) {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strconv.Unquote(s)
	}
	return s, nil
}

func convertByteSlice(val string) []byte {
	a := strings.Fields(val)
	var values []byte
	for _, v := range a {
		v = strings.Trim(v, "[]")
		int_val, _ := strconv.Atoi(v)
		values = append(values, byte(int_val))
	}
	return values
}
