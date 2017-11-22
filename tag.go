package parquet

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"

	gparq "github.com/bizenn/go-parquet/gen-go/parquet"
)

const (
	tagName = "parquet"
)

var t gparq.Type

// ColumnType ...
type ColumnType struct {
	Name      string
	Type      string
	Precision int32
	Scale     int32
}

var (
	nameMatcher      = regexp.MustCompile(`^[_[:alpha:]][_[:alnum:]]*`)
	delimiterMatcher = regexp.MustCompile(`^\s*,\s*`)
	typeMatcher      = regexp.MustCompile(`^[_[:alpha:]][_[:alnum:]]*`)
	typeParamMatcher = regexp.MustCompile(`^([_[:alpha:]][_[:alnum:]]*)\((\d+)(?:\s*,\s*(\d+))?\)`)
	repTypeMatcher   = regexp.MustCompile(`^[[:alpha:]]+$`)
)

func int32p(v int64) *int32 { vv := int32(v); return &vv }

// tag := parquet:"<columnName>,<type_descriptor>"
// columnName := valid parquet column name
// type_descriptor :=  BOOLEAN | INT32 | INT64 | FLOAT | DOUBLE | BYTE_ARRAY | FIXED_LEN_BYTE_ARRAY(len) \
//                     | UTF8 | INT_8 | INT_16 | INT_32 | INT_64 | UINT_8 | UINT_16 | UINT_32 | UINT_64 \
//                     | DECIMAL(precision,scale) \
//                     | DATE | TIME_MILLIS | TIME_MICROS | TIMESTAMP_MILLIS | TIMESTAMP_MICROS | INTERVAL
//                     | JSON | BSON
// Currently unsupported: Nested Types
func parseTag(stag reflect.StructTag) (se *gparq.SchemaElement, err error) {
	tag := string(stag)
	var is []int
	var name, typ, repType string
	var precision, scale int64
	if is = nameMatcher.FindStringIndex(tag); is == nil {
		err = fmt.Errorf("Failed to parse \"Name\" on %s", tag)
	} else {
		name = tag[is[0]:is[1]]
		tag = tag[is[1]:]
		if is = delimiterMatcher.FindStringIndex(tag); is == nil {
			err = fmt.Errorf("Failed to parse delimiter on %s", tag)
		} else {
			tag = tag[is[1]:]
			if is = typeParamMatcher.FindStringSubmatchIndex(tag); is != nil {
				typ = tag[is[2]:is[3]]
				if precision, err = strconv.ParseInt(tag[is[4]:is[5]], 10, 32); err == nil {
					if is[6] < is[7] {
						scale, err = strconv.ParseInt(tag[is[6]:is[7]], 10, 32)
					}
				}
			} else if is = typeMatcher.FindStringIndex(tag); is != nil {
				typ = tag[is[0]:is[1]]
			} else {
				err = fmt.Errorf("Failed to parse \"Type\" on %s", tag)
			}
			if err == nil {
				tag = tag[is[1]:]
				if tag != "" {
					if is = delimiterMatcher.FindStringIndex(tag); is == nil {
						err = fmt.Errorf("Failed to parse delimiter on %s", tag)
					} else {
						tag = tag[is[1]:]
						if is = repTypeMatcher.FindStringIndex(tag); is != nil {
							repType = tag[is[0]:is[1]]
						} else {
							err = fmt.Errorf("Failed to parse repetition type on %s", tag)
						}
					}
				}
			}
		}
	}
	if err == nil {
		se = gparq.NewSchemaElement()
		err = setupSchemaElement(se, name, typ, repType, precision, scale)
	}
	return se, err
}
