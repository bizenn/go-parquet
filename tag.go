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

var (
	nameMatcher      = regexp.MustCompile(`^[_[:alpha:]][_[:alnum:]]*`)
	delimiterMatcher = regexp.MustCompile(`^\s*,\s*`)
	typeMatcher      = regexp.MustCompile(`^[_[:alpha:]][_[:alnum:]]*`)
	typeParamMatcher = regexp.MustCompile(`^([_[:alpha:]][_[:alnum:]]*)\((\d+)(?:\s*,\s*(\d+))?\)`)
	repTypeMatcher   = regexp.MustCompile(`^[[:alpha:]]+$`)
)

func int32p(v int64) *int32 { vv := int32(v); return &vv }

// tag := parquet:"<columnName>,<type_descriptor>,<repetition_type>"
// columnName := valid parquet column name
// type_descriptor :=  BOOLEAN | INT32 | INT64 | FLOAT | DOUBLE | BYTE_ARRAY | FIXED_LEN_BYTE_ARRAY(len) \
//                     | UTF8 | INT_8 | INT_16 | INT_32 | INT_64 | UINT_8 | UINT_16 | UINT_32 | UINT_64 \
//                     | DECIMAL(precision,scale) \
//                     | DATE | TIME_MILLIS | TIME_MICROS | TIMESTAMP_MILLIS | TIMESTAMP_MICROS | INTERVAL
//                     | JSON | BSON
// Currently unsupported: Nested Types
// repetition_type := REQUIRED | OPTIONAL | REPEATED
func parseTag(tag string) (se *gparq.SchemaElement, err error) {
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

func makeSchemaElements(t reflect.Type) (ses []*gparq.SchemaElement, err error) {
	if t.Kind() != reflect.Struct {
		err = fmt.Errorf("Only struct can be converted to SchemaElements but got: %s", t.Kind().String())
	} else {
		ses = make([]*gparq.SchemaElement, t.NumField(), t.NumField())
		for i := 0; i < t.NumField(); i++ {
			st := t.Field(i)
			var se *gparq.SchemaElement
			if se, err = parseTag(st.Tag.Get(tagName)); err != nil {
				break
			}
			ses[i] = se
		}
	}
	return ses, err
}
