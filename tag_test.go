package parquet

import (
	"reflect"
	"testing"

	g "github.com/bizenn/go-parquet/gen-go/parquet"
)

func newSchemaElement(name string, t g.Type, ct g.ConvertedType, rt g.FieldRepetitionType,
	length, precision, scale int32) (s *g.SchemaElement) {
	s = g.NewSchemaElement()
	s.Name = name
	s.Type = g.TypePtr(t)
	s.RepetitionType = g.FieldRepetitionTypePtr(rt)
	if int(ct) >= 0 {
		s.ConvertedType = g.ConvertedTypePtr(ct)
	}
	if length > 0 {
		s.TypeLength = &length
	}
	if precision > 0 {
		s.Precision = &precision
	}
	if scale >= 0 {
		s.Scale = &scale
	}
	return s
}

func assertSchemaElement(t *testing.T, id interface{}, r, e *g.SchemaElement) {
	if r != e {
		if r.Type != e.Type {
			switch {
			case r.Type == nil:
				t.Errorf("%v: Type expected %s but got nil", id, *e.Type)
			case e == nil:
				t.Errorf("%v: Type expected nil but got %s", id, *r.Type)
			case *r.Type != *e.Type:
				t.Errorf("%v: Type expected %s but got %s", id, *e.Type, *r.Type)
			}
		}
		if r.TypeLength != e.TypeLength {
			switch {
			case r.TypeLength == nil:
				t.Errorf("%v: TypeLength expected %d but got nil", id, *e.TypeLength)
			case e.TypeLength == nil:
				t.Errorf("%v: TypeLength expected nil but got %d", id, *r.TypeLength)
			case *r.TypeLength != *e.TypeLength:
				t.Errorf("%v: TypeLength expected %d but got %d", id, *e.TypeLength, *r.TypeLength)
			}
		}
		if r.RepetitionType != e.RepetitionType {
			switch {
			case r.RepetitionType == nil:
				t.Errorf("%v: RepetitionType expected %s but got nil", id, *e.RepetitionType)
			case e.RepetitionType == nil:
				t.Errorf("%v: RepetitionType expected nil but got %s", id, *r.RepetitionType)
			case *r.RepetitionType != *e.RepetitionType:
				t.Errorf("%v: RepetitionType expected %s but got %s",
					id, *e.RepetitionType, *r.RepetitionType)
			}
		}
		if r.Name != e.Name {
			t.Errorf("%v: Name expected %s but got %s", id, e.Name, r.Name)
		}
		if r.NumChildren != e.NumChildren {
			switch {
			case r.NumChildren == nil:
				t.Errorf("%v: NumChildren expected %d but got nil", id, *e.NumChildren)
			case e.NumChildren == nil:
				t.Errorf("%v: NumChildren expected nil but got %d", id, *r.NumChildren)
			case *r.NumChildren != *e.NumChildren:
				t.Errorf("%v: NumChildren expected %d but got %d", id, *e.NumChildren, *r.NumChildren)
			}
		}
		if r.ConvertedType != e.ConvertedType {
			switch {
			case r.ConvertedType == nil:
				t.Errorf("%v: ConvertedType expected %s but got nil", id, *e.ConvertedType)
			case e.ConvertedType == nil:
				t.Errorf("%v: ConvertedType expected nil but got %s", id, *r.ConvertedType)
			case *r.ConvertedType != *e.ConvertedType:
				t.Errorf("%v: ConvertedType expected %s but got %s",
					id, *e.ConvertedType, *r.ConvertedType)
			}
		}
		if r.Scale != e.Scale {
			switch {
			case r.Scale == nil:
				t.Errorf("%v: Scale expected %d but got nil", id, *e.Scale)
			case e.Scale == nil:
				t.Errorf("%v: Scale expected nil but got %d", id, *r.Scale)
			case *r.Scale != *e.Scale:
				t.Errorf("%v: Scale expected %d but got %d", id, *e.Scale, *r.Scale)
			}
		}
		if r.Precision != e.Precision {
			switch {
			case r.Precision == nil:
				t.Errorf("%v: Precision expected %d but got nil", id, *e.Precision)
			case e.Precision == nil:
				t.Errorf("%v: Precision expected nil but got %d", id, *r.Precision)
			case *r.Precision != *e.Precision:
				t.Errorf("%v: Precision expected %d but got %d", id, *e.Precision, *r.Precision)
			}
		}
		if r.FieldID != e.FieldID {
			switch {
			case r.FieldID == nil:
				t.Errorf("%v: FieldID expected %d but got nil", id, *e.FieldID)
			case e.FieldID == nil:
				t.Errorf("%v: FieldID expected nil but got %d", id, *r.FieldID)
			case *r.FieldID != *e.FieldID:
				t.Errorf("%v: FieldID expected %d but got %d", id, *e.FieldID, *r.FieldID)
			}
		}
		if r.LogicalType != e.LogicalType {
			switch {
			case r.LogicalType == nil:
				t.Errorf("%v: LogicalType expected %v but got nil", id, *e.LogicalType)
			case e.LogicalType == nil:
				t.Errorf("%v: LogicalType expected nil but got %v", id, *r.LogicalType)
			case *r.LogicalType != *e.LogicalType:
				t.Errorf("%v: LogicalType expected %v but got %v", id, *e.LogicalType, *r.LogicalType)
			}
		}
	}
}

var ctNull = g.ConvertedType(-1)

func TestParseTag(t *testing.T) {
	for range []string{} {
	}

	dataSet := []struct {
		tag      string
		expected *g.SchemaElement
	}{
		// Primitive Types
		{"boolean,BOOLEAN", newSchemaElement("boolean", g.Type_BOOLEAN, ctNull, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"int32,INT32", newSchemaElement("int32", g.Type_INT32, ctNull, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"int64,INT64,REQUIRED", newSchemaElement("int64", g.Type_INT64, ctNull, g.FieldRepetitionType_REQUIRED, -1, -1, -1)},
		{"int96,INT96", newSchemaElement("int96", g.Type_INT96, ctNull, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"float,FLOAT", newSchemaElement("float", g.Type_FLOAT, ctNull, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"double,DOUBLE", newSchemaElement("double", g.Type_DOUBLE, ctNull, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"fixed,FIXED_LEN_BYTE_ARRAY(40)", newSchemaElement("fixed", g.Type_FIXED_LEN_BYTE_ARRAY, ctNull, g.FieldRepetitionType_OPTIONAL, 40, -1, -1)},
		{"binary,BYTE_ARRAY", newSchemaElement("binary", g.Type_BYTE_ARRAY, ctNull, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		// Converted Types
		{"utf8,UTF8", newSchemaElement("utf8", g.Type_BYTE_ARRAY, g.ConvertedType_UTF8, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"enum,ENUM", newSchemaElement("enum", g.Type_BYTE_ARRAY, g.ConvertedType_ENUM, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		// {"uuid,UUID", newSchemaElement("uuid", g.Type_FIXED_LEN_BYTE_ARRAY, g.ConvertedType_UUID, 16, -1, -1)},
		{"int_8,INT_8", newSchemaElement("int_8", g.Type_INT32, g.ConvertedType_INT_8, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"int_16,INT_16", newSchemaElement("int_16", g.Type_INT32, g.ConvertedType_INT_16, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"int_32,INT_32", newSchemaElement("int_32", g.Type_INT32, g.ConvertedType_INT_32, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"int_64,INT_64", newSchemaElement("int_64", g.Type_INT64, g.ConvertedType_INT_64, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"uint_8,UINT_8", newSchemaElement("uint_8", g.Type_INT32, g.ConvertedType_UINT_8, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"uint_16,UINT_16", newSchemaElement("uint_16", g.Type_INT32, g.ConvertedType_UINT_16, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"uint_32,UINT_32", newSchemaElement("uint_32", g.Type_INT32, g.ConvertedType_UINT_32, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"uint_64,UINT_64", newSchemaElement("uint_64", g.Type_INT64, g.ConvertedType_UINT_64, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"decimal,DECIMAL(9)", newSchemaElement("decimal", g.Type_INT32, g.ConvertedType_DECIMAL, g.FieldRepetitionType_OPTIONAL, -1, 9, 0)},
		{"decimal,DECIMAL(18,6)", newSchemaElement("decimal", g.Type_INT64, g.ConvertedType_DECIMAL, g.FieldRepetitionType_OPTIONAL, -1, 18, 6)},
		{"decimal,DECIMAL(24,8),REQUIRED", newSchemaElement("decimal", g.Type_BYTE_ARRAY, g.ConvertedType_DECIMAL, g.FieldRepetitionType_REQUIRED, -1, 24, 8)},
		{"date,DATE", newSchemaElement("date", g.Type_INT32, g.ConvertedType_DATE, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"time_ms,TIME_MILLIS", newSchemaElement("time_ms", g.Type_INT32, g.ConvertedType_TIME_MILLIS, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"time_us,TIME_MICROS", newSchemaElement("time_us", g.Type_INT64, g.ConvertedType_TIME_MICROS, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"timestamp_ms,TIMESTAMP_MILLIS", newSchemaElement("timestamp_ms", g.Type_INT64, g.ConvertedType_TIMESTAMP_MILLIS, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"timestamp_us,TIMESTAMP_MICROS", newSchemaElement("timestamp_us", g.Type_INT64, g.ConvertedType_TIMESTAMP_MICROS, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"interval,INTERVAL", newSchemaElement("interval", g.Type_FIXED_LEN_BYTE_ARRAY, g.ConvertedType_INTERVAL, g.FieldRepetitionType_OPTIONAL, 12, -1, -1)},
		{"json,JSON", newSchemaElement("json", g.Type_BYTE_ARRAY, g.ConvertedType_JSON, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
		{"bson,BSON", newSchemaElement("bson", g.Type_BYTE_ARRAY, g.ConvertedType_BSON, g.FieldRepetitionType_OPTIONAL, -1, -1, -1)},
	}
	for _, data := range dataSet {
		result, err := parseTag(reflect.StructTag(data.tag))
		if err != nil {
			t.Errorf("%v: Unexpected error: %s", data.tag, err)
		} else {
			assertSchemaElement(t, data.tag, result, data.expected)
		}
	}
}
