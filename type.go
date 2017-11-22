package parquet

import (
	"errors"
	"fmt"

	gparq "github.com/bizenn/go-parquet/gen-go/parquet"
)

func setupSchemaElement(se *gparq.SchemaElement, name, typ, repType string, precision, scale int64) (err error) {
	se.Name = name
	if t, xerr := gparq.TypeFromString(typ); xerr == nil {
		se.Type = gparq.TypePtr(t)
		if t == gparq.Type_FIXED_LEN_BYTE_ARRAY {
			if precision > 0 {
				se.TypeLength = int32p(precision)
			} else {
				err = errors.New("Type FIXED_LEN_BYTE_ARRAY requires greater than 0 as its length")
			}
		}
	} else if t, xerr := gparq.ConvertedTypeFromString(typ); xerr == nil {
		se.ConvertedType = gparq.ConvertedTypePtr(t)
		switch t {
		case gparq.ConvertedType_UTF8, gparq.ConvertedType_ENUM:
			se.Type = gparq.TypePtr(gparq.Type_BYTE_ARRAY)
			// Because now ConvertedType_UUID doesn't exist. Why?
			// case gparq.ConvertedType_UUID:
			// 	se.Type = gparq.TypePtr(gparq.Type_FIXED_LEN_BYTE_ARRAY)
			// 	se.TypeLength = int32p(16)
		case gparq.ConvertedType_INT_8, gparq.ConvertedType_INT_16, gparq.ConvertedType_INT_32:
			se.Type = gparq.TypePtr(gparq.Type_INT32)
		case gparq.ConvertedType_INT_64:
			se.Type = gparq.TypePtr(gparq.Type_INT64)
		case gparq.ConvertedType_UINT_8, gparq.ConvertedType_UINT_16, gparq.ConvertedType_UINT_32:
			se.Type = gparq.TypePtr(gparq.Type_INT32)
		case gparq.ConvertedType_UINT_64:
			se.Type = gparq.TypePtr(gparq.Type_INT64)
		case gparq.ConvertedType_DECIMAL:
			if scale < 0 {
				err = fmt.Errorf("Scale must be positive or zero but: %d", scale)
			} else if precision <= 0 {
				err = fmt.Errorf("Precision must be positive but: %d", precision)
			} else if precision < scale {
				err = fmt.Errorf("Precision must be greater than or equal to scale but: %d.%d", precision, scale)
			} else {
				se.Precision, se.Scale = int32p(precision), int32p(scale)
				if precision <= 9 {
					se.Type = gparq.TypePtr(gparq.Type_INT32)
				} else if precision <= 18 {
					se.Type = gparq.TypePtr(gparq.Type_INT64)
				} else {
					se.Type = gparq.TypePtr(gparq.Type_BYTE_ARRAY)
				}
			}
		case gparq.ConvertedType_DATE, gparq.ConvertedType_TIME_MILLIS:
			se.Type = gparq.TypePtr(gparq.Type_INT32)
		case gparq.ConvertedType_TIME_MICROS, gparq.ConvertedType_TIMESTAMP_MILLIS, gparq.ConvertedType_TIMESTAMP_MICROS:
			se.Type = gparq.TypePtr(gparq.Type_INT64)
		case gparq.ConvertedType_INTERVAL:
			se.Type = gparq.TypePtr(gparq.Type_FIXED_LEN_BYTE_ARRAY)
			se.TypeLength = int32p(12)
		case gparq.ConvertedType_JSON, gparq.ConvertedType_BSON:
			se.Type = gparq.TypePtr(gparq.Type_BYTE_ARRAY)
		default:
			err = fmt.Errorf("Unsupported type: %s", t.String())
		}
	} else {
		err = xerr
	}
	if err == nil {
		if rt, xerr := gparq.FieldRepetitionTypeFromString(repType); xerr == nil {
			se.RepetitionType = gparq.FieldRepetitionTypePtr(rt)
		} else if repType == "" {
			se.RepetitionType = gparq.FieldRepetitionTypePtr(gparq.FieldRepetitionType_OPTIONAL)
		} else {
			err = xerr
		}
	}
	return err
}
