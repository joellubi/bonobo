package engine

import (
	"github.com/apache/arrow/go/v16/arrow"
)

var ArrowTypes = struct {
	BooleanType arrow.DataType
	Int8Type    arrow.DataType
	Int16Type   arrow.DataType
	Int32Type   arrow.DataType
	Int64Type   arrow.DataType
	FloatType   arrow.DataType
	DoubleType  arrow.DataType
	StringType  arrow.DataType
}{
	BooleanType: arrow.FixedWidthTypes.Boolean,
	Int8Type:    arrow.PrimitiveTypes.Int8,
	Int16Type:   arrow.PrimitiveTypes.Int16,
	Int32Type:   arrow.PrimitiveTypes.Int32,
	Int64Type:   arrow.PrimitiveTypes.Int64,
	FloatType:   arrow.PrimitiveTypes.Float32,
	DoubleType:  arrow.PrimitiveTypes.Float64,
	StringType:  arrow.BinaryTypes.String,
}
