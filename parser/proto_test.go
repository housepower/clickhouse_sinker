package parser

import (
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"github.com/golang/mock/gomock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/parser/testdata"
)

const (
	testTopic    = "test-proto"
	testSubject  = "test-proto-value"
	testSchemaID = 1
)

var schemaInfo schemaregistry.SchemaInfo

var (
	testDate        = time.Date(2022, 9, 1, 10, 20, 30, 0, time.UTC)
	testBaseMessage = &testdata.Test{
		BoolTrue:       true,
		BoolFalse:      false,
		NumInt32:       123,
		NumInt64:       123,
		NumFloat:       123.321,
		NumDouble:      1234.4321,
		NumUint32:      123,
		NumUint64:      123,
		Str:            "escaped_\"ws",
		StrDate:        "2009-07-13",
		Timestamp:      timestamppb.New(testDate),
		Obj:            &testdata.NestedTest{Str: "test"},
		ArrayEmpty:     []int32{},
		ArrayBool:      []bool{true, false},
		ArrayNumInt32:  []int32{-123, 0, 123},
		ArrayNumInt64:  []int64{-123, 0, 123},
		ArrayNumFloat:  []float32{0, 1.0},
		ArrayNumDouble: []float64{0, 1.0},
		ArrayNumUint32: []uint32{0, 123},
		ArrayNumUint64: []uint64{0, 123},
		ArrayStr:       []string{"aa", "bb", "cc"},
		ArrayTimestamp: []*timestamppb.Timestamp{timestamppb.New(testDate)},
		Uuid:           "2211a6ec-3799-41c1-ac41-4ab02f8e3cf2",
		ArrayUuid:      []string{"2211a6ec-3799-41c1-ac41-4ab02f8e3cf2", "f6acf2ad-757a-4eb3-96b2-6c3d6f2bec6e"},
		Ipv4:           "1.2.3.4",
		ArrayIpv4:      []string{"1.2.3.4", "2.3.4.5"},
		Ipv6:           "fe80::74e6:b5f3:fe92:830e",
		ArrayIpv6:      []string{"fe80::74e6:b5f3:fe92:830e", "fe80::2a3:aeff:fe53:743e"},
	}
	testMaxNumMessage = &testdata.Test{
		NumInt32:  math.MaxInt32,
		NumInt64:  math.MaxInt64,
		NumFloat:  math.MaxFloat32,
		NumDouble: math.MaxFloat64,
		NumUint32: math.MaxUint32,
		NumUint64: math.MaxUint64,
	}
	testMinNumMessage = &testdata.Test{
		NumInt32:  math.MinInt32,
		NumInt64:  math.MinInt64,
		NumFloat:  math.SmallestNonzeroFloat32,
		NumDouble: math.SmallestNonzeroFloat64,
		NumUint32: 0,
		NumUint64: 0,
	}
)

func createProtoMetric(t *testing.T, message *testdata.Test) model.Metric {
	t.Helper()

	ctrl := gomock.NewController(t)
	schemaRegistry := NewMockSchemaRegistryClient(ctrl)
	schemaRegistry.EXPECT().Register(testSubject, schemaInfo, false).Return(testSchemaID, nil)
	schemaRegistry.EXPECT().GetBySubjectAndID(testSubject, testSchemaID).Return(schemaInfo, nil).Times(2)

	pp, err := NewParserPool(protoName, nil, "", "", timeUnit, testTopic, schemaRegistry, "")
	require.NoError(t, err)

	serializer, err := protobuf.NewSerializer(schemaRegistry, serde.ValueSerde, protobuf.NewSerializerConfig())
	require.NoError(t, err)
	sample, err := serializer.Serialize(testTopic, message)
	require.NoError(t, err)

	parser, err := pp.Get()
	require.NoError(t, err)
	metric, err := parser.Parse(sample)
	require.NoError(t, err)

	return metric
}

func TestProtoGetString(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, ""},
		{"null", false, "<nil>"},
		{"bool_true", false, "true"},
		{"bool_false", false, "false"},
		{"num_int32", false, "123"},
		{"num_int64", false, "123"},
		{"num_float", false, "123.321"},
		{"num_double", false, "1234.4321"},
		{"num_uint32", false, "123"},
		{"num_uint64", false, "123"},
		{"str", false, `escaped_"ws`},
		{"obj", false, "str:\"test\""},
		{"array_empty", false, "[]"},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, "<nil>"},
		{"bool_true", true, "true"},
		{"bool_false", true, "false"},
		{"num_int32", true, "123"},
		{"num_int64", true, "123"},
		{"num_float", true, "123.321"},
		{"num_double", true, "1234.4321"},
		{"num_uint32", true, "123"},
		{"num_uint64", true, "123"},
		{"str", true, `escaped_"ws`},
		{"obj", true, "str:\"test\""},
		{"timestamp", true, "seconds:1662027630"},
		{"array_empty", true, "[]"},
	}

	metric := createProtoMetric(t, testBaseMessage)
	doTestSimpleForParser(t, protoName, "GetString", testCases, metric)
}

func TestProtoGetBool(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, false},
		{"null", false, false},
		{"bool_true", false, true},
		{"bool_false", false, false},
		{"num_int32", false, false},
		{"num_int64", false, false},
		{"num_float", false, false},
		{"num_double", false, false},
		{"num_uint32", false, false},
		{"num_uint64", false, false},
		{"str", false, false},
		{"timestamp", false, false},
		{"obj", false, false},
		{"array_empty", false, false},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, true},
		{"bool_false", true, false},
		{"num_int32", true, nil},
		{"num_int64", true, nil},
		{"num_float", true, nil},
		{"num_double", true, nil},
		{"num_fixed32", true, nil},
		{"num_fixed64", true, nil},
		{"str", true, nil},
		{"timestamp", true, nil},
		{"obj", true, nil},
		{"array_empty", true, nil}}

	metric := createProtoMetric(t, testBaseMessage)
	doTestSimpleForParser(t, protoName, "GetBool", testCases, metric)
}

func TestProtoGetInt(t *testing.T) {
	t.Parallel()

	t.Run("all types", func(t *testing.T) {
		t.Parallel()

		testCases := []SimpleCase{
			// nullable: false
			{"not_exist", false, int64(0)},
			{"null", false, int64(0)},
			{"bool_true", false, int64(1)},
			{"bool_false", false, int64(0)},
			{"num_int32", false, int64(123)},
			{"num_int64", false, int64(123)},
			{"num_float", false, int64(0)},
			{"num_double", false, int64(0)},
			{"num_uint32", false, int64(123)},
			{"num_uint64", false, int64(123)},
			{"str", false, int64(0)},
			{"timestamp", false, int64(0)},
			{"obj", false, int64(0)},
			{"array_empty", false, int64(0)},
			// nullable: true
			{"not_exist", true, nil},
			{"null", true, nil},
			{"bool_true", true, int64(1)},
			{"bool_false", true, int64(0)},
			{"num_int32", true, int64(123)},
			{"num_int64", true, int64(123)},
			{"num_float", true, nil},
			{"num_double", true, nil},
			{"num_uint32", true, int64(123)},
			{"num_uint64", true, int64(123)},
			{"str", true, nil},
			{"timestamp", true, nil},
			{"obj", true, nil},
			{"array_empty", true, nil},
		}

		baseMetric := createProtoMetric(t, testBaseMessage)
		doTestSimpleForParser(t, protoName, "GetInt64", testCases, baseMetric)
	})
}

func TestProtoGetInt8(t *testing.T) {
	t.Parallel()
	method := "GetInt8"

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		minNumCases := []SimpleCase{
			{"num_int32", false, int8(math.MinInt8)},
			{"num_int64", false, int8(math.MinInt8)},
			{"num_uint32", false, int8(0)},
			{"num_uint64", false, int8(0)},
		}
		minNumMetric := createProtoMetric(t, testMinNumMessage)
		doTestSimpleForParser(t, protoName, method, minNumCases, minNumMetric)
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_int32", false, int8(math.MaxInt8)},
			{"num_int64", false, int8(math.MaxInt8)},
			{"num_uint32", false, int8(math.MaxInt8)},
			{"num_uint64", false, int8(math.MaxInt8)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, method, maxNumCases, maxNumMetric)
	})
}

func TestProtoGetInt16(t *testing.T) {
	t.Parallel()
	method := "GetInt16"

	t.Run("min values", func(t *testing.T) {
		t.Parallel()

		minNumCases := []SimpleCase{
			{"num_int32", false, int16(math.MinInt16)},
			{"num_int64", false, int16(math.MinInt16)},
			{"num_uint32", false, int16(0)},
			{"num_uint64", false, int16(0)},
		}
		minNumMetric := createProtoMetric(t, testMinNumMessage)
		doTestSimpleForParser(t, protoName, method, minNumCases, minNumMetric)
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_int32", false, int16(math.MaxInt16)},
			{"num_int64", false, int16(math.MaxInt16)},
			{"num_uint32", false, int16(math.MaxInt16)},
			{"num_uint64", false, int16(math.MaxInt16)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, method, maxNumCases, maxNumMetric)
	})
}

func TestProtoGetInt32(t *testing.T) {
	t.Parallel()
	method := "GetInt32"

	t.Run("min values", func(t *testing.T) {
		t.Parallel()

		minNumCases := []SimpleCase{
			{"num_int32", false, int32(math.MinInt32)},
			{"num_int64", false, int32(math.MinInt32)},
			{"num_uint32", false, int32(0)},
			{"num_uint64", false, int32(0)},
		}
		minNumMetric := createProtoMetric(t, testMinNumMessage)
		doTestSimpleForParser(t, protoName, method, minNumCases, minNumMetric)
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_int32", false, int32(math.MaxInt32)},
			{"num_int64", false, int32(math.MaxInt32)},
			{"num_uint32", false, int32(math.MaxInt32)},
			{"num_uint64", false, int32(math.MaxInt32)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, method, maxNumCases, maxNumMetric)
	})
}

func TestProtoGetInt64(t *testing.T) {
	t.Parallel()
	method := "GetInt64"

	t.Run("min values", func(t *testing.T) {
		t.Parallel()

		minNumCases := []SimpleCase{
			{"num_int32", false, int64(math.MinInt32)},
			{"num_int64", false, int64(math.MinInt64)},
			{"num_uint32", false, int64(0)},
			{"num_uint64", false, int64(0)},
		}
		minNumMetric := createProtoMetric(t, testMinNumMessage)
		doTestSimpleForParser(t, protoName, method, minNumCases, minNumMetric)
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_int32", false, int64(math.MaxInt32)},
			{"num_int64", false, int64(math.MaxInt64)},
			{"num_uint32", false, int64(math.MaxUint32)},
			{"num_uint64", false, int64(math.MaxInt64)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, method, maxNumCases, maxNumMetric)
	})
}

func TestProtoGetUint(t *testing.T) {
	t.Parallel()

	t.Run("all types", func(t *testing.T) {
		t.Parallel()

		testCases := []SimpleCase{
			// nullable: false
			{"not_exist", false, uint64(0)},
			{"null", false, uint64(0)},
			{"bool_true", false, uint64(1)},
			{"bool_false", false, uint64(0)},
			{"num_int32", false, uint64(123)},
			{"num_int64", false, uint64(123)},
			{"num_float", false, uint64(0)},
			{"num_double", false, uint64(0)},
			{"num_uint32", false, uint64(123)},
			{"num_uint64", false, uint64(123)},
			{"str", false, uint64(0)},
			{"timestamp", false, uint64(0)},
			{"obj", false, uint64(0)},
			{"array_empty", false, uint64(0)},
			// nullable: true
			{"not_exist", true, nil},
			{"null", true, nil},
			{"bool_true", true, uint64(1)},
			{"bool_false", true, uint64(0)},
			{"num_int32", true, uint64(123)},
			{"num_int64", true, uint64(123)},
			{"num_float", true, nil},
			{"num_double", true, nil},
			{"num_uint32", true, uint64(123)},
			{"num_uint64", true, uint64(123)},
			{"str", true, nil},
			{"timestamp", true, nil},
			{"obj", true, nil},
			{"array_empty", true, nil},
		}

		baseMetric := createProtoMetric(t, testBaseMessage)
		doTestSimpleForParser(t, protoName, "GetUint64", testCases, baseMetric)
	})
}

func TestProtoGetUint8(t *testing.T) {
	t.Parallel()
	method := "GetUint8"

	t.Run("min values", func(t *testing.T) {
		t.Parallel()

		minNumCases := []SimpleCase{
			{"num_int32", false, uint8(0)},
			{"num_int64", false, uint8(0)},
			{"num_uint32", false, uint8(0)},
			{"num_uint64", false, uint8(0)},
		}
		minNumMetric := createProtoMetric(t, testMinNumMessage)
		doTestSimpleForParser(t, protoName, method, minNumCases, minNumMetric)
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_int32", false, uint8(math.MaxUint8)},
			{"num_int64", false, uint8(math.MaxUint8)},
			{"num_uint32", false, uint8(math.MaxUint8)},
			{"num_uint64", false, uint8(math.MaxUint8)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, method, maxNumCases, maxNumMetric)
	})
}

func TestProtoGetUint16(t *testing.T) {
	t.Parallel()
	method := "GetUint16"

	t.Run("min values", func(t *testing.T) {
		t.Parallel()

		minNumCases := []SimpleCase{
			{"num_int32", false, uint16(0)},
			{"num_int64", false, uint16(0)},
			{"num_uint32", false, uint16(0)},
			{"num_uint64", false, uint16(0)},
		}
		minNumMetric := createProtoMetric(t, testMinNumMessage)
		doTestSimpleForParser(t, protoName, method, minNumCases, minNumMetric)
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_int32", false, uint16(math.MaxUint16)},
			{"num_int64", false, uint16(math.MaxUint16)},
			{"num_uint32", false, uint16(math.MaxUint16)},
			{"num_uint64", false, uint16(math.MaxUint16)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, method, maxNumCases, maxNumMetric)
	})
}

func TestProtoGetUint32(t *testing.T) {
	t.Parallel()
	method := "GetUint32"

	t.Run("min values", func(t *testing.T) {
		t.Parallel()

		minNumCases := []SimpleCase{
			{"num_int32", false, uint32(0)},
			{"num_int64", false, uint32(0)},
			{"num_uint32", false, uint32(0)},
			{"num_uint64", false, uint32(0)},
		}
		minNumMetric := createProtoMetric(t, testMinNumMessage)
		doTestSimpleForParser(t, protoName, method, minNumCases, minNumMetric)
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_int32", false, uint32(math.MaxInt32)},
			{"num_int64", false, uint32(math.MaxUint32)},
			{"num_uint32", false, uint32(math.MaxUint32)},
			{"num_uint64", false, uint32(math.MaxUint32)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, method, maxNumCases, maxNumMetric)
	})
}

func TestProtoGetUint64(t *testing.T) {
	t.Parallel()
	method := "GetUint64"

	t.Run("min values", func(t *testing.T) {
		t.Parallel()

		minNumCases := []SimpleCase{
			{"num_int32", false, uint64(0)},
			{"num_int64", false, uint64(0)},
			{"num_uint32", false, uint64(0)},
			{"num_uint64", false, uint64(0)},
		}
		minNumMetric := createProtoMetric(t, testMinNumMessage)
		doTestSimpleForParser(t, protoName, method, minNumCases, minNumMetric)
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_int32", false, uint64(math.MaxInt32)},
			{"num_int64", false, uint64(math.MaxInt64)},
			{"num_uint32", false, uint64(math.MaxUint32)},
			{"num_uint64", false, uint64(math.MaxUint64)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, method, maxNumCases, maxNumMetric)
	})
}

func TestProtoGetFloat(t *testing.T) {
	t.Parallel()

	t.Run("all types", func(t *testing.T) {
		t.Parallel()

		testCases := []SimpleCase{
			// nullable: false
			{"not_exist", false, 0.0},
			{"null", false, 0.0},
			{"bool_true", false, 0.0},
			{"bool_false", false, 0.0},
			{"num_int32", false, 0.0},
			{"num_int64", false, 0.0},
			{"num_float", false, 123.321},
			{"num_double", false, 1234.4321},
			{"num_uint32", false, 0.0},
			{"num_uint64", false, 0.0},
			{"str", false, 0.0},
			{"timestamp", false, 0.0},
			{"obj", false, 0.0},
			{"array_empty", false, 0.0},
			// nullable: true
			{"not_exist", true, nil},
			{"null", true, nil},
			{"bool_true", true, nil},
			{"bool_false", true, nil},
			{"num_int32", true, nil},
			{"num_int64", true, nil},
			{"num_float", true, 123.321},
			{"num_double", true, 1234.4321},
			{"num_uint32", true, nil},
			{"num_uint64", true, nil},
			{"str", true, nil},
			{"timestamp", true, nil},
			{"obj", true, nil},
			{"array_empty", true, nil},
		}

		metric := createProtoMetric(t, testBaseMessage)
		for i := range testCases {
			tc := &testCases[i]

			desc := testCaseDescription(protoName, "GetFloat64", tc.Field, tc.Nullable)
			v := metric.GetFloat64(tc.Field, tc.Nullable)
			if v == nil {
				require.Equal(t, tc.ExpVal, v, desc)
			} else {
				require.InDelta(t, tc.ExpVal, v, 1e-6, desc)
			}
		}
	})
}

func TestProtoGetFloat32(t *testing.T) {
	t.Parallel()

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_float", false, float32(math.MaxFloat32)},
			{"num_double", false, float32(math.MaxFloat32)},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, "GetFloat32", maxNumCases, maxNumMetric)
	})
}

func TestProtoGetFloat64(t *testing.T) {
	t.Parallel()

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		maxNumCases := []SimpleCase{
			{"num_float", false, math.MaxFloat32},
			{"num_double", false, math.MaxFloat64},
		}
		maxNumMetric := createProtoMetric(t, testMaxNumMessage)
		doTestSimpleForParser(t, protoName, "GetFloat64", maxNumCases, maxNumMetric)
	})
}

func TestProtoGetDateTime(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, Epoch},
		{"null", false, Epoch},
		{"bool_true", false, Epoch},
		{"bool_false", false, Epoch},
		{"num_int32", false, Epoch},
		{"num_int64", false, Epoch},
		{"num_float", false, Epoch},
		{"num_double", false, Epoch},
		{"num_uint32", false, Epoch},
		{"num_uint64", false, Epoch},
		{"str", false, Epoch},
		{"timestamp", false, testDate},
		{"obj", false, Epoch},
		{"array_empty", false, Epoch},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, nil},
		{"bool_false", true, nil},
		{"num_int32", true, nil},
		{"num_int64", true, nil},
		{"num_float", true, nil},
		{"num_double", true, nil},
		{"num_fixed32", true, nil},
		{"num_fixed64", true, nil},
		{"str", true, nil},
		{"timestamp", true, testDate},
		{"obj", true, nil},
		{"array_empty", true, nil},
	}

	metric := createProtoMetric(t, testBaseMessage)
	doTestSimpleForParser(t, protoName, "GetDateTime", testCases, metric)
}

func TestProtoGetArray(t *testing.T) {
	t.Parallel()

	testCases := []ArrayCase{
		{"array_empty", model.Bool, []bool{}},
		{"array_empty", model.Int64, []int64{}},
		{"array_empty", model.Float64, []float64{}},
		{"array_empty", model.Decimal, []decimal.Decimal{}},
		{"array_empty", model.String, []string{}},
		{"array_empty", model.DateTime, []time.Time{}},

		{"array_bool", model.Bool, []bool{true, false}},
		{"array_num_int32", model.Int8, []int8{-123, 0, 123}},
		{"array_num_int32", model.Int16, []int16{-123, 0, 123}},
		{"array_num_int32", model.Int32, []int32{-123, 0, 123}},
		{"array_num_int32", model.Int64, []int64{-123, 0, 123}},
		{"array_num_int64", model.Int64, []int64{-123, 0, 123}},
		{"array_num_uint32", model.UInt8, []uint8{0, 123}},
		{"array_num_uint32", model.UInt16, []uint16{0, 123}},
		{"array_num_uint32", model.UInt32, []uint32{0, 123}},
		{"array_num_uint32", model.UInt64, []uint64{0, 123}},
		{"array_num_uint64", model.UInt64, []uint64{0, 123}},
		{"array_num_float", model.Float32, []float32{0, 1.0}},
		{"array_num_double", model.Float64, []float64{0, 1.0}},
		{"array_num_float", model.Decimal, []decimal.Decimal{decimal.NewFromFloat(0), decimal.NewFromFloat(1.0)}},
		{"array_str", model.String, []string{"aa", "bb", "cc"}},
		{"array_timestamp", model.DateTime, []time.Time{testDate}},
	}

	metric := createProtoMetric(t, testBaseMessage)
	for i := range testCases {
		tc := &testCases[i]

		desc := fmt.Sprintf(`%s.GetArray("%s", %s)`, protoName, tc.Field, model.GetTypeName(tc.Type))
		v := metric.GetArray(tc.Field, tc.Type)
		require.Equal(t, tc.ExpVal, v, desc)
	}
}

func TestProtoGetUUID(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, zeroUUID},
		{"uuid", false, "2211a6ec-3799-41c1-ac41-4ab02f8e3cf2"},
		{"array_uuid", false, "[2211a6ec-3799-41c1-ac41-4ab02f8e3cf2 f6acf2ad-757a-4eb3-96b2-6c3d6f2bec6e]"},
		{"array_empty", false, "[]"},
		// nullable: true
		{"not_exist", true, nil},
		{"uuid", true, "2211a6ec-3799-41c1-ac41-4ab02f8e3cf2"},
		{"array_uuid", true, "[2211a6ec-3799-41c1-ac41-4ab02f8e3cf2 f6acf2ad-757a-4eb3-96b2-6c3d6f2bec6e]"},
		{"array_empty", true, "[]"},
	}

	metric := createProtoMetric(t, testBaseMessage)
	doTestSimpleForParser(t, protoName, "GetUUID", testCases, metric)
}

func TestProtoGetIPv4(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, net.IPv4zero.String()},
		{"ipv4", false, "1.2.3.4"},
		{"array_ipv4", false, "[1.2.3.4 2.3.4.5]"},
		{"array_empty", false, "[]"},
		// nullable: true
		{"not_exist", true, nil},
		{"ipv4", true, "1.2.3.4"},
		{"array_ipv4", true, "[1.2.3.4 2.3.4.5]"},
		{"array_empty", true, "[]"},
	}

	metric := createProtoMetric(t, testBaseMessage)
	doTestSimpleForParser(t, protoName, "GetIPv4", testCases, metric)
}

func TestProtoGetIPv6(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, net.IPv6zero.String()},
		{"ipv6", false, "fe80::74e6:b5f3:fe92:830e"},
		{"array_ipv6", false, "[fe80::74e6:b5f3:fe92:830e fe80::2a3:aeff:fe53:743e]"},
		{"array_empty", false, "[]"},
		// nullable: true
		{"not_exist", true, nil},
		{"ipv6", true, "fe80::74e6:b5f3:fe92:830e"},
		{"array_ipv6", true, "[fe80::74e6:b5f3:fe92:830e fe80::2a3:aeff:fe53:743e]"},
		{"array_empty", true, "[]"},
	}

	metric := createProtoMetric(t, testBaseMessage)
	doTestSimpleForParser(t, protoName, "GetIPv6", testCases, metric)
}
