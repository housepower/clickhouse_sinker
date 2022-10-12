package goetty

// RawDecoder decoder raw byte array
type RawDecoder struct {
}

// Decode decode with raw byte array
func (decoder *RawDecoder) Decode(in *ByteBuf) (bool, interface{}, error) {
	_, data, err := in.ReadMarkedBytes()

	if err != nil {
		return true, data, err
	}

	return true, data, nil
}

type rawEncoder struct {
}

// NewRawEncoder returns a encoder to encode raw byte array
func NewRawEncoder() Encoder {
	return &rawEncoder{}
}

func (encoder *rawEncoder) Encode(data interface{}, out *ByteBuf) error {
	out.Write(data.([]byte))
	return nil
}
