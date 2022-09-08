package sdk

type value struct {
	EmptyString string
	Zero        int
	Zero32      int32
	Zero64      int64
	ZeroFloat32 float32
	ZeroFloat64 float64
	True        bool
	False       bool
}

var Value = value{
	EmptyString: "",
	Zero:        0,
	Zero32:      0,
	Zero64:      0,
	ZeroFloat32: 0,
	ZeroFloat64: 0,
	True:        true,
	False:       false,
}
