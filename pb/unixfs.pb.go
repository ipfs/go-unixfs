// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: unixfs.proto

package unixfs_pb

import (
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type Data_DataType int32

const (
	Data_Raw       Data_DataType = 0
	Data_Directory Data_DataType = 1
	Data_File      Data_DataType = 2
	Data_Metadata  Data_DataType = 3
	Data_Symlink   Data_DataType = 4
	Data_HAMTShard Data_DataType = 5
)

var Data_DataType_name = map[int32]string{
	0: "Raw",
	1: "Directory",
	2: "File",
	3: "Metadata",
	4: "Symlink",
	5: "HAMTShard",
}

var Data_DataType_value = map[string]int32{
	"Raw":       0,
	"Directory": 1,
	"File":      2,
	"Metadata":  3,
	"Symlink":   4,
	"HAMTShard": 5,
}

func (x Data_DataType) Enum() *Data_DataType {
	p := new(Data_DataType)
	*p = x
	return p
}

func (x Data_DataType) String() string {
	return proto.EnumName(Data_DataType_name, int32(x))
}

func (x *Data_DataType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Data_DataType_value, data, "Data_DataType")
	if err != nil {
		return err
	}
	*x = Data_DataType(value)
	return nil
}

func (Data_DataType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_e2fd76cc44dfc7c3, []int{0, 0}
}

type Data struct {
	Type       *Data_DataType `protobuf:"varint,1,req,name=Type,enum=unixfs.pb.Data_DataType" json:"Type,omitempty"`
	Data       []byte         `protobuf:"bytes,2,opt,name=Data" json:"Data,omitempty"`
	Filesize   *uint64        `protobuf:"varint,3,opt,name=filesize" json:"filesize,omitempty"`
	Blocksizes []uint64       `protobuf:"varint,4,rep,name=blocksizes" json:"blocksizes,omitempty"`
	HashType   *uint64        `protobuf:"varint,5,opt,name=hashType" json:"hashType,omitempty"`
	Fanout     *uint64        `protobuf:"varint,6,opt,name=fanout" json:"fanout,omitempty"`
	// unixfs v1.5 metadata additions
	Mode                 *uint32   `protobuf:"varint,7,opt,name=mode" json:"mode,omitempty"`
	Mtime                *UnixTime `protobuf:"bytes,8,opt,name=mtime" json:"mtime,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *Data) Reset()         { *m = Data{} }
func (m *Data) String() string { return proto.CompactTextString(m) }
func (*Data) ProtoMessage()    {}
func (*Data) Descriptor() ([]byte, []int) {
	return fileDescriptor_e2fd76cc44dfc7c3, []int{0}
}
func (m *Data) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Data.Unmarshal(m, b)
}
func (m *Data) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Data.Marshal(b, m, deterministic)
}
func (m *Data) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Data.Merge(m, src)
}
func (m *Data) XXX_Size() int {
	return xxx_messageInfo_Data.Size(m)
}
func (m *Data) XXX_DiscardUnknown() {
	xxx_messageInfo_Data.DiscardUnknown(m)
}

var xxx_messageInfo_Data proto.InternalMessageInfo

func (m *Data) GetType() Data_DataType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return Data_Raw
}

func (m *Data) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Data) GetFilesize() uint64 {
	if m != nil && m.Filesize != nil {
		return *m.Filesize
	}
	return 0
}

func (m *Data) GetBlocksizes() []uint64 {
	if m != nil {
		return m.Blocksizes
	}
	return nil
}

func (m *Data) GetHashType() uint64 {
	if m != nil && m.HashType != nil {
		return *m.HashType
	}
	return 0
}

func (m *Data) GetFanout() uint64 {
	if m != nil && m.Fanout != nil {
		return *m.Fanout
	}
	return 0
}

func (m *Data) GetMode() uint32 {
	if m != nil && m.Mode != nil {
		return *m.Mode
	}
	return 0
}

func (m *Data) GetMtime() *UnixTime {
	if m != nil {
		return m.Mtime
	}
	return nil
}

type Metadata struct {
	MimeType             *string  `protobuf:"bytes,1,opt,name=MimeType" json:"MimeType,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Metadata) Reset()         { *m = Metadata{} }
func (m *Metadata) String() string { return proto.CompactTextString(m) }
func (*Metadata) ProtoMessage()    {}
func (*Metadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_e2fd76cc44dfc7c3, []int{1}
}
func (m *Metadata) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Metadata.Unmarshal(m, b)
}
func (m *Metadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Metadata.Marshal(b, m, deterministic)
}
func (m *Metadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Metadata.Merge(m, src)
}
func (m *Metadata) XXX_Size() int {
	return xxx_messageInfo_Metadata.Size(m)
}
func (m *Metadata) XXX_DiscardUnknown() {
	xxx_messageInfo_Metadata.DiscardUnknown(m)
}

var xxx_messageInfo_Metadata proto.InternalMessageInfo

func (m *Metadata) GetMimeType() string {
	if m != nil && m.MimeType != nil {
		return *m.MimeType
	}
	return ""
}

type UnixTime struct {
	Seconds               *int64   `protobuf:"varint,1,req,name=Seconds" json:"Seconds,omitempty"`
	FractionalNanoseconds *uint32  `protobuf:"fixed32,2,opt,name=FractionalNanoseconds" json:"FractionalNanoseconds,omitempty"`
	XXX_NoUnkeyedLiteral  struct{} `json:"-"`
	XXX_unrecognized      []byte   `json:"-"`
	XXX_sizecache         int32    `json:"-"`
}

func (m *UnixTime) Reset()         { *m = UnixTime{} }
func (m *UnixTime) String() string { return proto.CompactTextString(m) }
func (*UnixTime) ProtoMessage()    {}
func (*UnixTime) Descriptor() ([]byte, []int) {
	return fileDescriptor_e2fd76cc44dfc7c3, []int{2}
}
func (m *UnixTime) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_UnixTime.Unmarshal(m, b)
}
func (m *UnixTime) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_UnixTime.Marshal(b, m, deterministic)
}
func (m *UnixTime) XXX_Merge(src proto.Message) {
	xxx_messageInfo_UnixTime.Merge(m, src)
}
func (m *UnixTime) XXX_Size() int {
	return xxx_messageInfo_UnixTime.Size(m)
}
func (m *UnixTime) XXX_DiscardUnknown() {
	xxx_messageInfo_UnixTime.DiscardUnknown(m)
}

var xxx_messageInfo_UnixTime proto.InternalMessageInfo

func (m *UnixTime) GetSeconds() int64 {
	if m != nil && m.Seconds != nil {
		return *m.Seconds
	}
	return 0
}

func (m *UnixTime) GetFractionalNanoseconds() uint32 {
	if m != nil && m.FractionalNanoseconds != nil {
		return *m.FractionalNanoseconds
	}
	return 0
}

func init() {
	proto.RegisterEnum("unixfs.pb.Data_DataType", Data_DataType_name, Data_DataType_value)
	proto.RegisterType((*Data)(nil), "unixfs.pb.Data")
	proto.RegisterType((*Metadata)(nil), "unixfs.pb.Metadata")
	proto.RegisterType((*UnixTime)(nil), "unixfs.pb.UnixTime")
}

func init() { proto.RegisterFile("unixfs.proto", fileDescriptor_e2fd76cc44dfc7c3) }

var fileDescriptor_e2fd76cc44dfc7c3 = []byte{
	// 335 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x90, 0x41, 0x6f, 0xe2, 0x30,
	0x10, 0x85, 0x37, 0x89, 0x21, 0x61, 0x80, 0x55, 0x34, 0xab, 0x5d, 0x59, 0x7b, 0xa8, 0xa2, 0x1c,
	0x2a, 0x57, 0xaa, 0x38, 0xa0, 0xfe, 0x81, 0x4a, 0x08, 0xf5, 0x42, 0x0f, 0x86, 0xf6, 0xd0, 0x9b,
	0x49, 0x8c, 0xb0, 0x48, 0x6c, 0x94, 0x18, 0x15, 0xfa, 0x27, 0xfb, 0x97, 0x2a, 0x27, 0x84, 0x72,
	0xe8, 0xc5, 0xf2, 0xe7, 0xf7, 0x9e, 0x35, 0xf3, 0x60, 0x74, 0xd0, 0xea, 0xb8, 0xa9, 0x27, 0xfb,
	0xca, 0x58, 0x83, 0x83, 0x8e, 0xd6, 0xe9, 0xa7, 0x0f, 0x64, 0x26, 0xac, 0xc0, 0x7b, 0x20, 0xab,
	0xd3, 0x5e, 0x52, 0x2f, 0xf1, 0xd9, 0xef, 0x29, 0x9d, 0x5c, 0x2c, 0x13, 0x27, 0x37, 0x87, 0xd3,
	0x79, 0xe3, 0x42, 0x6c, 0x53, 0xd4, 0x4f, 0x3c, 0x36, 0xe2, 0xed, 0x0f, 0xff, 0x21, 0xda, 0xa8,
	0x42, 0xd6, 0xea, 0x43, 0xd2, 0x20, 0xf1, 0x18, 0xe1, 0x17, 0xc6, 0x1b, 0x80, 0x75, 0x61, 0xb2,
	0x9d, 0x83, 0x9a, 0x92, 0x24, 0x60, 0x84, 0x5f, 0xbd, 0xb8, 0xec, 0x56, 0xd4, 0xdb, 0x66, 0x82,
	0x5e, 0x9b, 0xed, 0x18, 0xff, 0x41, 0x7f, 0x23, 0xb4, 0x39, 0x58, 0xda, 0x6f, 0x94, 0x33, 0xb9,
	0x19, 0x4a, 0x93, 0x4b, 0x1a, 0x26, 0x1e, 0x1b, 0xf3, 0xe6, 0x8e, 0x77, 0xd0, 0x2b, 0xad, 0x2a,
	0x25, 0x8d, 0x12, 0x8f, 0x0d, 0xa7, 0x7f, 0xae, 0xd6, 0x78, 0xd1, 0xea, 0xb8, 0x52, 0xa5, 0xe4,
	0xad, 0x23, 0x7d, 0x85, 0xa8, 0x5b, 0x0a, 0x43, 0x08, 0xb8, 0x78, 0x8f, 0x7f, 0xe1, 0x18, 0x06,
	0x33, 0x55, 0xc9, 0xcc, 0x9a, 0xea, 0x14, 0x7b, 0x18, 0x01, 0x99, 0xab, 0x42, 0xc6, 0x3e, 0x8e,
	0x20, 0x5a, 0x48, 0x2b, 0x72, 0x61, 0x45, 0x1c, 0xe0, 0x10, 0xc2, 0xe5, 0xa9, 0x2c, 0x94, 0xde,
	0xc5, 0xc4, 0x65, 0x9e, 0x1e, 0x17, 0xab, 0xe5, 0x56, 0x54, 0x79, 0xdc, 0x4b, 0x6f, 0xbf, 0x9d,
	0x6e, 0xad, 0x85, 0x2a, 0xe5, 0xb9, 0x58, 0x8f, 0x0d, 0xf8, 0x85, 0xd3, 0x37, 0x88, 0xba, 0x91,
	0x90, 0x42, 0xb8, 0x94, 0x99, 0xd1, 0x79, 0xdd, 0xf4, 0x1f, 0xf0, 0x0e, 0xf1, 0x01, 0xfe, 0xce,
	0x2b, 0x91, 0x59, 0x65, 0xb4, 0x28, 0x9e, 0x85, 0x36, 0xf5, 0xd9, 0xe7, 0x9a, 0x0f, 0xf9, 0xcf,
	0xe2, 0x57, 0x00, 0x00, 0x00, 0xff, 0xff, 0x1f, 0xc3, 0xf5, 0x68, 0xef, 0x01, 0x00, 0x00,
}
