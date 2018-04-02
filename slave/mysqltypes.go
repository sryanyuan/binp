package slave

import (
	"encoding/binary"
)

// CapabilityFlags
// https://dev.mysql.com/doc/internals/en/capability-flags.html
const (
	// ClientConnectWithDB can contains a database name in handshake response
	ClientConnectWithDB = 0x00000008
	// ClientPluginAuth represents client supports authentication plugins.
	ClientPluginAuth = 0x00080000
	// ClientSecureConnection represents client supports Authentication::Native41
	ClientSecureConnection = 0x00008000
	// ClientProtocol41 represents server supports the 4.1 protocol
	ClientProtocol41 = 0x00000200
)

// Charset SELECT id, collation_name FROM information_schema.collations ORDER BY id;
// https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
// Not all charset ...
const (
	_ = iota
	CharsetBig5ChineseCI
	CharsetLatin2CzechCS
	CharsetDec8SwedishCI
	CharsetCp850GeneralCI
	CharsetGerman1CI
	CharsetHp8EnglishCI
	CharsetKoi8rGeneralCI
	CharsetLatin1SwedishCI
	CharsetLatin2GeneralCI
	CharsetSwe7SwedishCI
	CharsetAsciiGeneralCI
	CharsetUjisJapaneseCI
	CharsetSjisJapaneseCI
	CharsetCp1251BulgarianCI
	CharsetLatin1DanishCI
	CharsetHebrewGeneralCI
	CharsetTis620ThaiCI
	CharsetEuckrKoreanCI
	CharsetLatin7EstonianCS
	CharsetLatin2HungarianCI
	CharsetKoi8uGeneralCI
	CharsetCp1251UkrainianCI
	CharsetGb2312ChineseCI
	CharsetGreekGeneralCI
	CharsetCp1250GeneralCI
	CharsetLatin2CroatianCI
	CharsetGbkChineseCI
	CharsetCp1257LithuanianCI
	CharsetLatin5KurkishCI
	CharsetLatin1German2CI
	CharsetArmscii8GeneralCI
	CharsetUtf8GeneralCI
)

const (
	MySQLNativePasswordPlugin = "mysql_native_password"
)

// LenencInt is a length encoded integer type
// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
type LenencInt struct {
	Flag  uint8
	Value uint64
	EOF   bool
}

// FromData reads value from a binary data
func (i *LenencInt) FromData(data []byte) int {
	flag := data[0]
	i.Flag = uint8(flag)

	parsed := 0
	var buf [8]byte

	switch {
	case flag < 0xfb:
		{
			i.Value = uint64(data[1])
			parsed = 2
		}
	case flag == 0xfc:
		{
			i.Value = uint64(binary.LittleEndian.Uint16(data[1:3]))
			parsed = 3
		}
	case flag == 0xfd:
		{
			copy(buf[:], data[1:4])
			i.Value = uint64(binary.LittleEndian.Uint32(buf[:]))
			parsed = 4
		}
	case flag == 0xfe:
		{
			if len(data) < 9 {
				// EOF
				i.Value = 0
				parsed = 1
				i.EOF = true
			} else {
				copy(buf[:], data[1:9])
				i.Value = binary.LittleEndian.Uint64(buf[:])
				parsed = 9
			}
		}
	}

	return parsed
}

// Get gets the lenenc int number as normal number
func (i *LenencInt) Get() uint64 {
	return i.Value
}

// Set stores normal number in lenenc number format
func (i *LenencInt) Set(v uint64) {
	var buf [8]byte
	switch {
	case v < 0xfb:
		{
			buf[0] = 0xfb
			buf[1] = byte(v)
		}
	case v >= 0xfb && v < 1<<16:
		{
			buf[0] = 0xfc
			binary.LittleEndian.PutUint16(buf[1:], uint16(v))
		}
	case v >= 1<<16 && v < 1<<24:
		{
			buf[1] = 0xfd
			binary.LittleEndian.PutUint32(buf[1:], uint32(v))
		}
	case v >= 1<<24 && v <= (1<<64-1):
		{
			buf[1] = 0xfe
			binary.LittleEndian.PutUint64(buf[1:], v)
		}
	}
	i.FromData(buf[:])
}

type LenencString struct {
	Len   LenencInt
	Value string
	EOF   bool
}

func (s *LenencString) FromData(data []byte) int {
	// Parse length first
	parsed := s.Len.FromData(data)
	if s.Len.EOF {
		s.EOF = true
		return parsed
	}
	l := s.Len.Get()
	if uint64(len(data)-parsed) < l {
		s.EOF = true
		return parsed
	}
	s.Value = string(data[1 : 1+l])
	return parsed + int(l)
}
