package mconn

import (
	"encoding/binary"
)

// CapabilityFlags
// https://dev.mysql.com/doc/internals/en/capability-flags.html
const (
	clientLongPassword = 0x00000001
	clientLongFlag     = 0x00000004
	// clientConnectWithDB can contains a database name in handshake response
	clientConnectWithDB = 0x00000008
	// clientProtocol41 represents server supports the 4.1 protocol
	clientProtocol41 = 0x00000200
	// clientTransactions, server can send status flags in EOF_Packet.
	clientTransactions = 0x00002000
	// clientSecureConnection represents client supports Authentication::Native41
	clientSecureConnection = 0x00008000
	// clientPluginAuth represents client supports authentication plugins.
	clientPluginAuth = 0x00080000
	// clientSessionTrace can contains session-state changes after OK packet if server set the flag
	clientSessionTrace = 0x00800000
)

const (
	PacketHeaderOK          = 0x00
	PacketHeaderEOF         = 0xfe
	PacketHeaderLocalInFile = 0xfb
	PacketHeaderERR         = 0xff
)

//
const (
	// https://dev.mysql.com/doc/internals/en/com-query.html
	comQuery = 0x03
	// https://dev.mysql.com/doc/internals/en/com-register-slave.html
	comRegisterSlave = 0x15
	// https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
	comBinlogDump = 0x12
)

// Charset SELECT id, collation_name FROM information_schema.collations ORDER BY id;
// https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
// Not all charset ...
const (
	_                         = iota
	CharsetBig5ChineseCI      // 1
	CharsetLatin2CzechCS      // 2
	CharsetDec8SwedishCI      // 3
	CharsetCp850GeneralCI     // 4
	CharsetGerman1CI          // 5
	CharsetHp8EnglishCI       // 6
	CharsetKoi8rGeneralCI     // 7
	CharsetLatin1SwedishCI    // 8
	CharsetLatin2GeneralCI    // 9
	CharsetSwe7SwedishCI      // 10
	CharsetAsciiGeneralCI     // 11
	CharsetUjisJapaneseCI     // 12
	CharsetSjisJapaneseCI     // 13
	CharsetCp1251BulgarianCI  // 14
	CharsetLatin1DanishCI     // 15
	CharsetHebrewGeneralCI    // 16
	CharsetMissing1           // 17
	CharsetTis620ThaiCI       // 18
	CharsetEuckrKoreanCI      // 19
	CharsetLatin7EstonianCS   // 20
	CharsetLatin2HungarianCI  // 21
	CharsetKoi8uGeneralCI     // 22
	CharsetCp1251UkrainianCI  // 23
	CharsetGb2312ChineseCI    // 24
	CharsetGreekGeneralCI     // 25
	CharsetCp1250GeneralCI    // 26
	CharsetLatin2CroatianCI   // 27
	CharsetGbkChineseCI       // 28
	CharsetCp1257LithuanianCI // 29
	CharsetLatin5KurkishCI    // 30
	CharsetLatin1German2CI    // 31
	CharsetArmscii8GeneralCI  // 32
	CharsetUtf8GeneralCI      // 33
)

const (
	// MySQLNativePasswordPlugin is the mysql native password auth type
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
			i.Value = uint64(data[0])
			parsed = 1
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
			buf[0] = byte(v)
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

// LenencString is a length prefixed string
type LenencString struct {
	Len   LenencInt
	Value string
	EOF   bool
}

// FromData parse binary data to lenenc string
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
