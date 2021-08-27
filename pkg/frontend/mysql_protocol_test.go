package frontend

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/fagongzi/goetty"
	"matrixone/pkg/config"
	"matrixone/pkg/defines"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/host"
)

type TestRoutineManager struct {
	rwlock  sync.RWMutex
	clients map[goetty.IOSession]*Routine

	pu *config.ParameterUnit
}

func (tRM *TestRoutineManager) Created(rs goetty.IOSession) {
	IO := NewIOPackage(true)
	pro := NewMysqlClientProtocol(IO, nextConnectionID())
	exe := NewMysqlCmdExecutor()
	ses := NewSessionWithParameterUnit(tRM.pu)
	routine := NewRoutine(rs, pro, exe, ses)

	hsV10pkt := pro.makeHandshakeV10Payload()
	err := pro.writePackets(hsV10pkt)
	if err != nil {
		panic(err)
	}

	tRM.rwlock.Lock()
	defer tRM.rwlock.Unlock()
	tRM.clients[rs] = routine
}

func (tRM *TestRoutineManager) Closed(rs goetty.IOSession) {
	tRM.rwlock.Lock()
	defer tRM.rwlock.Unlock()
	delete(tRM.clients, rs)
}

func NewTestRoutineManager(pu *config.ParameterUnit) *TestRoutineManager {
	rm := &TestRoutineManager{
		clients: make(map[goetty.IOSession]*Routine),
		pu: pu,
	}
	return rm
}

func TestReadIntLenEnc(t *testing.T) {
	var intEnc MysqlProtocol
	var data = make([]byte, 24)
	var cases = [][]uint64{
		{0, 123, 250},
		{251, 10000, 1<<16 - 1},
		{1 << 16, 1<<16 + 10000, 1<<24 - 1},
		{1 << 24, 1<<24 + 10000, 1<<64 - 1},
	}
	var caseLens = []int{1, 3, 4, 9}
	for j := 0; j < len(cases); j++ {
		for i := 0; i < len(cases[j]); i++ {
			value := cases[j][i]
			p1 := intEnc.writeIntLenEnc(data, 0, value)
			val, p2, ok := intEnc.readIntLenEnc(data, 0)
			if !ok || p1 != caseLens[j] || p1 != p2 || val != value {
				t.Errorf("IntLenEnc %d failed.", value)
				break
			}
			val, p2, ok = intEnc.readIntLenEnc(data[0:caseLens[j]-1], 0)
			if ok {
				t.Errorf("read IntLenEnc failed.")
				break
			}
		}
	}
}

func TestReadCountOfBytes(t *testing.T) {
	var client MysqlProtocol
	var data = make([]byte, 24)
	var length = 10
	for i := 0; i < length; i++ {
		data[i] = byte(length - i)
	}

	r, pos, ok := client.readCountOfBytes(data, 0, length)
	if !ok || pos != length {
		t.Error("read bytes failed.")
		return
	}

	for i := 0; i < length; i++ {
		if r[i] != data[i] {
			t.Error("read != write")
			break
		}
	}

	r, pos, ok = client.readCountOfBytes(data, 0, 100)
	if ok {
		t.Error("read bytes failed.")
		return
	}

	r, pos, ok = client.readCountOfBytes(data, 0, 0)
	if !ok || pos != 0 {
		t.Error("read bytes failed.")
		return
	}
}

func TestReadStringFix(t *testing.T) {
	var client MysqlProtocol
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringFix(data, 0, s, length)
	if pos != length {
		t.Error("write string fix failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringFix(data, 0, length)
	if !ok || pos != length || x != s[0:length] {
		t.Error("read string fix failed.")
		return
	}
	var sLen = []int{
		length + 10,
		length + 20,
		length + 30,
	}
	for i := 0; i < len(sLen); i++ {
		x, pos, ok = client.readStringFix(data, 0, sLen[i])
		if ok && pos == sLen[i] && x == s[0:sLen[i]] {
			t.Error("read string fix failed.")
			return
		}
	}

	//empty string
	pos = client.writeStringFix(data, 0, s, 0)
	if pos != 0 {
		t.Error("write string fix failed.")
		return
	}

	x, pos, ok = client.readStringFix(data, 0, 0)
	if !ok || pos != 0 || x != "" {
		t.Error("read string fix failed.")
		return
	}
}

func TestReadStringNUL(t *testing.T) {
	var client MysqlProtocol
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringNUL(data, 0, s[0:length])
	if pos != length+1 {
		t.Error("write string NUL failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringNUL(data, 0)
	if !ok || pos != length+1 || x != s[0:length] {
		t.Error("read string NUL failed.")
		return
	}
	var sLen = []int{
		length + 10,
		length + 20,
		length + 30,
	}
	for i := 0; i < len(sLen); i++ {
		x, pos, ok = client.readStringNUL(data, 0)
		if ok && pos == sLen[i]+1 && x == s[0:sLen[i]] {
			t.Error("read string NUL failed.")
			return
		}
	}
}

func TestReadStringLenEnc(t *testing.T) {
	var client MysqlProtocol
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringLenEnc(data, 0, s[0:length])
	if pos != length+1 {
		t.Error("write string lenenc failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringLenEnc(data, 0)
	if !ok || pos != length+1 || x != s[0:length] {
		t.Error("read string lenenc failed.")
		return
	}

	//empty string
	pos = client.writeStringLenEnc(data, 0, s[0:0])
	if pos != 1 {
		t.Error("write string lenenc failed.")
		return
	}

	x, pos, ok = client.readStringLenEnc(data, 0)
	if !ok || pos != 1 || x != s[0:0] {
		t.Error("read string lenenc failed.")
		return
	}
}

func TestMysqlClientProtocol_Handshake(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connection method: mysql -h 127.0.0.1 -P 6001 -udump -p
	//echoServer(handshakeHandler)

	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	if err := config.LoadvarsConfigFromFile("../../system_vars_config.toml",
		&config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())
	config.Mempool = mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor()))
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes)

	ppu := NewPDCallbackParameterUnit(int(config.GlobalSystemVariables.GetPeriodOfEpochTimer()), int(config.GlobalSystemVariables.GetPeriodOfPersistence()), int(config.GlobalSystemVariables.GetPeriodOfDDLDeleteTimer()), int(config.GlobalSystemVariables.GetTimeoutOfHeartbeat()), config.GlobalSystemVariables.GetEnableEpochLogging())
	pci := NewPDCallbackImpl(ppu)
	pci.Id = 0
	rm := NewRoutineManager(pu, pci)

	encoder, decoder := NewSqlCodec()
	echoServer(rm.Handler, rm, encoder, decoder)
}

func makeMysqlTinyIntResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Tiny"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_TINY)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint8{0,1,254,255}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int8{-128,-127,127}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlTinyResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlTinyIntResultSet(unsigned))
}

func makeMysqlShortResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Short"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint16{0,1,254,255,65535}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int16{-32768,0,32767}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlShortResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlShortResultSet(unsigned))
}

func makeMysqlLongResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Long"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_LONG)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint32{0,4294967295}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int32{-2147483648,0,2147483647}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlLongResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlLongResultSet(unsigned))
}

func makeMysqlLongLongResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "LongLong"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint64{0,4294967295,18446744073709551615}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int64{-9223372036854775808,0,9223372036854775807}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlLongLongResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlLongLongResultSet(unsigned))
}

func makeMysqlInt24ResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Int24"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_INT24)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		//[0,16777215]
		var cases=[]uint32{0,16777215,4294967295}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		//[-8388608,8388607]
		var cases=[]int32{-2147483648,-8388608,0,8388607,2147483647}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlInt24Result(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlInt24ResultSet(unsigned))
}

func makeMysqlYearResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Year"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_YEAR)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint16{0,1,254,255,65535}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int16{-32768,0,32767}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlYearResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlYearResultSet(unsigned))
}

func makeMysqlVarcharResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varchar"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]string{"abc","abcde","","x-","xx"}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlVarcharResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlVarcharResultSet())
}

func makeMysqlVarStringResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varstring"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]string{"abc","abcde","","x-","xx"}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlVarStringResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlVarStringResultSet())
}

func makeMysqlStringResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "String"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]string{"abc","abcde","","x-","xx"}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlStringResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlStringResultSet())
}

func makeMysqlFloatResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Float"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_FLOAT)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]float32{math.MaxFloat32,math.SmallestNonzeroFloat32,-math.MaxFloat32,-math.SmallestNonzeroFloat32}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlFloatResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlFloatResultSet())
}

func makeMysqlDoubleResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Double"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_DOUBLE)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]float64{math.MaxFloat64,math.SmallestNonzeroFloat64,-math.MaxFloat64,-math.SmallestNonzeroFloat64}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlDoubleResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlDoubleResultSet())
}

func make8ColumnsResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	var columnTypes = []uint8{
		defines.MYSQL_TYPE_TINY,
		defines.MYSQL_TYPE_SHORT,
		defines.MYSQL_TYPE_LONG,
		defines.MYSQL_TYPE_LONGLONG,
		defines.MYSQL_TYPE_VARCHAR,
		defines.MYSQL_TYPE_FLOAT}

	var names=[]string{
		"Tiny",
		"Short",
		"Long",
		"Longlong",
		"Varchar",
		"Float",
	}

	var cases=[][]interface{}{
		{-128,-32768,-2147483648,-9223372036854775808,"abc",math.MaxFloat32},
		{-127,0,    0,0,"abcde",math.SmallestNonzeroFloat32},
		{127,32767,2147483647,9223372036854775807,"",-math.MaxFloat32},
		{126,32766,2147483646,9223372036854775806,"x-",-math.SmallestNonzeroFloat32},
	}

	for i,ct := range columnTypes{
		name := names[i]
		mysqlCol := new(MysqlColumn)
		mysqlCol.SetName(name)
		mysqlCol.SetOrgName(name + "OrgName")
		mysqlCol.SetColumnType(ct)
		mysqlCol.SetSchema(name + "Schema")
		mysqlCol.SetTable(name + "Table")
		mysqlCol.SetOrgTable(name + "Table")
		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

		rs.AddColumn(mysqlCol)
	}

	for _,v := range cases{
		rs.AddRow(v)
	}

	return rs
}

func makeMysql8ColumnsResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,make8ColumnsResultSet())
}

func makeMoreThan16MBResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	var columnTypes = []uint8{
		defines.MYSQL_TYPE_LONGLONG,
		defines.MYSQL_TYPE_DOUBLE,
		defines.MYSQL_TYPE_VARCHAR,
	}

	var names=[]string{
		"Longlong",
		"Double",
		"Varchar",
	}

	var rowCase =[]interface{}{9223372036854775807,math.MaxFloat64,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}

	for i,ct := range columnTypes{
		name := names[i]
		mysqlCol := new(MysqlColumn)
		mysqlCol.SetName(name)
		mysqlCol.SetOrgName(name + "OrgName")
		mysqlCol.SetColumnType(ct)
		mysqlCol.SetSchema(name + "Schema")
		mysqlCol.SetTable(name + "Table")
		mysqlCol.SetOrgTable(name + "Table")
		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

		rs.AddColumn(mysqlCol)
	}

	//the size of the total result set will be more than 16MB
	for i := 0 ; i < 40000; i++{
		rs.AddRow(rowCase)
	}

	return rs
}

//the size of resultset will be morethan 16MB
func makeMoreThan16MBResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMoreThan16MBResultSet())
}

func make16MBRowResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varstring"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	/*
		How to test the max size of the data in one packet that the client can received ?
		Environment: Mysql Version 8.0.23
		1. shell: mysql --help | grep allowed-packet
			something like:
			"
			  --max-allowed-packet=#
			max-allowed-packet                16777216
			"
			so, we get:
				max-allowed-packet means : The maximum packet length to send to or receive from server.
				default value : 16777216 (16MB)
		2. shell execution: mysql -uroot -e "select repeat('a',16*1024*1024-4);" > 16MB-mysql.txt
			we get: ERROR 2020 (HY000) at line 1: Got packet bigger than 'max_allowed_packet' bytes
		3. shell execution: mysql -uroot -e "select repeat('a',16*1024*1024-5);" > 16MB-mysql.txt
			execution succeeded
		4. so, the max size of the data in one packet is (max-allowed-packet - 5).
		5. To change max-allowed-packet.
			shell execution: mysql max-allowed-packet=xxxxx ....
	*/


	//test in shell : mysql -h 127.0.0.1 -P 6001 -udump -p111 -e "16mbrow" > 16mbrow.txt
	//max data size : 16 * 1024 * 1024 - 5
	var stuff = make([]byte, 16 * 1024 * 1024 - 5)
	for i := range stuff{
		stuff[i] = 'a'
	}

	var rowCase = []interface{} {string(stuff)}
	for i := 0 ; i < 1; i++{
		rs.AddRow(rowCase)
	}

	return rs
}

//the size of resultset row will be more than 16MB
func make16MBRowResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,make16MBRowResultSet())
}

func (tRM *TestRoutineManager)resultsetHandler(rs goetty.IOSession, msg interface{}, _ uint64) error {
	tRM.rwlock.RLock()
	routine, ok := tRM.clients[rs]
	tRM.rwlock.RUnlock()

	pro := routine.protocol
	if !ok {
		return errors.New("routine does not exist")
	}
	packet, ok := msg.(*Packet)
	pro.sequenceId = uint8(packet.SequenceID + 1)
	if !ok {
		return errors.New("message is not Packet")
	}

	length := packet.Length
	payload := packet.Payload
	for uint32(length) == MaxPayloadSize {
		var err error
		msg, err = routine.io.Read()
		if err != nil {
			return errors.New("read msg error")
		}

		packet, ok = msg.(*Packet)
		if !ok {
			return errors.New("message is not Packet")
		}

		pro.sequenceId = uint8(packet.SequenceID + 1)
		payload = append(payload, packet.Payload...)
		length = packet.Length
	}

	// finish handshake process
	if !routine.established {
		err := routine.handleHandshake(payload)
		if err != nil {
			return err
		}
		return nil
	}

	var req *Request
	var resp *Response
	req = pro.GetRequest(payload)
	switch uint8(req.GetCmd()) {
	case COM_QUIT:
		resp = &Response{
			category: OkResponse,
			status:   0,
			data:     nil,
		}
		if err := pro.SendResponse(resp); err != nil {
			fmt.Printf("send response failed. error:%v", err)
			break
		}
	case COM_QUERY:
		var query = string(req.GetData().([]byte))

		switch query {
		case "tiny":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				cmd:      0,
				data:     makeMysqlTinyResult(false),
			}
		case "tinyu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlTinyResult(true),
			}
		case "short":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlShortResult(false),
			}
		case "shortu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlShortResult(true),
			}
		case "long":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlLongResult(false),
			}
		case "longu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlLongResult(true),
			}
		case "longlong":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlLongLongResult(false),
			}
		case "longlongu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlLongLongResult(true),
			}
		case "int24":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlInt24Result(false),
			}
		case "int24u":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlInt24Result(true),
			}
		case "year":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlYearResult(false),
			}
		case "yearu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlYearResult(true),
			}
		case "varchar":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlVarcharResult(),
			}
		case "varstring":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlVarStringResult(),
			}
		case "string":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlStringResult(),
			}
		case "float":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlFloatResult(),
			}
		case "double":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlDoubleResult(),
			}
		case "8columns":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysql8ColumnsResult(),
			}
		case "16mb":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMoreThan16MBResult(),
			}
		case "16mbrow":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     make16MBRowResult(),
			}
		default:
			resp = &Response{
				category: OkResponse,
				status:   0,
				data:     nil,
			}
		}

		if err := pro.SendResponse(resp); err != nil {
			fmt.Printf("send response failed. error:%v", err)
			break
		}

	default:
		fmt.Printf("unsupported command. 0x%x \n", req.cmd)
	}
	if uint8(req.cmd) == COM_QUIT {
		return nil
	}
	return nil
}


func TestMysqlResultSet(t *testing.T){
	//client connection method: mysql -h 127.0.0.1 -P 6001 -udump -p
	//pwd: mysql-server-mysql-8.0.23/mysql-test
	//with mysqltest: mysqltest --test-file=t/1st.test --result-file=r/1st.result --user=dump -p111 -P 6001 --host=127.0.0.1

	//test:
	//./mysql-test-run 1st --extern user=root --extern port=3306 --extern host=127.0.0.1
	//  mysql5.7 failed
	//	mysql-8.0.23 success
	//./mysql-test-run 1st --extern user=root --extern port=6001 --extern host=127.0.0.1
	//	matrixone failed: mysql-test-run: *** ERROR: Could not connect to extern server using command: '/Users/pengzhen/Documents/mysql-server-mysql-8.0.23/bld/runtime_output_directory//mysql --no-defaults --user=root --user=root --port=6001 --host=127.0.0.1 --silent --database=mysql --execute="SHOW GLOBAL VARIABLES"'
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	if err := config.LoadvarsConfigFromFile("../../system_vars_config.toml",
		&config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())
	config.Mempool = mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor()))
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes)

	encoder, decoder := NewSqlCodec()
	trm := NewTestRoutineManager(pu)
	echoServer(trm.resultsetHandler, trm, encoder, decoder)
}
