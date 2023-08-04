package ingres

/*
#cgo pkg-config: iiapi

#include <stdlib.h>
#include <iiapi.h>

IIAPI_INITPARM InitParm = {0, IIAPI_VERSION, 0, NULL};

// golang doesn't support C array, use this to get descriptor item
static inline IIAPI_DESCRIPTOR * get_descr(IIAPI_GETDESCRPARM *descrParm, size_t i)
{
    return &descrParm->gd_descriptor[i];
}

static inline IIAPI_DATAVALUE * allocate_cols(short len)
{
    return malloc(sizeof(IIAPI_DATAVALUE) * len);
}

static inline void set_dv_value(IIAPI_DATAVALUE *dest, int i, void *val)
{
    dest[i].dv_value = val;
}

//common/aif/demo/apiautil.c

*/
import "C"
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"reflect"
	"sync"
	"time"
	"unicode/utf16"
	"unsafe"

	"database/sql/driver"
)

type OpenAPIEnv struct {
	handle C.II_PTR
}

type OpenAPIConn struct {
	env                  *OpenAPIEnv
	handle               C.II_PTR
	AutoCommitTransation OpenAPITransaction
}

type OpenAPITransaction struct {
	conn   *OpenAPIConn
	handle C.II_PTR
}

type ConnParams struct {
	DbName   string // vnode::dbname/server_class
	UserName string
	Password string
	Timeout  int
}

type columnDesc struct {
	ingDataType C.IIAPI_DT_ID
	name        string
	nullable    bool
	length      uint16
	precision   int16
	scale       int16
}

type rowsHeader struct {
	colNames []string
	colTyps  []columnDesc
}

type rows struct {
	transactionIsNew bool

	transHandle C.II_PTR
	stmtHandle  C.II_PTR

	finish func()
	rowsHeader
	done   bool
	result driver.Result

	cols *C.IIAPI_DATAVALUE
	vals [][]byte

	lastInsertId int64
	rowsAffected int64

	getColParm C.IIAPI_GETCOLPARM
}

var (
	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}

	nativeEndian binary.ByteOrder
	_            driver.Result = rows{}
	verbose                    = false
)

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("Could not determine native endianness.")
	}
}

func InitOpenAPI() (*OpenAPIEnv, error) {
	C.IIapi_initialize(&C.InitParm)

	if C.InitParm.in_status != 0 {
		return nil, errors.New("could not initialize Ingres OpenAPI")
	}

	return &OpenAPIEnv{handle: C.InitParm.in_envHandle}, nil
}

func ReleaseOpenAPI(env *OpenAPIEnv) {
	var rel C.IIAPI_RELENVPARM
	var term C.IIAPI_TERMPARM

	rel.re_envHandle = env.handle
	C.IIapi_releaseEnv(&rel)
	C.IIapi_terminate(&term)
}

func (env *OpenAPIEnv) Connect(params ConnParams) (*OpenAPIConn, error) {
	var connParm C.IIAPI_CONNPARM

	connParm.co_genParm.gp_callback = nil
	connParm.co_genParm.gp_closure = nil
	connParm.co_type = C.IIAPI_CT_SQL
	connParm.co_target = C.CString(params.DbName)
	connParm.co_connHandle = env.handle
	connParm.co_tranHandle = nil
	connParm.co_username = nil
	connParm.co_password = nil
	if len(params.UserName) > 0 {
		connParm.co_username = C.CString(params.UserName)
	}
	if len(params.Password) > 0 {
		connParm.co_password = C.CString(params.Password)
	}

	if params.Timeout > 0 {
		connParm.co_timeout = C.int(params.Timeout)
	} else {
		connParm.co_timeout = -1
	}

	C.IIapi_connect(&connParm)
	Wait(&connParm.co_genParm)
	err := checkError("IIapi_connect()", &connParm.co_genParm)

	if connParm.co_genParm.gp_status == C.IIAPI_ST_SUCCESS {
		return &OpenAPIConn{
			env:    env,
			handle: connParm.co_connHandle,
		}, nil
	}

	if connParm.co_connHandle != nil {
		var abortParm C.IIAPI_ABORTPARM

		abortParm.ab_genParm.gp_callback = nil
		abortParm.ab_genParm.gp_closure = nil
		abortParm.ab_connHandle = connParm.co_connHandle

		/*
		 ** Make sync request.
		 */
		C.IIapi_abort(&abortParm)
		Wait(&abortParm.ab_genParm)

		abortErr := checkError("IIapi_abort()", &abortParm.ab_genParm)
		if verbose && abortErr != nil {
			log.Printf("could not abort connection: %v", abortErr)
		}
	}

	if err == nil {
		err = errors.New("undefined error")
	}

	return nil, err
}

func (c *OpenAPIConn) Disconnect() error {
	var disconnParm C.IIAPI_DISCONNPARM

	disconnParm.dc_genParm.gp_callback = nil
	disconnParm.dc_genParm.gp_closure = nil
	disconnParm.dc_connHandle = c.handle

	C.IIapi_disconnect(&disconnParm)
	Wait(&disconnParm.dc_genParm)

	// Check results.
	err := checkError("IIapi_disconnect()", &disconnParm.dc_genParm)
	return err
}

func autoCommit(connHandle C.II_PTR, transHandle C.II_PTR) (C.II_PTR, error) {
	var autoParm C.IIAPI_AUTOPARM

	autoParm.ac_genParm.gp_callback = nil
	autoParm.ac_genParm.gp_closure = nil
	autoParm.ac_connHandle = connHandle
	autoParm.ac_tranHandle = transHandle

	C.IIapi_autocommit(&autoParm)
	Wait(&autoParm.ac_genParm)

	/*
	 ** Check and return results.
	 **
	 ** If an error occurs enabling autocommit, the transaction
	 ** handle returned must be freed by disabling autocommit.
	 ** This is done with a extra call to this routine.
	 */
	err := checkError("IIapi_autocommit()", &autoParm.ac_genParm)
	return autoParm.ac_tranHandle, err
}

func (c *OpenAPIConn) AutoCommit() error {
	handle, err := autoCommit(c.handle, nil)
	if err != nil {
		if handle != nil {
			var nullHandle C.II_PTR = nil
			autoCommit(nullHandle, handle)
		}
		return err
	}

	c.AutoCommitTransation = OpenAPITransaction{conn: c, handle: handle}
	return nil
}

func (c *OpenAPIConn) DisableAutoCommit() error {
	var err error

	if c.AutoCommitTransation.handle != nil {
		var nullHandle C.II_PTR = nil
		_, err = autoCommit(nullHandle, c.AutoCommitTransation.handle)
	}

	c.AutoCommitTransation.handle = nil
	return err
}

// Wait a command to complete
func Wait(genParm *C.IIAPI_GENPARM) {
	var waitParm C.IIAPI_WAITPARM

	for genParm.gp_completed == 0 {
		waitParm.wt_timeout = -1
		C.IIapi_wait(&waitParm)

		if waitParm.wt_status != C.IIAPI_ST_SUCCESS {
			genParm.gp_status = waitParm.wt_status
			break
		}
	}
}

type QueryType uint

const (
	SELECT         QueryType = C.IIAPI_QT_QUERY
	SELECT_ONE     QueryType = C.IIAPI_QT_SELECT_SINGLETON
	EXEC           QueryType = C.IIAPI_QT_EXEC
	OPEN           QueryType = C.IIAPI_QT_OPEN
	EXEC_PROCEDURE QueryType = C.IIAPI_QT_EXEC_PROCEDURE
)

func query(connHandle C.II_PTR, transHandle C.II_PTR, queryStr string,
	queryType QueryType) (*rows, error) {
	var queryParm C.IIAPI_QUERYPARM
	var getDescrParm C.IIAPI_GETDESCRPARM

	queryParm.qy_genParm.gp_callback = nil
	queryParm.qy_genParm.gp_closure = nil
	queryParm.qy_connHandle = connHandle
	queryParm.qy_queryType = C.uint(queryType)
	queryParm.qy_queryText = C.CString(queryStr)
	queryParm.qy_parameters = 0
	queryParm.qy_tranHandle = transHandle
	queryParm.qy_stmtHandle = nil

	// Run query
	C.IIapi_query(&queryParm)
	Wait(&queryParm.qy_genParm)
	err := checkError("IIapi_query()", &queryParm.qy_genParm)
	if err != nil {
		return nil, err
	}

	res := &rows{
		transactionIsNew: transHandle == nil,
		transHandle:      queryParm.qy_tranHandle,
		stmtHandle:       queryParm.qy_stmtHandle,
	}

	// Get query result descriptors.
	getDescrParm.gd_genParm.gp_callback = nil
	getDescrParm.gd_genParm.gp_closure = nil
	getDescrParm.gd_stmtHandle = res.stmtHandle
	getDescrParm.gd_descriptorCount = 0
	getDescrParm.gd_descriptor = nil

	C.IIapi_getDescriptor(&getDescrParm)
	Wait(&getDescrParm.gd_genParm)

	err = checkError("IIapi_getDescriptor()", &getDescrParm.gd_genParm)
	if err != nil {
		res.Close()
		return nil, err
	}

	res.colTyps = make([]columnDesc, getDescrParm.gd_descriptorCount)
	res.colNames = make([]string, getDescrParm.gd_descriptorCount)
	res.cols = C.allocate_cols(getDescrParm.gd_descriptorCount)
	res.vals = make([][]byte, len(res.colTyps))

	for i := 0; i < len(res.colTyps); i++ {
		descr := C.get_descr(&getDescrParm, C.ulong(i))
		res.colTyps[i].ingDataType = descr.ds_dataType
		res.colTyps[i].nullable = (descr.ds_nullable == 1)
		res.colTyps[i].length = uint16(descr.ds_length)
		res.colTyps[i].precision = int16(descr.ds_precision)
		res.colTyps[i].scale = int16(descr.ds_scale)

		res.colNames[i] = C.GoString(descr.ds_columnName)
		res.vals[i] = make([]byte, res.colTyps[i].length)
		C.set_dv_value(res.cols, C.int(i), unsafe.Pointer(&res.vals[i][0]))
	}
	res.getColParm.gc_genParm.gp_callback = nil
	res.getColParm.gc_genParm.gp_closure = nil
	res.getColParm.gc_rowCount = 1
	res.getColParm.gc_columnCount = getDescrParm.gd_descriptorCount
	res.getColParm.gc_columnData = res.cols
	res.getColParm.gc_stmtHandle = res.stmtHandle
	res.getColParm.gc_moreSegments = 0

	return res, nil
}

func closeTransaction(tranHandle C.II_PTR) error {
	var rollbackParm C.IIAPI_ROLLBACKPARM

	rollbackParm.rb_genParm.gp_callback = nil
	rollbackParm.rb_genParm.gp_closure = nil
	rollbackParm.rb_tranHandle = tranHandle
	rollbackParm.rb_savePointHandle = nil

	C.IIapi_rollback(&rollbackParm)
	Wait(&rollbackParm.rb_genParm)
	return checkError("IIapi_rollback", &rollbackParm.rb_genParm)
}

func (c *OpenAPIConn) Fetch(queryStr string) (*rows, error) {
	return query(c.handle, nil, queryStr, SELECT)
}

func (c *OpenAPIConn) Exec(queryStr string) (*rows, error) {
    rows, err := query(c.handle, c.AutoCommitTransation.handle, queryStr, EXEC)
    if err != nil {
        return nil, err
    }

    rows.fetchInfo()
    err = rows.Close()
    if err != nil {
        return nil, err
    }

    return rows, nil
}

func checkError(location string, genParm *C.IIAPI_GENPARM) error {
	var desc string
	var err error

	if genParm.gp_status >= C.IIAPI_ST_ERROR {
		switch genParm.gp_status {
		case C.IIAPI_ST_ERROR:
			desc = "IIAPI_ST_ERROR"
		case C.IIAPI_ST_FAILURE:
			desc = "IIAPI_ST_FAILURE"
		case C.IIAPI_ST_NOT_INITIALIZED:
			desc = "IIAPI_ST_NOT_INITIALIZED"
		case C.IIAPI_ST_INVALID_HANDLE:
			desc = "IIAPI_ST_INVALID_HANDLE"
		case C.IIAPI_ST_OUT_OF_MEMORY:
			desc = "IIAPI_ST_OUT_OF_MEMORY"
		default:
			desc = fmt.Sprintf("%d", genParm.gp_status)
		}

		err = fmt.Errorf("%s status = %s", location, desc)
	}

	if genParm.gp_errorHandle != nil {
		var getErrParm C.IIAPI_GETEINFOPARM
		/*
		 ** Provide initial error handle.
		 */
		getErrParm.ge_errorHandle = genParm.gp_errorHandle

		/*
		 ** Call IIapi_getErrorInfo() in loop until no data.
		 */
		for {
			C.IIapi_getErrorInfo(&getErrParm)
			if getErrParm.ge_status != C.IIAPI_ST_SUCCESS {
				break
			}

			/*
			 ** Print error message info.
			 */
			switch getErrParm.ge_type {
			case C.IIAPI_GE_ERROR:
				desc = "ERROR"
			case C.IIAPI_GE_WARNING:
				desc = "WARNING"
			case C.IIAPI_GE_MESSAGE:
				desc = "USER MESSAGE"
			default:
				desc = "UNKNOWN"
			}

			var msg string = "NULL"
			if getErrParm.ge_message != nil {
				msg = C.GoString(getErrParm.ge_message)
			}

			errText := fmt.Sprintf("Type:%s State:%s Code:0x%x Message:%s",
				desc, getErrParm.ge_SQLSTATE, getErrParm.ge_errorCode, msg)

			if err != nil {
				err = fmt.Errorf("%w\n%s", err, errText)
			} else {
				err = fmt.Errorf(errText)
			}
		}
	}

	if verbose && err != nil {
		log.Printf("%v\n", err)
	}

	return err
}

func (c *columnDesc) getType() reflect.Type {
	switch c.ingDataType {
	case
		C.IIAPI_CHR_TYPE,
		C.IIAPI_CHA_TYPE,
		C.IIAPI_VCH_TYPE,
		C.IIAPI_LVCH_TYPE,
		C.IIAPI_NCHA_TYPE,
		C.IIAPI_NVCH_TYPE,
		C.IIAPI_LNVCH_TYPE,
		C.IIAPI_TXT_TYPE,
		C.IIAPI_LTXT_TYPE:
		return reflect.TypeOf("")
	case
		C.IIAPI_BYTE_TYPE,
		C.IIAPI_VBYTE_TYPE,
		C.IIAPI_LBYTE_TYPE:
		return reflect.TypeOf([]byte(nil))
	case C.IIAPI_INT_TYPE:
		if c.length == 2 {
			return reflect.TypeOf(int16(0))
		} else if c.length == 4 {
			return reflect.TypeOf(int32(0))
		} else if c.length == 8 {
			return reflect.TypeOf(int32(0))
		}
	case C.IIAPI_FLT_TYPE:
		if c.length == 4 {
			return reflect.TypeOf(float32(0))
		} else if c.length == 8 {
			return reflect.TypeOf(float64(0))
		}
	case
		C.IIAPI_MNY_TYPE, /* Money */
		C.IIAPI_DEC_TYPE: /* Decimal */
		return reflect.TypeOf("")
	case
		C.IIAPI_BOOL_TYPE: /* Boolean */
		return reflect.TypeOf(false)
	case
		C.IIAPI_UUID_TYPE, /* UUID */
		C.IIAPI_IPV4_TYPE, /* IPv4 */
		C.IIAPI_IPV6_TYPE: /* IPv6 */
		return reflect.TypeOf([]byte(nil))
	case
		C.IIAPI_DTE_TYPE,  /* Ingres Date */
		C.IIAPI_DATE_TYPE, /* ANSI Date */
		C.IIAPI_TIME_TYPE, /* Ingres Time */
		C.IIAPI_TMWO_TYPE, /* Time without Timezone */
		C.IIAPI_TMTZ_TYPE, /* Time with Timezone */
		C.IIAPI_TS_TYPE,   /* Ingres Timestamp */
		C.IIAPI_TSWO_TYPE, /* Timestamp without Timezone */
		C.IIAPI_TSTZ_TYPE: /* Timestamp with Timezone */
		return reflect.TypeOf(time.Time{})
	case C.IIAPI_INTYM_TYPE, /* Interval Year to Month */
		C.IIAPI_INTDS_TYPE: /* Interval Day to Second */
		return reflect.TypeOf(time.Duration(0))
	}
	return reflect.TypeOf([]byte(nil))
}

var ingresTypes = map[C.IIAPI_DT_ID]string{
	C.IIAPI_CHR_TYPE:   "c",
	C.IIAPI_CHA_TYPE:   "char",
	C.IIAPI_VCH_TYPE:   "varchar",
	C.IIAPI_LVCH_TYPE:  "long varchar",
	C.IIAPI_LCLOC_TYPE: "long char locator",
	C.IIAPI_NCHA_TYPE:  "nchar",
	C.IIAPI_NVCH_TYPE:  "nvarchar",
	C.IIAPI_LNVCH_TYPE: "long nvarchar",
	C.IIAPI_TXT_TYPE:   "text",
	C.IIAPI_LTXT_TYPE:  "long text",
	C.IIAPI_BYTE_TYPE:  "byte",
	C.IIAPI_VBYTE_TYPE: "varbyte",
	C.IIAPI_LBYTE_TYPE: "long byte",
	C.IIAPI_LBLOC_TYPE: "long byte locator",
	C.IIAPI_MNY_TYPE:   "money",
	C.IIAPI_DEC_TYPE:   "decimal",
	C.IIAPI_BOOL_TYPE:  "boolean",
	C.IIAPI_UUID_TYPE:  "UUID",
	C.IIAPI_IPV4_TYPE:  "IPV4",
	C.IIAPI_IPV6_TYPE:  "IPV6",
	C.IIAPI_DTE_TYPE:   "ingresdate",
	C.IIAPI_DATE_TYPE:  "ansidate",
	C.IIAPI_TIME_TYPE:  "time with local time zone",
	C.IIAPI_TMWO_TYPE:  "time without time zone",
	C.IIAPI_TMTZ_TYPE:  "time with time zone",
	C.IIAPI_TS_TYPE:    "timestamp with local time zone",
	C.IIAPI_TSWO_TYPE:  "timestamp without time zone",
	C.IIAPI_TSTZ_TYPE:  "timestamp with time zone",
	C.IIAPI_INTYM_TYPE: "interval year to month",
	C.IIAPI_INTDS_TYPE: "interval day to second",
}

func (c *columnDesc) getTypeName() string {
	val, ok := ingresTypes[c.ingDataType]

	if !ok {
		if c.ingDataType == C.IIAPI_INT_TYPE {
			switch c.length {
			case 1:
				return "integer1"
			case 2:
				return "integer2"
			case 4:
				return "integer4"
			case 8:
				return "integer8"
			}
		} else if c.ingDataType == C.IIAPI_FLT_TYPE {
			switch c.length {
			case 4:
				return "float4"
			case 8:
				return "float8"
			}
		}

		return "UNKNOWN"
	}

	return val
}

func (c *columnDesc) isDecimal() bool {
	return c.ingDataType == C.IIAPI_DEC_TYPE
}

var varTypes = map[C.IIAPI_DT_ID]int64{
	C.IIAPI_VCH_TYPE:   -1, // use length
	C.IIAPI_LVCH_TYPE:  2_000_000_000,
	C.IIAPI_NVCH_TYPE:  -1, // use length
	C.IIAPI_LNVCH_TYPE: 1_000_000_000,
	C.IIAPI_TXT_TYPE:   -1, // use length
	C.IIAPI_LTXT_TYPE:  -1, // use length
	C.IIAPI_VBYTE_TYPE: -1, // use length
	C.IIAPI_LBYTE_TYPE: 2_000_000_000,
}

func (c *columnDesc) Length() (int64, bool) {
	val, ok := varTypes[c.ingDataType]
	if !ok {
		return 0, false
	}

	if val == -1 {
		return int64(c.length), true
	}

	return val, true
}

func (rs *rows) Close() error {
	if finish := rs.finish; finish != nil {
		defer finish()
	}

	var closeParm C.IIAPI_CLOSEPARM

	closeParm.cl_genParm.gp_callback = nil
	closeParm.cl_genParm.gp_closure = nil
	closeParm.cl_stmtHandle = rs.stmtHandle

	C.IIapi_close(&closeParm)
	Wait(&closeParm.cl_genParm)
	err := checkError("IIapi_close()", &closeParm.cl_genParm)

	// close transaction if it was not specified by caller
	if rs.transactionIsNew && rs.transHandle != nil {
		rollbackErr := closeTransaction(rs.transHandle)
		if rollbackErr != nil {
			return rollbackErr
		}
        rs.transHandle = nil
	}

	if rs.cols != nil {
		C.free(unsafe.Pointer(rs.cols))
        rs.cols = nil
	}

	return err
}

func (rs *rows) fetchData() error {
	var err error

	C.IIapi_getColumns(&rs.getColParm)
	Wait(&rs.getColParm.gc_genParm)
	err = checkError("IIapi_getColumns()", &rs.getColParm.gc_genParm)
	if err != nil {
		return err
	}

	if rs.getColParm.gc_genParm.gp_status == C.IIAPI_ST_NO_DATA {
		rs.done = true
	}

	if rs.done {
        rs.fetchInfo()
	}

	return err
}

func (rs *rows) fetchInfo() error {
	var getQInfoParm C.IIAPI_GETQINFOPARM

    /* Get fetch result info */
    getQInfoParm.gq_genParm.gp_callback = nil
    getQInfoParm.gq_genParm.gp_closure = nil
    getQInfoParm.gq_stmtHandle = rs.stmtHandle

    info := &getQInfoParm
    C.IIapi_getQueryInfo(info)
    Wait(&info.gq_genParm)
    err := checkError("IIapi_getQueryInfo()", &info.gq_genParm)
    if err != nil {
        if info.gq_rowStatus == C.IIAPI_ROW_INSERTED ||
            info.gq_rowStatus == C.IIAPI_ROW_UPDATED ||
            info.gq_rowStatus == C.IIAPI_ROW_DELETED {
            rs.rowsAffected = int64(info.gq_rowCountEx)
        }
    }

	return err
}

func decode(col *columnDesc, val []byte) (driver.Value, error) {
	var res driver.Value
	switch col.ingDataType {
	case C.IIAPI_INT_TYPE:
		switch col.length {
		case 1:
			res = int8(val[0])
		case 2:
			res = int16(nativeEndian.Uint16(val))
		case 4:
			res = int32(nativeEndian.Uint32(val))
		case 8:
			res = int64(nativeEndian.Uint64(val))
		}
	case C.IIAPI_FLT_TYPE:
		switch col.length {
		case 4:
			bits := nativeEndian.Uint32(val)
			res = math.Float32frombits(bits)
		case 8:
			bits := nativeEndian.Uint64(val)
			res = math.Float64frombits(bits)
		}
	case C.IIAPI_CHR_TYPE, C.IIAPI_CHA_TYPE:
		res = string(val)
	case C.IIAPI_LVCH_TYPE:
		fallthrough
	case C.IIAPI_LTXT_TYPE:
		fallthrough
	case C.IIAPI_TXT_TYPE, C.IIAPI_VCH_TYPE:
		res = string(val[2:])
	case C.IIAPI_BOOL_TYPE:
		res = (val[0] == 1)
	case C.IIAPI_VBYTE_TYPE:
		res = val[2:]
	case C.IIAPI_BYTE_TYPE:
		res = val
	case C.IIAPI_NVCH_TYPE:
		val = val[2:]
		fallthrough
	case C.IIAPI_NCHA_TYPE:
		out := make([]uint16, len(val)/2)
		for i := range out {
			out[i] = nativeEndian.Uint16(val[i*2:])
		}
		res = string(utf16.Decode(out))
	default:
		return nil, errors.New("type is not supported")
	}

	return res, nil
}

func (rs *rows) Next(dest []driver.Value) (err error) {
	rs.fetchData()

	if rs.done {
		return io.EOF
	}

	for i, val := range rs.vals {
		dest[i], err = decode(&rs.colTyps[i], val)
		if err != nil {
			return err
		}
	}

	return nil
}
