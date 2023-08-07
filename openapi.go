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

static inline void set_dv_length(IIAPI_DATAVALUE *dest, int i, short len)
{
    dest[i].dv_length = len;
}

//common/aif/demo/apiautil.c

*/
import "C"
import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"reflect"
	"strings"
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
	env                *OpenAPIEnv
	handle             C.II_PTR
	currentTransaction *OpenAPITransaction
}

type OpenAPITransaction struct {
	conn       *OpenAPIConn
	handle     C.II_PTR
	autocommit bool
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

	block *colBlock
}

type rowsHeader struct {
	colNames []string
	colTyps  []columnDesc
}

// part of Columns to get
type colBlock struct {
	colIndex  uint16
	segmented bool
	count     uint16
	cols      *C.IIAPI_DATAVALUE

	buffer *bytes.Buffer
}

type rows struct {
	stmt               *stmt
	transactionCreated bool

	transHandle C.II_PTR
	stmtHandle  C.II_PTR
	queryType   QueryType

	finish func()
	rowsHeader
	done   bool
	result driver.Result

	vals      [][]byte
	colBlocks []*colBlock

	lastInsertId int64
	rowsAffected int64
}

var (
	bufferPool = sync.Pool{
		New: func() any {
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
	wait(&connParm.co_genParm)
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
		wait(&abortParm.ab_genParm)

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

func disconnect(c *OpenAPIConn) error {
	var disconnParm C.IIAPI_DISCONNPARM

	disconnParm.dc_genParm.gp_callback = nil
	disconnParm.dc_genParm.gp_closure = nil
	disconnParm.dc_connHandle = c.handle

	C.IIapi_disconnect(&disconnParm)
	wait(&disconnParm.dc_genParm)

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
	wait(&autoParm.ac_genParm)

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
	if c.currentTransaction != nil {
		return errors.New("can't enable autocommit with active transactions")
	}

	handle, err := autoCommit(c.handle, nil)
	if err != nil {
		if handle != nil {
			var nullHandle C.II_PTR = nil
			autoCommit(nullHandle, handle)
		}
		return err
	}

	c.currentTransaction = &OpenAPITransaction{conn: c, handle: handle, autocommit: true}
	return nil
}

func (c *OpenAPIConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.currentTransaction != nil {
		if c.currentTransaction.autocommit {
			return nil, fmt.Errorf("%s", "autocommit is enabled")
		} else {
			return nil, fmt.Errorf("%s", "already in transaction")
		}
	}

	s := makeStmt(c, "begin transaction", EXEC)
	rows, err := s.runQuery(s.conn.handle, nil)
	if err != nil {
		return nil, err
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	c.currentTransaction = s.transaction
	return s.transaction, nil
}

func (c *OpenAPIConn) DisableAutoCommit() error {
	var err error

	if c.currentTransaction != nil {
		if !c.currentTransaction.autocommit {
			return errors.New("can't disable autocommit: there is ongoing transaction")
		}
		var nullHandle C.II_PTR = nil
		_, err = autoCommit(nullHandle, c.currentTransaction.handle)
	}

	c.currentTransaction = nil
	return err
}

// Wait a command to complete
func wait(genParm *C.IIAPI_GENPARM) {
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

type stmt struct {
	conn        *OpenAPIConn
	query       string
	queryType   QueryType
	transaction *OpenAPITransaction
}

func (s *stmt) runQuery(connHandle C.II_PTR, transHandle C.II_PTR) (*rows, error) {
	var queryParm C.IIAPI_QUERYPARM
	var getDescrParm C.IIAPI_GETDESCRPARM

	queryParm.qy_genParm.gp_callback = nil
	queryParm.qy_genParm.gp_closure = nil
	queryParm.qy_connHandle = connHandle
	queryParm.qy_queryType = C.uint(s.queryType)
	queryParm.qy_queryText = C.CString(s.query)
	queryParm.qy_parameters = 0
	queryParm.qy_tranHandle = transHandle
	queryParm.qy_stmtHandle = nil

	C.IIapi_query(&queryParm)
	wait(&queryParm.qy_genParm)
	err := checkError("IIapi_query()", &queryParm.qy_genParm)
	if err != nil {
		return nil, err
	}

	res := &rows{
		stmt:               s,
		transactionCreated: transHandle == nil,
		transHandle:        queryParm.qy_tranHandle,
		stmtHandle:         queryParm.qy_stmtHandle,
	}

	s.transaction = &OpenAPITransaction{
		handle: queryParm.qy_tranHandle,
		conn:   s.conn,
	}

	// Get query result descriptors.
	if s.queryType != EXEC {
		getDescrParm.gd_genParm.gp_callback = nil
		getDescrParm.gd_genParm.gp_closure = nil
		getDescrParm.gd_stmtHandle = res.stmtHandle
		getDescrParm.gd_descriptorCount = 0
		getDescrParm.gd_descriptor = nil

		C.IIapi_getDescriptor(&getDescrParm)
		wait(&getDescrParm.gd_genParm)

		err = checkError("IIapi_getDescriptor()", &getDescrParm.gd_genParm)
		if err != nil {
			res.Close()
			return nil, err
		}

		res.colTyps = make([]columnDesc, getDescrParm.gd_descriptorCount)
		res.colNames = make([]string, getDescrParm.gd_descriptorCount)
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
		}

		newColBlock := func(start, end uint16, segmented bool) *colBlock {
			var i uint16

			count := end - start + 1
			if count <= 0 {
				return nil
			}

			block := &colBlock{
				count:     count,
				cols:      C.allocate_cols(C.short(count)),
				segmented: segmented,
			}

			// save link to the block for decoding

			if segmented {
				block.colIndex = start
				block.buffer = bufferPool.Get().(*bytes.Buffer)
				res.colTyps[start].block = block
			}

			for i = 0; i < count; i++ {
				j := start + i
				C.set_dv_length(block.cols, C.int(i), C.short(res.colTyps[i].length))
				C.set_dv_value(block.cols, C.int(i), unsafe.Pointer(&res.vals[j][0]))
			}
			return block
		}

		var start = uint16(0)
		var current = uint16(0)

		for current < uint16(len(res.colTyps)) {
			if res.colTyps[current].isLongType() {
				b := newColBlock(start, current-1, false)
				if b != nil {
					res.colBlocks = append(res.colBlocks, b)
				}

				res.colBlocks = append(res.colBlocks, newColBlock(current, current, true))
				start = current + 1
			}
			current += 1
		}

		b := newColBlock(start, current-1, false)
		if b != nil {
			res.colBlocks = append(res.colBlocks, b)
		}
	}

	return res, nil
}

func rollbackTransaction(tranHandle C.II_PTR) error {
	var rollbackParm C.IIAPI_ROLLBACKPARM

	rollbackParm.rb_genParm.gp_callback = nil
	rollbackParm.rb_genParm.gp_closure = nil
	rollbackParm.rb_tranHandle = tranHandle
	rollbackParm.rb_savePointHandle = nil

	C.IIapi_rollback(&rollbackParm)
	wait(&rollbackParm.rb_genParm)
	return checkError("IIapi_rollback", &rollbackParm.rb_genParm)
}

func commitTransaction(tranHandle C.II_PTR) error {
	var commitParm C.IIAPI_COMMITPARM

	commitParm.cm_genParm.gp_callback = nil
	commitParm.cm_genParm.gp_closure = nil
	commitParm.cm_tranHandle = tranHandle

	C.IIapi_commit(&commitParm)
	wait(&commitParm.cm_genParm)
	return checkError("IIapi_commit", &commitParm.cm_genParm)
}

func checkError(location string, genParm *C.IIAPI_GENPARM) error {
	var status, desc string
	var err error

	if genParm.gp_status >= C.IIAPI_ST_ERROR {
		switch genParm.gp_status {
		case C.IIAPI_ST_ERROR:
			status = "IIAPI_ST_ERROR"
		case C.IIAPI_ST_FAILURE:
			status = "IIAPI_ST_FAILURE"
		case C.IIAPI_ST_NOT_INITIALIZED:
			status = "IIAPI_ST_NOT_INITIALIZED"
		case C.IIAPI_ST_INVALID_HANDLE:
			status = "IIAPI_ST_INVALID_HANDLE"
		case C.IIAPI_ST_OUT_OF_MEMORY:
			status = "IIAPI_ST_OUT_OF_MEMORY"
		default:
			status = fmt.Sprintf("%d", genParm.gp_status)
		}

		err = fmt.Errorf("%s status = %s", location, status)
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

			msg = fmt.Sprintf("%s: %s", desc, msg)

			state := fmt.Sprintf("%s", getErrParm.ge_SQLSTATE)
			errorCode := int(getErrParm.ge_errorCode)
			if err != nil {
				wrapped := fmt.Errorf("%w\n%s", err, msg)
				err = newIngresError(state, errorCode, wrapped)
			} else {
				err = newIngresError(state, errorCode, fmt.Errorf(msg))
			}
		}
	}

	if err != nil && verbose {
		log.Printf("%v\n", err)
	}

	return err
}

func (c *columnDesc) isLongType() bool {
	switch c.ingDataType {
	case
		C.IIAPI_LVCH_TYPE,
		C.IIAPI_LNVCH_TYPE,
		C.IIAPI_LTXT_TYPE,
		C.IIAPI_LBYTE_TYPE:
		return true
	default:
		return false
	}
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

func (rs *rows) closeStmt() error {
	var closeParm C.IIAPI_CLOSEPARM

	closeParm.cl_genParm.gp_callback = nil
	closeParm.cl_genParm.gp_closure = nil
	closeParm.cl_stmtHandle = rs.stmtHandle

	C.IIapi_close(&closeParm)
	wait(&closeParm.cl_genParm)
	err := checkError("IIapi_close()", &closeParm.cl_genParm)

	return err
}

func (rs *rows) Close() error {
	if finish := rs.finish; finish != nil {
		defer finish()
	}

	err := rs.closeStmt()

	// free C allocated arrays
	for _, block := range rs.colBlocks {
		if block.cols != nil {
			C.free(unsafe.Pointer(block.cols))
			block.cols = nil
		}

        // reuse buffers
        if block.buffer != nil {
            bufferPool.Put(block.buffer)
            block.buffer = nil
        }
	}

	return err
}

func (rs *rows) fetchData() error {
	var err error
	var getColParm C.IIAPI_GETCOLPARM

	for _, block := range rs.colBlocks {
		if block.segmented {
			block.buffer.Reset()
		}

		getColParm.gc_genParm.gp_callback = nil
		getColParm.gc_genParm.gp_closure = nil
		getColParm.gc_rowCount = 1
		getColParm.gc_columnCount = C.short(block.count)
		getColParm.gc_columnData = block.cols
		getColParm.gc_stmtHandle = rs.stmtHandle

		for {
			getColParm.gc_moreSegments = 0

			C.IIapi_getColumns(&getColParm)
			wait(&getColParm.gc_genParm)
			err = checkError("IIapi_getColumns()", &getColParm.gc_genParm)

			if err != nil {
				return err
			}

			if block.segmented {
				sz := block.cols.dv_length

                // first 2 two bytes contain the size, but we need all content
                if sz > 2 {
				    block.buffer.Write(rs.vals[block.colIndex][2:sz])
                }
			}

			if getColParm.gc_moreSegments == 0 {
				break
			}
		}
	}

	if getColParm.gc_genParm.gp_status == C.IIAPI_ST_NO_DATA {
		rs.done = true
	}

	if rs.done {
		err = rs.fetchInfo()
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
	wait(&info.gq_genParm)
	err := checkError("IIapi_getQueryInfo()", &info.gq_genParm)
	if err != nil {
		rs.rowsAffected = int64(info.gq_rowCountEx)
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
		res = strings.TrimRight(string(val), "\x00")
	case C.IIAPI_LVCH_TYPE, C.IIAPI_LTXT_TYPE:
		if col.block == nil {
			return nil, errors.New("internal: long types should have a link to column block")
		}

        // TODO: optimize here, shrink at the end for \0
		val = col.block.buffer.Bytes()
		res = strings.TrimRight(string(val), "\x00")
	case C.IIAPI_TXT_TYPE, C.IIAPI_VCH_TYPE:
		res = strings.TrimRight(string(val[2:]), "\x00")
	case C.IIAPI_BOOL_TYPE:
		res = (val[0] == 1)
	case C.IIAPI_VBYTE_TYPE:
		res = val[2:]
	case C.IIAPI_BYTE_TYPE:
		res = val
	case C.IIAPI_LBYTE_TYPE:
		if col.block == nil {
			return nil, errors.New("internal: long types should have a link to column block")
		}

		res = col.block.buffer.Bytes()
	case C.IIAPI_NVCH_TYPE:
		val = val[2:]
		fallthrough
	case C.IIAPI_NCHA_TYPE:
		out := make([]uint16, len(val)/2)
		for i := range out {
			out[i] = nativeEndian.Uint16(val[i*2:])
		}
		res = string(utf16.Decode(out))
		res = strings.TrimRight(res.(string), "\x00")
	case C.IIAPI_LNVCH_TYPE:
		val = col.block.buffer.Bytes()
		out := make([]uint16, len(val)/2)
		for i := range out {
			out[i] = nativeEndian.Uint16(val[i*2:])
		}
		res = string(utf16.Decode(out))
		res = strings.TrimRight(res.(string), "\x00")
	default:
		return nil, errors.New("type is not supported")
	}

	return res, nil
}

func (rs *rows) Next(dest []driver.Value) (err error) {
	err = rs.fetchData()
	if err != nil {
		return err
	}

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
