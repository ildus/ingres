package ingres

/*
#cgo pkg-config: iiapi

#include <stdlib.h>
#include <iiapi.h>

IIAPI_INITPARM InitParm = {0, IIAPI_VERSION, 0, NULL};

// golang doesn't support C array, use this to get descriptor item
static inline IIAPI_DESCRIPTOR * get_descr(IIAPI_GETDESCRPARM *descrParm, size_t i) {
    return &descrParm->gd_descriptor[i];
}

//common/aif/demo/apiautil.c

*/
import "C"
import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"
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

type QueryResult struct {
	transHandle C.II_PTR
	stmtHandle  C.II_PTR
	columns     []columnDesc
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
            env: env,
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
		if abortErr != nil {
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

func query(connHandle C.II_PTR, transHandle C.II_PTR, queryStr string) (*QueryResult, error) {
	var queryParm C.IIAPI_QUERYPARM
	var getDescrParm C.IIAPI_GETDESCRPARM
	//var getColParm C.IIAPI_GETCOLPARM
	//var getQInfoParm C.IIAPI_GETQINFOPARM
	//var closeParm C.IIAPI_CLOSEPARM

	queryParm.qy_genParm.gp_callback = nil
	queryParm.qy_genParm.gp_closure = nil
	queryParm.qy_connHandle = connHandle
	queryParm.qy_queryType = C.IIAPI_QT_OPEN
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

	res := &QueryResult{
		transHandle: queryParm.qy_tranHandle,
		stmtHandle:  queryParm.qy_stmtHandle,
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
		return nil, err
	}

	res.columns = make([]columnDesc, getDescrParm.gd_descriptorCount)

	for i := 0; i < len(res.columns); i++ {
		descr := C.get_descr(&getDescrParm, C.ulong(i))
		res.columns[i].ingDataType = descr.ds_dataType
		res.columns[i].nullable = (descr.ds_nullable == 1)
		res.columns[i].length = uint16(descr.ds_length)
		res.columns[i].precision = int16(descr.ds_precision)
		res.columns[i].scale = int16(descr.ds_scale)
		res.columns[i].name = C.GoString(descr.ds_columnName)
	}

	return res, nil
}

func (qr *QueryResult) Close() error {
	var closeParm C.IIAPI_CLOSEPARM

	closeParm.cl_genParm.gp_callback = nil
	closeParm.cl_genParm.gp_closure = nil
	closeParm.cl_stmtHandle = qr.stmtHandle

	C.IIapi_close(&closeParm)
	Wait(&closeParm.cl_genParm)
	return checkError("IIapi_close()", &closeParm.cl_genParm)
}

func (c *OpenAPIConn) Query(queryStr string) (*QueryResult, error) {
	return query(c.handle, c.AutoCommitTransation.handle, queryStr)
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

			log.Printf("OpenAPI error: %s\n", errText)

			if err != nil {
				err = fmt.Errorf("%w\n%s", err, errText)
			} else {
				err = fmt.Errorf(errText)
			}
		}
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
		C.IIAPI_LCLOC_TYPE,
		C.IIAPI_NCHA_TYPE,
		C.IIAPI_NVCH_TYPE,
		C.IIAPI_LNVCH_TYPE,
		C.IIAPI_LNLOC_TYPE,
		C.IIAPI_TXT_TYPE,
		C.IIAPI_LTXT_TYPE:
		return reflect.TypeOf("")
	case
		C.IIAPI_BYTE_TYPE,
		C.IIAPI_VBYTE_TYPE,
		C.IIAPI_LBYTE_TYPE,
		C.IIAPI_LBLOC_TYPE:
		return reflect.TypeOf([]byte(nil))
	case C.IIAPI_INT_TYPE:
		return reflect.TypeOf(int64(0))
	case C.IIAPI_FLT_TYPE:
		return reflect.TypeOf(float64(0))
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
	default:
		return reflect.TypeOf([]byte(nil))
	}
}
