/* $Id: dbdimp.c,v 1.2 1997/09/26 23:26:26 mpeppler Exp $

   Copyright (c) 1997  Michael Peppler

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.
   
   Based on DBD::Oracle dbdimp.c, Copyright (c) 1994,1995 Tim Bunce

*/

#include "Sybase.h"

DBISTATE_DECLARE;

static void cleanUp _((imp_sth_t *));
static char *GetAggOp _((CS_INT));
static CS_INT display_dlen _((CS_DATAFMT *));
static CS_RETCODE display_header _((CS_INT, CS_DATAFMT*));
static CS_RETCODE describe _((imp_sth_t *, int));
static CS_RETCODE fetch_data _((CS_COMMAND*));
static CS_RETCODE clientmsg_cb _((CS_CONTEXT*, CS_CONNECTION*, CS_CLIENTMSG*));
static CS_RETCODE servermsg_cb _((CS_CONTEXT*, CS_CONNECTION*, CS_SERVERMSG*));
static CS_RETCODE notification_cb _((CS_CONNECTION*, CS_CHAR*, CS_INT));
static CS_COMMAND *syb_alloc_cmd _((CS_CONNECTION*));


static CS_CONTEXT *context;
static char scriptName[255];

static imp_dbh_t *DBH;

static CS_RETCODE
clientmsg_cb(context, connection, errmsg)
CS_CONTEXT	*context;
CS_CONNECTION	*connection;	
CS_CLIENTMSG	*errmsg;
{
    imp_dbh_t *imp_dbh = NULL;
    char buff[255];

    if((ct_con_props(connection, CS_GET, CS_USERDATA,
		     &imp_dbh, CS_SIZEOF(imp_dbh), NULL)) != CS_SUCCEED)
	croak("Panic: servermsg_cb: Can't find handle from connection");
    
    sv_setiv(DBIc_ERR(imp_dbh), (IV)CS_NUMBER(errmsg->msgnumber));
    
    sv_setpv(DBIc_ERRSTR(imp_dbh), "Open Client Message:\n");
    sprintf(buff, "Message number: LAYER = (%ld) ORIGIN = (%ld) ",
	    CS_LAYER(errmsg->msgnumber), CS_ORIGIN(errmsg->msgnumber));
    sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
    sprintf(buff, "SEVERITY = (%ld) NUMBER = (%ld)\n",
	    CS_SEVERITY(errmsg->msgnumber), CS_NUMBER(errmsg->msgnumber));
    sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
    sprintf(buff, "Message String: %s\n", errmsg->msgstring);
    sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
    if (errmsg->osstringlen > 0) {
	    sprintf(buff, "Operating System Error: %s\n",
		    errmsg->osstring);
	    sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
    }
    return CS_SUCCEED;
}

static CS_RETCODE
servermsg_cb(context, connection, srvmsg)
CS_CONTEXT	*context;
CS_CONNECTION	*connection;
CS_SERVERMSG	*srvmsg;
{
    CS_COMMAND	*cmd;
    CS_RETCODE	retcode;
    imp_dbh_t *imp_dbh = NULL;
    char buff[255];

    if((ct_con_props(connection, CS_GET, CS_USERDATA,
		     &imp_dbh, CS_SIZEOF(imp_dbh), NULL)) != CS_SUCCEED)
	croak("Panic: servermsg_cb: Can't find handle from connection");
    
    if(imp_dbh) {
	sv_setiv(DBIc_ERR(imp_dbh), (IV)srvmsg->msgnumber);
	
	sv_setpv(DBIc_ERRSTR(imp_dbh), "Server message:\n");
	sprintf(buff, "Message number: %ld, Severity %ld, ",
		srvmsg->msgnumber, srvmsg->severity);
	sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	sprintf(buff, "State %ld, Line %ld\n",
		srvmsg->state, srvmsg->line);
	sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	if (srvmsg->svrnlen > 0) {
	    sprintf(buff, "Server '%s'\n", srvmsg->svrname);
	    sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	}
	if (srvmsg->proclen > 0) {
	    sprintf(buff, " Procedure '%s'\n", srvmsg->proc);
	    sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	}
	
	sprintf(buff, "Message String: %s\n", srvmsg->text);
	sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	if (srvmsg->status & CS_HASEED)	{
	    fprintf(DBILOGFP, "\n[Start Extended Error]\n");
	    if (ct_con_props(connection, CS_GET, CS_EED_CMD,
			     &cmd, CS_UNUSED, NULL) != CS_SUCCEED)
	    {
		warn("servermsg_cb: ct_con_props(CS_EED_CMD) failed");
		return CS_FAIL;
	    }
	    retcode = fetch_data(cmd);
	    fprintf(DBILOGFP, "\n[End Extended Error]\n\n");
	}
	else
	    retcode = CS_SUCCEED;
	fflush(DBILOGFP);
	
	return retcode;
    }
    return CS_SUCCEED;
}

static CS_RETCODE
notification_cb(connection, procname, pnamelen)
CS_CONNECTION	*connection;
CS_CHAR		*procname;
CS_INT		pnamelen;
{
    CS_RETCODE	retcode;
    CS_COMMAND	*cmd;

    fprintf(DBILOGFP,
	    "\n-- Notification received --\nprocedure name = '%s'\n\n",
	    procname);
    fflush(DBILOGFP);
    
    if (ct_con_props(connection, CS_GET, CS_EED_CMD,
		     &cmd, CS_UNUSED, NULL) != CS_SUCCEED)
    {
	warn("notification_cb: ct_con_props(CS_EED_CMD) failed");
	return CS_FAIL;
    }
    retcode = fetch_data(cmd);
    fprintf(DBILOGFP, "\n[End Notification]\n\n");
    
    return retcode;
}

static CS_CHAR * 
GetAggOp(op)
CS_INT op;
{
    CS_CHAR *name;

    switch ((int)op)
    {
      case CS_OP_SUM:
	name = "sum";
	break;
      case CS_OP_AVG:
	name = "avg";
	break;
      case CS_OP_COUNT:
	name = "count";
	break;
      case CS_OP_MIN:
	name = "min";
	break;
      case CS_OP_MAX:
	name = "max";
	break;
      default:
	name = "unknown";
	break;
    }
    return name;
}

static CS_INT
display_dlen(column)
CS_DATAFMT *column;
{
    CS_INT		len;

    switch ((int) column->datatype)
    {
      case CS_CHAR_TYPE:
      case CS_VARCHAR_TYPE:
      case CS_TEXT_TYPE:
      case CS_IMAGE_TYPE:
	len = MIN(column->maxlength, MAX_CHAR_BUF);
	break;

      case CS_BINARY_TYPE:
      case CS_VARBINARY_TYPE:
	len = MIN((2 * column->maxlength) + 2, MAX_CHAR_BUF);
	break;
	
      case CS_BIT_TYPE:
      case CS_TINYINT_TYPE:
	len = 3;
	break;
	
      case CS_SMALLINT_TYPE:
	len = 6;
	break;
	
      case CS_INT_TYPE:
	len = 11;
	break;
	
      case CS_REAL_TYPE:
      case CS_FLOAT_TYPE:
	len = 20;
	break;
	
      case CS_MONEY_TYPE:
      case CS_MONEY4_TYPE:
	len = 24;
	break;
	
      case CS_DATETIME_TYPE:
      case CS_DATETIME4_TYPE:
	len = 30;
	break;
	
      case CS_NUMERIC_TYPE:
      case CS_DECIMAL_TYPE:
	len = (CS_MAX_PREC + 2);
	break;
	
      default:
	len = column->maxlength;
	break;
    }
    
    return MAX(strlen(column->name) + 1, len);
}

/* FIXME:
   All of the output in this function goes to stdout. The function is
   called from fetch_data (which is called from servermsg_cb), which
   normally outputs it's messages to stderr... */
static CS_RETCODE
display_header(numcols, columns)
CS_INT		numcols;
CS_DATAFMT	columns[];
{
    CS_INT		i;
    CS_INT		l;
    CS_INT		j;
    CS_INT		disp_len;

    fputc('\n', DBILOGFP);
    for (i = 0; i < numcols; i++)
    {
	disp_len = display_dlen(&columns[i]);
	fprintf(DBILOGFP, "%s", columns[i].name);
	fflush(DBILOGFP);
	l = disp_len - strlen(columns[i].name);
	for (j = 0; j < l; j++)
	{
	    fputc(' ', DBILOGFP);
	    fflush(DBILOGFP);
	}
    }
    fputc('\n', DBILOGFP);
    fflush(DBILOGFP);
    for (i = 0; i < numcols; i++)
    {
	disp_len = display_dlen(&columns[i]);
	l = disp_len - 1;
	for (j = 0; j < l; j++)
	{
	    fputc('-', DBILOGFP);
	}
	fputc(' ', DBILOGFP);
    }
    fputc('\n', DBILOGFP);
    
    return CS_SUCCEED;
}


void syb_init(dbistate)
    dbistate_t *dbistate;
{
    SV 		*sv;
    CS_RETCODE	retcode;
    CS_INT	netio_type = CS_SYNC_IO;

    DBIS = dbistate;

    if((retcode = cs_ctx_alloc(CTLIB_VERSION, &context)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: cs_ctx_alloc() failed");

    if((retcode = ct_init(context, CTLIB_VERSION)) != CS_SUCCEED)
    {
	cs_ctx_drop(context);
	context = NULL;
	croak("DBD::Sybase initialize: ct_init() failed");
    }

    if((retcode = ct_callback(context, NULL, CS_SET, CS_CLIENTMSG_CB,
			  (CS_VOID *)clientmsg_cb)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: ct_callback(clientmsg) failed");
    if((retcode = ct_callback(context, NULL, CS_SET, CS_SERVERMSG_CB,
			      (CS_VOID *)servermsg_cb)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: ct_callback(servermsg) failed");

    if((retcode = ct_callback(context, NULL, CS_SET, CS_NOTIF_CB,
			      (CS_VOID *)notification_cb)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: ct_callback(notification) failed");

    if((retcode = ct_config(context, CS_SET, CS_NETIO, &netio_type, 
			    CS_UNUSED, NULL)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: ct_config(netio) failed");


    if((sv = perl_get_sv("0", FALSE)))
    {
	char *p;
	strcpy(scriptName, SvPV(sv, na));
	if((p = strrchr(scriptName, '/')))
	{
	    ++p;
	    strcpy(scriptName, p);
	}
    }
}

  
int
syb_db_login(dbh, imp_dbh, server, uid, pwd)
    SV         *dbh;
    struct imp_dbh_st *imp_dbh;
    char       *server;
    char       *uid;
    char       *pwd;
{
    CS_RETCODE     retcode;
    CS_CONNECTION *connection = NULL;
    CS_COMMAND    *cmd;
    int len;

    if((retcode = ct_con_alloc(context, &connection)) != CS_SUCCEED)
	warn("ct_con_alloc failed");
  
    if(retcode == CS_SUCCEED && uid && *uid) {
	if((retcode = ct_con_props(connection, CS_SET, CS_USERNAME, 
				   uid, CS_NULLTERM, NULL)) != CS_SUCCEED)
	    warn("ct_con_props(username) failed");
    }
    if(retcode == CS_SUCCEED && pwd && *pwd) {
	if((retcode = ct_con_props(connection, CS_SET, CS_PASSWORD, 
				   pwd, CS_NULLTERM, NULL)) != CS_SUCCEED)
	    warn("ct_con_props(password) failed");
    }
    if(retcode == CS_SUCCEED)
    {
	if((retcode = ct_con_props(connection, CS_SET, CS_APPNAME, 
				   scriptName, CS_NULLTERM, NULL)) != CS_SUCCEED)
	    warn("ct_con_props(appname) failed");
    }
    if (retcode == CS_SUCCEED)
    {
	len = (server == NULL || !*server) ? 0 : CS_NULLTERM;
	if((retcode = ct_connect(connection, server, len)) != CS_SUCCEED)
	    warn("ct_connect failed");
    }

    imp_dbh->connection = connection;

    if((retcode = ct_con_props(connection, CS_SET, CS_USERDATA,
			       &imp_dbh, CS_SIZEOF(imp_dbh), NULL)) != CS_SUCCEED)
	warn("ct_con_props(userdata) failed");

    DBIc_on(imp_dbh, DBIcf_IMPSET);	/* imp_dbh set up now		*/
    DBIc_on(imp_dbh, DBIcf_ACTIVE);	/* call disconnect before freeing*/

    DBH = imp_dbh;

    return 1;
}


int      syb_discon_all(drh, imp_drh)
    SV *drh;
    imp_drh_t *imp_drh;
{
    return 1;
}

int      syb_db_commit(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    CS_COMMAND *cmd;
    char buff[128];
    CS_INT      restype;
    CS_RETCODE  retcode;
    int         failFlag = 0;

    if(!imp_dbh->inTransaction)
	return 1;
    if(DBIc_is(imp_dbh, DBIcf_AutoCommit)) {
	warn("commit ineffective with AutoCommit");
	return 1;
    }

    cmd = syb_alloc_cmd(imp_dbh->connection);
    sprintf(buff, "\nCOMMIT TRAN %s\n", imp_dbh->tranName);
    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "    syb_db_commit() -> ct_command(%s)\n", buff);
    retcode = ct_command(cmd, CS_LANG_CMD, buff,
		     CS_NULLTERM, CS_UNUSED);
    if(retcode != CS_SUCCEED) 
	return 0;

    if(ct_send(cmd) != CS_SUCCEED)
	return 0;

    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "    syb_db_commit() -> ct_send() OK\n");

    while((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(dbis->debug >= 2)
	    fprintf(DBILOGFP, "    syb_db_commit() -> ct_results(%d) == %d\n",
		    restype, retcode);

	if(restype == CS_CMD_FAIL)
	    failFlag = 1;
    }

    ct_cmd_drop(cmd);
    imp_dbh->inTransaction = 0;
    return !failFlag;
}

int      syb_db_rollback(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    CS_COMMAND *cmd;
    char buff[128];
    CS_INT      restype;
    CS_RETCODE  retcode;
    int         failFlag = 0;

    if(!imp_dbh->inTransaction)
	return 1;
    if(DBIc_is(imp_dbh, DBIcf_AutoCommit)) {
	warn("rollback ineffective with AutoCommit");
	return 1;
    }

    cmd = syb_alloc_cmd(imp_dbh->connection);
    sprintf(buff, "\nROLLBACK TRAN %s\n", imp_dbh->tranName);
    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "    syb_db_rollback() -> ct_command(%s)\n", buff);
    retcode = ct_command(cmd, CS_LANG_CMD, buff,
		     CS_NULLTERM, CS_UNUSED);
    if(retcode != CS_SUCCEED) 
	return 0;

    if(ct_send(cmd) != CS_SUCCEED)
	return 0;

    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "    syb_db_rollback() -> ct_send() OK\n");

    while((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(dbis->debug >= 2)
	    fprintf(DBILOGFP, "    syb_db_rollback() -> ct_results(%d) == %d\n",
		    restype, retcode);

	if(restype == CS_CMD_FAIL)
	    failFlag = 1;
    }

    ct_cmd_drop(cmd);
    imp_dbh->inTransaction = 0;
    return !failFlag;
}

static int syb_db_opentran(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    CS_COMMAND *cmd;
    char buff[128];
    CS_INT      restype;
    CS_RETCODE  retcode;
    int         failFlag = 0;

    if(DBIc_is(imp_dbh, DBIcf_AutoCommit) || imp_dbh->inTransaction)
	return 1;

    cmd = syb_alloc_cmd(imp_dbh->connection);
    sprintf(imp_dbh->tranName, "DBI%x", imp_dbh);
    sprintf(buff, "\nBEGIN TRAN %s\n", imp_dbh->tranName);
    retcode = ct_command(cmd, CS_LANG_CMD, buff,
			 CS_NULLTERM, CS_UNUSED);
    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "    syb_db_opentran() -> ct_command(%s) = %d\n", 
		buff, retcode);
    if(retcode != CS_SUCCEED) 
	return 0;
    retcode = ct_send(cmd);
    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "    syb_db_opentran() -> ct_send() = %d\n",
		retcode);

    if(retcode != CS_SUCCEED)
	return 0;

    while((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(dbis->debug >= 2)
	    fprintf(DBILOGFP, "    syb_db_opentran() -> ct_results(%d) == %d\n",
		    restype, retcode);

	if(restype == CS_CMD_FAIL)
	    failFlag = 1;
    }

    ct_cmd_drop(cmd);
    if(!failFlag)
	imp_dbh->inTransaction = 1;
    return !failFlag;
}


int      syb_db_disconnect(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    CS_RETCODE retcode;

    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "    syb_db_disconnect() -> ct_close()\n");
    if((retcode = ct_close(imp_dbh->connection, CS_FORCE_CLOSE)) != CS_SUCCEED)
	fprintf(DBILOGFP, "    syb_db_disconnect(): ct_close() failed\n");
    if((retcode = ct_con_drop(imp_dbh->connection)) != CS_SUCCEED)
	fprintf(DBILOGFP, "    syb_db_disconnect(): ct_con_drop() failed\n");

    DBIc_off(imp_dbh, DBIcf_ACTIVE);

    return 1;
}


void     syb_db_destroy(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    if (DBIc_ACTIVE(imp_dbh))
	syb_db_disconnect(dbh, imp_dbh);
    /* Nothing in imp_dbh to be freed	*/

    DBIc_IMPSET_off(imp_dbh);
}

int
syb_db_STORE_attrib(dbh, imp_dbh, keysv, valuesv)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    SV *keysv;
    SV *valuesv;
{
    STRLEN kl;
    STRLEN plen;
    int on;
    char *key = SvPV(keysv,kl);

    if (kl == 10 && strEQ(key, "AutoCommit")) {
	DBIc_set(imp_dbh, DBIcf_AutoCommit, SvTRUE(valuesv));
	return TRUE;
    }
    return FALSE;
}

SV      *syb_db_FETCH_attrib(dbh, imp_dbh, keysv)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    SV *keysv;
{
    STRLEN kl;
    STRLEN plen;
    char *key = SvPV(keysv,kl);
    int on;
    SV *retsv = NULL;

    if (kl == 10 && strEQ(key, "AutoCommit")) {
	if(DBIc_is(imp_dbh, DBIcf_AutoCommit)) 
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    return retsv;
}

static CS_COMMAND *
syb_alloc_cmd(connection) 
    CS_CONNECTION *connection;
{
    CS_RETCODE retcode;
    CS_COMMAND *cmd;

    if((retcode = ct_cmd_alloc(connection, &cmd)) != CS_SUCCEED) {
	warn("ct_cmd_alloc failed");
	return NULL;
    }
    return cmd;
}

int      
syb_st_prepare(sth, imp_sth, statement, attribs)
    SV *sth;
    imp_sth_t *imp_sth;
    char *statement;
    SV *attribs;
{
    D_imp_dbh_from_sth;
    CS_RETCODE ret;

    if(!DBIc_is(imp_dbh, DBIcf_AutoCommit))
	if(syb_db_opentran(NULL, imp_dbh) == 0)
	    return 0;
	
    imp_sth->cmd = syb_alloc_cmd(imp_dbh->connection);

    ret = ct_command(imp_sth->cmd, CS_LANG_CMD, statement,
		     CS_NULLTERM, CS_UNUSED);

    if(ret != CS_SUCCEED) 
	return 0;

    DBIc_on(imp_sth, DBIcf_IMPSET);

    return 1;
}


int      syb_st_rows(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    return imp_sth->numRows;
}

static void
cleanUp(imp_sth)
    imp_sth_t *imp_sth;
{
    int i;
    int numCols = DBIc_NUM_FIELDS(imp_sth);
    for(i = 0; i < numCols; ++i)
	if(imp_sth->coldata[i].type == CS_CHAR_TYPE ||
	   imp_sth->coldata[i].type == CS_TEXT_TYPE)
	    Safefree(imp_sth->coldata[i].value.c);
    
    if(imp_sth->datafmt)
	Safefree(imp_sth->datafmt);
    if(imp_sth->coldata)
	Safefree(imp_sth->coldata);
    imp_sth->numCols = 0;
    imp_sth->coldata = NULL;
    imp_sth->datafmt = NULL;
}

static CS_RETCODE
describe(imp_sth, restype)
    imp_sth_t *imp_sth;
    int restype;
{
    CS_RETCODE retcode;
    int i;
    int numCols;
    
    if((retcode = ct_res_info(imp_sth->cmd, CS_NUMDATA,
			      &numCols, CS_UNUSED, NULL)) != CS_SUCCEED)
    {
	warn("ct_res_info() failed");
	goto GoodBye;
    }
    if(numCols <= 0) {
	warn("ct_res_info() returned 0 columns");
	DBIc_NUM_FIELDS(imp_sth) = numCols;
	imp_sth->numCols = 0;
	goto GoodBye;
    }
    DBIc_NUM_FIELDS(imp_sth) = numCols;
    imp_sth->numCols = numCols;
    
    New(902, imp_sth->coldata, numCols, ColData);
    New(902, imp_sth->datafmt, numCols, CS_DATAFMT);
    
    /* this routine may be called without the connection reference */
    if(restype == CS_COMPUTE_RESULT) {
	CS_INT comp_id, outlen;
    
	if((retcode = ct_compute_info(imp_sth->cmd, CS_COMP_ID, CS_UNUSED,
			      &comp_id, CS_UNUSED, &outlen)) != CS_SUCCEED)
	{
	    warn("ct_compute_info failed");
	    goto GoodBye;
	}
    }

    for(i = 0; i < numCols; ++i) {
	if((retcode = ct_describe(imp_sth->cmd, (i + 1),
				  &imp_sth->datafmt[i])) != CS_SUCCEED)
	{
	    warn("ct_describe() failed");
	    cleanUp(imp_sth);
	    goto GoodBye;
	}
	/* Make sure we have at least some sort of column name: */
	if(imp_sth->datafmt[i].namelen == 0)
	    sprintf(imp_sth->datafmt[i].name, "COL(%d)", i+1);
	if(restype == CS_COMPUTE_RESULT) {
	    CS_INT agg_op, outlen;
	    CS_CHAR *agg_op_name;
	    
	    if((retcode = ct_compute_info(imp_sth->cmd, CS_COMP_OP, (i + 1),
				  &agg_op, CS_UNUSED, &outlen)) != CS_SUCCEED)
	    {
		warn("ct_compute_info failed");
		goto GoodBye;
	    }
	    agg_op_name = GetAggOp(agg_op);
	    if((retcode = ct_compute_info(imp_sth->cmd, CS_COMP_COLID, (i + 1),
				  &agg_op, CS_UNUSED, &outlen)) != CS_SUCCEED)
	    {
		warn("ct_compute_info failed");
		goto GoodBye;
	    }
	    sprintf(imp_sth->datafmt[i].name, "%s(%d)", agg_op_name, agg_op);
	}

	imp_sth->coldata[i].realType = imp_sth->datafmt[i].datatype;
	switch(imp_sth->datafmt[i].datatype) 
	{
	  case CS_BIT_TYPE:
	  case CS_TINYINT_TYPE:
	  case CS_SMALLINT_TYPE:
	  case CS_INT_TYPE:
	    imp_sth->datafmt[i].maxlength = sizeof(CS_INT);
	    imp_sth->datafmt[i].format    = CS_FMT_UNUSED;
	    imp_sth->coldata[i].type      = CS_INT_TYPE;
	    imp_sth->datafmt[i].datatype  = CS_INT_TYPE;
	    retcode = ct_bind(imp_sth->cmd, (i + 1), &imp_sth->datafmt[i],
			      &imp_sth->coldata[i].value.i,
			      &imp_sth->coldata[i].valuelen,
			      &imp_sth->coldata[i].indicator);
	    break;
	    
	  case CS_REAL_TYPE:
	  case CS_FLOAT_TYPE:
	  case CS_MONEY_TYPE:
	  case CS_MONEY4_TYPE:
	    imp_sth->datafmt[i].maxlength = sizeof(CS_FLOAT);
	    imp_sth->datafmt[i].format    = CS_FMT_UNUSED;
	    imp_sth->coldata[i].type      = CS_FLOAT_TYPE;
	    imp_sth->datafmt[i].datatype  = CS_FLOAT_TYPE;
	    retcode = ct_bind(imp_sth->cmd, (i + 1), &imp_sth->datafmt[i],
			      &imp_sth->coldata[i].value.f,
			      &imp_sth->coldata[i].valuelen,
			      &imp_sth->coldata[i].indicator);
	    break;
	    
	  case CS_TEXT_TYPE:
	  case CS_IMAGE_TYPE:
	    New(902, imp_sth->coldata[i].value.c,
		imp_sth->datafmt[i].maxlength, char);
	    imp_sth->datafmt[i].format   = CS_FMT_UNUSED; /*CS_FMT_NULLTERM;*/
	    imp_sth->coldata[i].type     = CS_TEXT_TYPE;
	    imp_sth->datafmt[i].datatype = CS_TEXT_TYPE;
	    retcode = ct_bind(imp_sth->cmd, (i + 1), &imp_sth->datafmt[i],
			      imp_sth->coldata[i].value.c,
			      &imp_sth->coldata[i].valuelen,
			      &imp_sth->coldata[i].indicator);
	    break;
	    
	  case CS_CHAR_TYPE:
	  case CS_VARCHAR_TYPE:
	  case CS_BINARY_TYPE:
	  case CS_VARBINARY_TYPE:
	  case CS_NUMERIC_TYPE:
	  case CS_DECIMAL_TYPE:
	  case CS_DATETIME_TYPE:
	  case CS_DATETIME4_TYPE:
	  default:
	    imp_sth->datafmt[i].maxlength =
		display_dlen(&imp_sth->datafmt[i]) + 1;
	    imp_sth->datafmt[i].format   = CS_FMT_UNUSED;
	    New(902, imp_sth->coldata[i].value.c,
		imp_sth->datafmt[i].maxlength, char);
	    imp_sth->coldata[i].type      = CS_CHAR_TYPE;
	    imp_sth->datafmt[i].datatype  = CS_CHAR_TYPE;
	    retcode = ct_bind(imp_sth->cmd, (i + 1), &imp_sth->datafmt[i],
			      imp_sth->coldata[i].value.c,
			      &imp_sth->coldata[i].valuelen,
			      &imp_sth->coldata[i].indicator);
	    break;
	}	
	/* check the return code of the call to ct_bind in the
	   switch above: */
	if (retcode != CS_SUCCEED) {
	    warn("ct_bind() failed");
	    cleanUp(imp_sth);
	    break;
	}
    }
  GoodBye:;
    if(retcode == CS_SUCCEED) {
	imp_sth->done_desc = 1;
    }
    return retcode == CS_SUCCEED;
}

static int
st_next_result(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    CS_COMMAND *cmd = imp_sth->cmd;
    CS_INT      restype;
    CS_RETCODE  retcode;
    int         failFlag = 0;

    imp_sth->numRows = -1;

    while((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(dbis->debug >= 2)
	    fprintf(DBILOGFP, "    st_next_result() -> ct_results(%d) == %d\n",
		    restype, retcode);

	if(restype == CS_CMD_FAIL)
	    failFlag = 1;
	if(restype == CS_CMD_DONE && !failFlag) {
	    ct_res_info(cmd, CS_ROW_COUNT, &imp_sth->numRows, CS_UNUSED, NULL);
	}
	switch(restype) 
	{
	  case CS_ROW_RESULT:
	  case CS_PARAM_RESULT:
	  case CS_STATUS_RESULT:
	  case CS_CURSOR_RESULT:
	  case CS_COMPUTE_RESULT:
	      if(imp_sth->done_desc) {
		  cleanUp(imp_sth);
	      }
	      retcode = describe(imp_sth, restype);
	      if(dbis->debug >= 2)
		  fprintf(DBILOGFP, "describe() retcode = %d\n", retcode);
	      goto Done;
	}
    }
    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "ct_execute() final retcode = %d\n", retcode);
  Done:
    if(failFlag)
	return CS_CMD_FAIL;

    imp_sth->lastResType = restype;
  
    return restype;
}

int      syb_st_execute(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    CS_COMMAND *cmd = imp_sth->cmd;
    int restype;

    if(ct_send(cmd) != CS_SUCCEED) {
	return -2;
    }
    if(dbis->debug >= 2)
	fprintf(DBILOGFP, "    syb_st_execute() -> ct_send() OK\n");

    restype = st_next_result(sth, imp_sth);
    if(restype == CS_CMD_FAIL)
	return -2;

    DBIc_on(imp_sth, DBIcf_ACTIVE);

    return imp_sth->numRows;
}


AV *
syb_st_fetch(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;
    CS_COMMAND *cmd = imp_sth->cmd;
    int debug = DBIS->debug;
    int num_fields;
    int ChopBlanks;
    int i;
    AV *av;
    CS_RETCODE retcode;
    CS_INT rows_read, restype;
    int len;

    /* Check that execute() was executed sucessfully. This also implies	*/
    /* that describe() executed sucessfuly so the memory buffers	*/
    /* are allocated and bound.						*/
    if ( !DBIc_is(imp_sth, DBIcf_ACTIVE) ) {
	warn("no statement executing");
	return Nullav;
    }

    av = DBIS->get_fbav(imp_sth);
    num_fields = AvFILL(av)+1;

    ChopBlanks = DBIc_has(imp_sth, DBIcf_ChopBlanks);

  TryAgain:
    retcode = ct_fetch(cmd, CS_UNUSED, CS_UNUSED, CS_UNUSED, &rows_read);

    switch(retcode)
    {
      case CS_ROW_FAIL:		/* not sure how I should handle this one! */
	goto TryAgain;
      case CS_SUCCEED:
	for(i = 0; i < num_fields; ++i)
	{
	    SV *sv = AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/
	    len = 0;

	    if(i >= imp_sth->numCols || imp_sth->coldata[i].indicator == CS_NULLDATA) {
		/* NULL data */
		(void)SvOK_off(sv);
	    } else {
		switch(imp_sth->coldata[i].type) {
		  case CS_TEXT_TYPE:
		  case CS_CHAR_TYPE:
		      len = imp_sth->coldata[i].valuelen;
		      sv_setpvn(sv, imp_sth->coldata[i].value.c, len);
		      if(imp_sth->coldata[i].realType == CS_CHAR_TYPE && 
			 ChopBlanks) 
		      {
			  char *p = SvEND(sv);
			  int len = SvCUR(sv);
			  while(len && *--p == ' ')
			      --len;
			  if (len != SvCUR(sv)) {
			      SvCUR_set(sv, len);
			      *SvEND(sv) = '\0';
			  }
		      }
		      break;
		  case CS_FLOAT_TYPE:
		      sv_setnv(sv, imp_sth->coldata[i].value.f);
		      break;
		  case CS_INT_TYPE:
		      sv_setiv(sv, imp_sth->coldata[i].value.i);
		      break;
		  default:
		    croak("syb_st_fetch: unknown datatype: %d, column %d",
			  imp_sth->datafmt[i].datatype, i);
		}
	    }
	}
	break;
      case CS_FAIL:		/* ohmygod */
	/* FIXME: Should we call ct_cancel() here, or should we let
	   the programmer handle it? */
	if(ct_cancel(imp_dbh->connection, NULL, CS_CANCEL_ALL) == CS_FAIL)
	    croak("ct_cancel() failed - dying");
	break;
      case CS_END_DATA:		/* we've seen all the data for this result
				   set. So see if this is the end of the
				   result sets */

	  restype = st_next_result(sth, imp_sth);
	  if(dbis->debug >= 2)
	      fprintf(DBILOGFP, "    syb_st_fetch() -> st_next_results() == %d\n",
		      restype);

	  if(restype == CS_CMD_DONE) {
	      imp_sth->moreResults = 0;
	      return Nullav;
	  } else {		/* XXX What to do here??? */
	      if(restype == CS_COMPUTE_RESULT)
		  goto TryAgain;
	      imp_sth->moreResults = 1;
	  }
	  return Nullav;
	break;
      default:
	warn("ct_fetch() returned an unexpected retcode");
    }

    return av;
}


int      syb_st_finish(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;

    if (DBIc_ACTIVE(imp_sth)) {
	if(ct_cancel(imp_dbh->connection, NULL, CS_CANCEL_ALL) == CS_FAIL)
	    croak("ct_cancel() failed - dying");
    }
    DBIc_ACTIVE_off(imp_sth);
    return 1;
}

void     syb_st_destroy(sth, imp_sth) 
    SV *sth;
    imp_sth_t *imp_sth;
{
    int ret;

    cleanUp(imp_sth);
    ret = ct_cmd_drop(imp_sth->cmd);
    if(dbis->debug >= 2) {
	fprintf(DBILOGFP, "    syb_st_destroy(): cmd dropped: %d\n", ret);
    }

    DBIc_IMPSET_off(imp_sth);		/* let DBI know we've done it	*/
}

int      syb_st_blob_read(sth, imp_sth, field, offset, len,
			  destrv, destoffset)
    SV *sth;
    imp_sth_t *imp_sth;
    int field;
    long offset;
    long len;
    SV *destrv;
    long destoffset;
{
  return 1;
}


/* Borrowed from DBD::ODBC */

typedef struct {
    const char *str;
    unsigned len:8;
    unsigned array:1;
    unsigned filler:23;
} T_st_params;

#define s_A(str) { str, sizeof(str)-1 }
static T_st_params S_st_fetch_params[] = 
{
    s_A("NUM_OF_PARAMS"),	/* 0 */
    s_A("NUM_OF_FIELDS"),	/* 1 */
    s_A("NAME"),		/* 2 */
    s_A("NULLABLE"),		/* 3 */
    s_A("TYPE"),		/* 4 */
    s_A("PRECISION"),		/* 5 */
    s_A("SCALE"),		/* 6 */
    s_A("syb_more_results"),	/* 7 */
    s_A(""),			/* END */
};

static T_st_params S_st_store_params[] = 
{
    s_A(""),			/* END */
};
#undef s_A

SV *
syb_st_FETCH_attrib(sth, imp_sth, keysv)
    SV *sth;
    imp_sth_t *imp_sth;
    SV *keysv;
{
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    int i;
    SV *retsv = NULL;
    T_st_params *par;
    int n_fields;
    imp_fbh_t *fbh;
    int rc;

    for (par = S_st_fetch_params; par->len > 0; par++)
	if (par->len == kl && strEQ(key, par->str))
	    break;

    if (par->len <= 0)
	return Nullsv;

    if (!imp_sth->done_desc) {
	/* Because of the way Sybase returns information on returned values
	   in a SELECT statement we can't call describe() here. */
	return Nullsv;
    }

    i = DBIc_NUM_FIELDS(imp_sth);
 
    switch(par - S_st_fetch_params) {  
	AV *av;

	case 0:			/* NUM_OF_PARAMS */
	    return Nullsv;	/* handled by DBI */
        case 1:			/* NUM_OF_FIELDS */
	    retsv = newSViv(i);
	    break;
	case 2: 			/* NAME */
	    av = newAV();
	    retsv = newRV(sv_2mortal((SV*)av));
	    while(--i >= 0)
		av_store(av, i, newSVpv(imp_sth->datafmt[i].name, 0));
	    break;
	case 3:			/* NULLABLE */
	    av = newAV();
	    retsv = newRV(sv_2mortal((SV*)av));
	    while(--i >= 0)
		av_store(av, i,
		    (imp_sth->datafmt[i].status & CS_CANBENULL)
			? &sv_yes : &sv_no);
	    break;
	case 4:			/* TYPE */
	    av = newAV();
	    retsv = newRV(sv_2mortal((SV*)av));
	    while(--i >= 0) 
		av_store(av, i, newSViv(imp_sth->datafmt[i].datatype));
	    break;
        case 5:			/* PRECISION */
	    av = newAV();
	    retsv = newRV(sv_2mortal((SV*)av));
	    while(--i >= 0) 
		av_store(av, i, newSViv(imp_sth->datafmt[i].precision));
	    break;
	case 6:			/* SCALE */
	    av = newAV();
	    retsv = newRV(sv_2mortal((SV*)av));
	    while(--i >= 0) 
		av_store(av, i, newSViv(imp_sth->datafmt[i].scale));
	    break;
	case 7:
	    retsv = newSViv(imp_sth->moreResults);
	    break;
	case 8:
	    retsv = newSViv(DBIc_LongReadLen(imp_sth));
	    break;
	default:
	    return Nullsv;
	}

    return sv_2mortal(retsv);
}


int
syb_st_STORE_attrib(sth, imp_sth, keysv, valuesv)
    SV *sth;
    imp_sth_t *imp_sth;
    SV *keysv;
    SV *valuesv;
{
    D_imp_dbh_from_sth;
    STRLEN kl;
    STRLEN vl;
    char *key = SvPV(keysv,kl);
    char *value = SvPV(valuesv, vl);
    T_st_params *par;
    int rc;
 
    for (par = S_st_store_params; par->len > 0; par++)
	if (par->len == kl && strEQ(key, par->str))
	    break;

    if (par->len <= 0)
	return FALSE;

    switch(par - S_st_store_params)
	{
	case 0:/*  */
	    return TRUE;
	}
    return FALSE;
}
 
int      syb_bind_ph(sth, imp_sth, param, value, sql_type,
		     attribs, is_inout, maxlen)
    SV *sth;
    imp_sth_t *imp_sth;
    SV *param;
    SV *value;
    IV sql_type;
    SV *attribs;
    int is_inout;
    IV maxlen;
{
  return 1;
}

/* FIXME:
   All of the output in this function goes to stdout. The function is
   called from servermsg_cb, which normally outputs it's messages
   to stderr... */

static CS_RETCODE
fetch_data(cmd)
CS_COMMAND	*cmd;
{
    CS_RETCODE	retcode;
    CS_INT	num_cols;
    CS_INT	i;
    CS_INT	j;
    CS_INT	row_count = 0;
    CS_INT	rows_read;
    CS_INT	disp_len;
    CS_DATAFMT	*datafmt;
    ColData	*coldata;

    /*
     ** Find out how many columns there are in this result set.
     */
    if((retcode = ct_res_info(cmd, CS_NUMDATA,
			      &num_cols, CS_UNUSED, NULL)) != CS_SUCCEED)
    {
	warn("fetch_data: ct_res_info() failed");
	return retcode;
    }

    /*
     ** Make sure we have at least one column
     */
    if (num_cols <= 0)
    {
	warn("fetch_data: ct_res_info() returned zero columns");
	return CS_FAIL;
    }

    New(902, coldata, num_cols, ColData);
    New(902, datafmt, num_cols, CS_DATAFMT);

    for (i = 0; i < num_cols; i++)
    {
	if((retcode = ct_describe(cmd, (i + 1), &datafmt[i])) != CS_SUCCEED)
	{
	    warn("fetch_data: ct_describe() failed");
	    break;
	}
	datafmt[i].maxlength = display_dlen(&datafmt[i]) + 1;
	datafmt[i].datatype = CS_CHAR_TYPE;
	datafmt[i].format   = CS_FMT_NULLTERM;

	New(902, coldata[i].value.c, datafmt[i].maxlength, char);
	if((retcode = ct_bind(cmd, (i + 1), &datafmt[i],
			      coldata[i].value.c, &coldata[i].valuelen,
			      &coldata[i].indicator)) != CS_SUCCEED)
	{
	    warn("fetch_data: ct_bind() failed");
	    break;
	}
    }
    if (retcode != CS_SUCCEED)
    {
	for (j = 0; j < i; j++)
	{
	    Safefree(coldata[j].value.c);
	}
	Safefree(coldata);
	Safefree(datafmt);
	return retcode;
    }

    display_header(num_cols, datafmt);

    while (((retcode = ct_fetch(cmd, CS_UNUSED, CS_UNUSED, CS_UNUSED,
				&rows_read)) == CS_SUCCEED)
	   || (retcode == CS_ROW_FAIL))
    {
	row_count = row_count + rows_read;

		/*
		** Check if we hit a recoverable error.
		*/
	if (retcode == CS_ROW_FAIL)
	{
	    fprintf(DBILOGFP, "Error on row %ld.\n", row_count);
	    fflush(DBILOGFP);
	}

	/*
	 ** We have a row.  Loop through the columns displaying the
	 ** column values.
	 */
	for (i = 0; i < num_cols; i++)
	{	  
	    /*
	     ** Display the column value
	     */
	    fprintf(DBILOGFP, "%s", coldata[i].value.c);
	    fflush(DBILOGFP);

	    /*
	     ** If not last column, Print out spaces between this
	     ** column and next one. 
	     */
	    if (i != num_cols - 1)
	    {
		disp_len = display_dlen(&datafmt[i]);
		disp_len -= coldata[i].valuelen - 1;
		for (j = 0; j < disp_len; j++)
		{
		    fputc(' ', DBILOGFP);
		}
	    }
	} 
	fprintf(DBILOGFP, "\n");
	fflush(DBILOGFP);
    }

    /*
     ** Free allocated space.
     */
    for (i = 0; i < num_cols; i++)
    {
	Safefree(coldata[i].value.c);
    }
    Safefree(coldata);
    Safefree(datafmt);
    
    /*
     ** We're done processing rows.  Let's check the final return
     ** value of ct_fetch().
     */
    switch ((int)retcode)
    {
      case CS_END_DATA:
	retcode = CS_SUCCEED;
	break;

      case CS_FAIL:
	warn("fetch_data: ct_fetch() failed");
	return retcode;
	break;

      default:			/* unexpected return value! */
	warn("fetch_data: ct_fetch() returned an expected retcode");
	return retcode;
	break;
    }
    return retcode;
}

