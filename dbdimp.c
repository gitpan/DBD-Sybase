/* $Id: dbdimp.c,v 1.75 2004/11/25 13:34:58 mpeppler Exp $

   Copyright (c) 1997-2004  Michael Peppler

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.
   
   Based on DBD::Oracle dbdimp.c, Copyright (c) 1994,1995 Tim Bunce

*/

#include "Sybase.h"

/* Defines needed for perl 5.005 / threading */

#if defined(op)
#undef op
#endif
#if !defined(dTHR)
#define dTHR	extern int errno
#endif

#if !defined(PATCHLEVEL)
#include "patchlevel.h"		/* this is the perl patchlevel.h */
#endif

#if PATCHLEVEL < 5 && SUBVERSION < 5

#define PL_na na
#define PL_sv_undef sv_undef
#define PL_dirty dirty

#endif

#ifndef PerlIO
#  define PerlIO FILE
#  define PerlIO_printf fprintf
#  define PerlIO_stderr() stderr
#  define PerlIO_close(f) fclose(f)
#  define PerlIO_open(f,m) fopen(f,m)
#  define PerlIO_flush(f) fflush(f)
#  define PerlIO_puts(f,s) fputs(s,f)
#endif

/* Requested by Alex Fridman */
#ifdef WIN32
#   define strncasecmp _strnicmp
#endif

/*#define NO_CHAINED_TRAN 1*/
#if !defined(NO_CHAINED_TRAN)
#define NO_CHAINED_TRAN 0
#endif

/* some systems have trouble with ct_cancel().
   If FLUSH_FINISH is 1 then the default behavior is to fetch all results
   from the server when $sth->finish() is called instead of the normal
   ct_cancel(CS_CANCEL_ALL) call. */
#if !defined(FLUSH_FINISH)
#define FLUSH_FINISH 0
#endif

#if !defined(PROC_STATUS)
#define PROC_STATUS 0
#endif


DBISTATE_DECLARE;

static void cleanUp _((imp_sth_t *));
static char *GetAggOp _((CS_INT));
static CS_INT get_cwidth _((CS_DATAFMT *));
static CS_INT display_dlen _((CS_DATAFMT *));
static CS_RETCODE display_header _((imp_dbh_t *, CS_INT, CS_DATAFMT*));
static CS_RETCODE describe _((imp_sth_t *, int));
static CS_RETCODE fetch_data _((imp_dbh_t *, CS_COMMAND*));
static CS_RETCODE CS_PUBLIC clientmsg_cb _((CS_CONTEXT*, CS_CONNECTION*, CS_CLIENTMSG*));
static CS_RETCODE CS_PUBLIC servermsg_cb _((CS_CONTEXT*, CS_CONNECTION*, CS_SERVERMSG*));
static CS_RETCODE CS_PUBLIC cslibmsg_cb(CS_CONTEXT *context, CS_CLIENTMSG *errmsg);
static CS_COMMAND *syb_alloc_cmd _((imp_dbh_t *, CS_CONNECTION*));
static void dealloc_dynamic _((imp_sth_t *));
static int map_syb_types _((int));
static int map_sql_types _((int));
static CS_CONNECTION *syb_db_connect _((struct imp_dbh_st *));
static int syb_db_use _((imp_dbh_t *, CS_CONNECTION *));
static int syb_st_describe_proc _((imp_sth_t *, char *));
static void syb_set_error(imp_dbh_t *, int, char *);
static char *my_strdup _((char *));
static void fetchKerbTicket(imp_dbh_t *imp_dbh);
static CS_RETCODE syb_blk_init(imp_dbh_t *imp_dbh, imp_sth_t *imp_sth);
static void blkCleanUp(imp_sth_t *imp_sth);
static int getTableName(char *statement, char *table, int maxwidth);
static int toggle_autocommit(SV *dbh, imp_dbh_t *imp_dbh, int flag);
static int date2str(CS_DATETIME *dt, CS_DATAFMT *srcfmt, 
		    char *buff, CS_INT len, int type, CS_LOCALE *locale);
static int syb_get_date_fmt(imp_dbh_t *imp_dbh, char *fmt);

static CS_INT BLK_VERSION;

#if PERL_VERSION >= 8 && defined(_REENTRANT)
static perl_mutex context_alloc_mutex[1];
#endif

/*#define USE_CSLIB_CB 1 */

static CS_CONTEXT *context;
static CS_LOCALE *locale;
static char scriptName[255];
static char hostname[255];
static char *ocVersion;

#define LOCALE(s)	((s)->locale ? (s)->locale : locale)

static SV *cslib_cb;

static void
syb_set_error(imp_dbh, err, errstr)
imp_dbh_t *imp_dbh;
int        err;
char      *errstr;
{
    sv_setiv(DBIc_ERR(imp_dbh), err);
    if(SvOK(DBIc_ERRSTR(imp_dbh))) 
	sv_catpv(DBIc_ERRSTR(imp_dbh), errstr);
    else
	sv_setpv(DBIc_ERRSTR(imp_dbh), errstr);
}

static CS_RETCODE CS_PUBLIC
cslibmsg_cb(CS_CONTEXT *context, CS_CLIENTMSG *errmsg)
{

    if(DBIS->debug >= 3) {
	PerlIO_printf(DBILOGFP, "    cslibmsg_cb -> %s\n", errmsg->msgstring);
	if (errmsg->osstringlen > 0) {
	    PerlIO_printf(DBILOGFP, "    cslibmsg_cb -> %s\n", errmsg->osstring);
	}
    }

    if(cslib_cb)
    {
	dSP;
	int retval, count;

	ENTER;
	SAVETMPS;
	PUSHMARK(sp);

	XPUSHs(sv_2mortal(newSViv(CS_LAYER(errmsg->msgnumber))));
	XPUSHs(sv_2mortal(newSViv(CS_ORIGIN(errmsg->msgnumber))));
	XPUSHs(sv_2mortal(newSViv(CS_SEVERITY(errmsg->msgnumber))));
	XPUSHs(sv_2mortal(newSViv(CS_NUMBER(errmsg->msgnumber))));
	XPUSHs(sv_2mortal(newSVpv(errmsg->msgstring, 0)));
	if (errmsg->osstringlen > 0)
	    XPUSHs(sv_2mortal(newSVpv(errmsg->osstring, 0)));
	else
	    XPUSHs(&PL_sv_undef);
    
	PUTBACK;
	if((count = perl_call_sv(cslib_cb, G_SCALAR)) != 1)
	    croak("A cslib handler cannot return a LIST");
	SPAGAIN;
	retval = POPi;
	
	PUTBACK;
	FREETMPS;
	LEAVE;
	
	return retval;
    }
    PerlIO_printf(PerlIO_stderr(), 
		  "\nCS Library Message:\n");
    PerlIO_printf(PerlIO_stderr(), 
		  "Message number: LAYER = (%ld) ORIGIN = (%ld) ",
		  CS_LAYER(errmsg->msgnumber), CS_ORIGIN(errmsg->msgnumber));
    PerlIO_printf(PerlIO_stderr(), "SEVERITY = (%ld) NUMBER = (%ld)\n",
		  CS_SEVERITY(errmsg->msgnumber), 
		  CS_NUMBER(errmsg->msgnumber));
    PerlIO_printf(PerlIO_stderr(), "Message String: %s\n", errmsg->msgstring);
    if (errmsg->osstringlen > 0)
    {
	PerlIO_printf(PerlIO_stderr(), "Operating System Error: %s\n",
		      errmsg->osstring);
    }

    return CS_SUCCEED;
}

static CS_RETCODE CS_PUBLIC
clientmsg_cb(context, connection, errmsg)
CS_CONTEXT	*context;
CS_CONNECTION	*connection;	
CS_CLIENTMSG	*errmsg;
{
    imp_dbh_t *imp_dbh = NULL;
    char buff[255];

    if(DBIS->debug >= 3) {
	PerlIO_printf(DBILOGFP, "    clientmsg_cb -> %s\n", errmsg->msgstring);
	if (errmsg->osstringlen > 0) {
	    PerlIO_printf(DBILOGFP, "    clientmsg_cb -> %s\n", errmsg->osstring);
	}
    }

    if(connection) {
	if((ct_con_props(connection, CS_GET, CS_USERDATA,
			 &imp_dbh, CS_SIZEOF(imp_dbh), NULL)) != CS_SUCCEED)
	    croak("Panic: clientmsg_cb: Can't find handle from connection");
	
	/* if LongTruncOK is set then ignore this error. */
	if(DBIc_is(imp_dbh, DBIcf_LongTruncOk) &&
	   CS_NUMBER(errmsg->msgnumber) == 132)
	    return CS_SUCCEED;

	if(imp_dbh->err_handler) {
	    dSP;
	    int retval, count;
	    
	    ENTER;
	    SAVETMPS;
	    PUSHMARK(sp);
	    
	    
	    XPUSHs(sv_2mortal(newSViv(CS_NUMBER(errmsg->msgnumber))));
	    XPUSHs(sv_2mortal(newSViv(CS_SEVERITY(errmsg->msgnumber))));
	    XPUSHs(sv_2mortal(newSViv(0)));
	    XPUSHs(sv_2mortal(newSViv(0)));
	    XPUSHs(&PL_sv_undef);
	    XPUSHs(&PL_sv_undef);
	    XPUSHs(sv_2mortal(newSVpv(errmsg->msgstring, 0)));
	    if(imp_dbh->sql)
		XPUSHs(sv_2mortal(newSVpv(imp_dbh->sql, 0)));
	    else
		XPUSHs(&PL_sv_undef);

	    XPUSHs(sv_2mortal(newSVpv("client", 0)));
	    
	    PUTBACK;
	    if((count = perl_call_sv(imp_dbh->err_handler, G_SCALAR | G_EVAL)) != 1)
		croak("An error handler can't return a LIST.");
	    SPAGAIN;

	    if(SvTRUE(ERRSV)) {
		POPs;
		retval = 1;
	    } else {
		retval = POPi;
	    }
	    
	    PUTBACK;
	    FREETMPS;
	    LEAVE;
	    
	    /* If the called sub returns 0 then ignore this error */
	    if(retval == 0)
		return CS_SUCCEED;
	}

	sv_setiv(DBIc_ERR(imp_dbh), (IV)CS_NUMBER(errmsg->msgnumber));
    
	if(SvOK(DBIc_ERRSTR(imp_dbh))) 
	    sv_catpv(DBIc_ERRSTR(imp_dbh), "OpenClient message: ");
	else
	    sv_setpv(DBIc_ERRSTR(imp_dbh), "OpenClient message: ");
	sprintf(buff, "LAYER = (%ld) ORIGIN = (%ld) ",
		CS_LAYER(errmsg->msgnumber), CS_ORIGIN(errmsg->msgnumber));
	sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	sprintf(buff, "SEVERITY = (%ld) NUMBER = (%ld)\n",
		CS_SEVERITY(errmsg->msgnumber), CS_NUMBER(errmsg->msgnumber));
	sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	sprintf(buff, "Server %s, database %s\n",
		imp_dbh->server, imp_dbh->curr_db);
	sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	sv_catpv(DBIc_ERRSTR(imp_dbh), "Message String: ");
	sv_catpv(DBIc_ERRSTR(imp_dbh), errmsg->msgstring);
	sv_catpv(DBIc_ERRSTR(imp_dbh), "\n");
	if (errmsg->osstringlen > 0) {
	    sv_catpv(DBIc_ERRSTR(imp_dbh), "Operating System Error: ");
	    sv_catpv(DBIc_ERRSTR(imp_dbh), errmsg->osstring);
	    sv_catpv(DBIc_ERRSTR(imp_dbh), "\n");
	}

	if(CS_NUMBER(errmsg->msgnumber) == 6) { /* disconnect */
	    imp_dbh->isDead = 1;
	}

	/* If this is a timeout message, cancel the current request.
	   If the cancel fails, then return CS_FAIL, and mark
	   the connection dead.
	   Do NOT return CS_FAIL in all cases, as this makes the
	   connection unusable, and that may not be the correct
	   behavior in all situations. */
	
	if (CS_SEVERITY(errmsg->msgnumber) == CS_SV_RETRY_FAIL &&
	    CS_NUMBER(errmsg->msgnumber) == 63 &&
	    CS_ORIGIN(errmsg->msgnumber) == 2 &&
	    CS_LAYER(errmsg->msgnumber) == 1) {
	    if(ct_cancel(connection, NULL, CS_CANCEL_ATTN) == CS_FAIL) {
/*		ct_close(connection, CS_FORCE_CLOSE);   XXX */
		imp_dbh->isDead = 1;
		return CS_FAIL;
	    }	  
/*	    imp_dbh->isDead = 1;	 XXX */
	    return CS_SUCCEED;
	}
    } else {			/* !connection */
	PerlIO_printf(PerlIO_stderr(), "OpenClient message: ");
	PerlIO_printf(PerlIO_stderr(), "LAYER = (%ld) ORIGIN = (%ld) ",
		CS_LAYER(errmsg->msgnumber), CS_ORIGIN(errmsg->msgnumber));
	PerlIO_printf(PerlIO_stderr(), "SEVERITY = (%ld) NUMBER = (%ld)\n",
		CS_SEVERITY(errmsg->msgnumber), CS_NUMBER(errmsg->msgnumber));
	PerlIO_printf(PerlIO_stderr(), "Message String: %s\n", errmsg->msgstring);
	if (errmsg->osstringlen > 0) {
	    PerlIO_printf(PerlIO_stderr(), "Operating System Error: %s\n",
		    errmsg->osstring);
	}
    }

    return CS_SUCCEED;
}

static CS_RETCODE CS_PUBLIC
servermsg_cb(context, connection, srvmsg)
CS_CONTEXT	*context;
CS_CONNECTION	*connection;
CS_SERVERMSG	*srvmsg;
{
    CS_COMMAND	*cmd;
    CS_RETCODE	retcode;
    imp_dbh_t *imp_dbh = NULL;
    char buff[1024];

    if(DBIS->debug >= 3) {
	if(srvmsg->msgnumber) {
	    PerlIO_printf(DBILOGFP, "    servermsg_cb -> number=%ld severity=%ld ",
		    srvmsg->msgnumber, srvmsg->severity);
	    PerlIO_printf(DBILOGFP, "state=%ld line=%ld ",
		    srvmsg->state, srvmsg->line);
	    if (srvmsg->svrnlen > 0) {
		PerlIO_printf(DBILOGFP, "server=%s ", srvmsg->svrname);
	    }
	    if (srvmsg->proclen > 0) {
		PerlIO_printf(DBILOGFP, "procedure=%s ", srvmsg->proc);
	    }
	    PerlIO_printf(DBILOGFP, "text=%s\n", srvmsg->text);
	} else {
	    PerlIO_printf(DBILOGFP, "   servermsg_cb -> %s\n", srvmsg->text);
	}
    }

    /* add check on connection not being NULL (PR/477)
       just to be on the safe side - freetds can call the server
       callback with a NULL connection */
    if(connection && (ct_con_props(connection, CS_GET, CS_USERDATA,
		     &imp_dbh, CS_SIZEOF(imp_dbh), NULL)) != CS_SUCCEED)
	croak("Panic: servermsg_cb: Can't find handle from connection");

    /* Track the "current" database */
    /* Borrowed from sqsh's cmd_connect.c */
    if(srvmsg->msgnumber == 5701 || srvmsg->msgnumber == 5703 
       || srvmsg->msgnumber == 5704) 
    {
	char *c;
	int i;
	if(srvmsg->text != NULL && (c = strchr( srvmsg->text, '\'' )) != NULL) 
	{
	    i = 0;
	    for( ++c; i <= 30 && *c != '\0' && *c != '\''; ++c )
		buff[i++] = *c;
	    buff[i] = '\0';
	    
	    /*
	     * On some systems, if the charset is mis-configured in the
	     * SQL Server, it will come back as the string "<NULL>".  If
	     * this is the case, then we want to ignore this value.
	     */
	    if (strcmp( buff, "<NULL>" ) != 0)
	    {
		switch (srvmsg->msgnumber) 
		{
		  case 5701: 
		    if(imp_dbh && DBIc_ACTIVE(imp_dbh) && 
		       imp_dbh->connection == connection) {
			strcpy(imp_dbh->curr_db, buff);
		    }
		    break;
		  case 5703: /* Language */
		    break;
		  case 5704: /* charset */
		    break;
		  default:
		    break;
		}
	    }
	}
	return CS_SUCCEED;
    }

    if(imp_dbh && imp_dbh->err_handler) {
	dSP;
	int retval, count;

	ENTER;
	SAVETMPS;
	PUSHMARK(sp);
	    
	
	XPUSHs(sv_2mortal(newSViv(srvmsg->msgnumber)));
	XPUSHs(sv_2mortal(newSViv(srvmsg->severity)));
	XPUSHs(sv_2mortal(newSViv(srvmsg->state)));
	XPUSHs(sv_2mortal(newSViv(srvmsg->line)));
	if(srvmsg->svrnlen > 0)
	    XPUSHs(sv_2mortal(newSVpv(srvmsg->svrname, 0)));
	else
	    XPUSHs(&PL_sv_undef);
	if(srvmsg->proclen > 0)
	    XPUSHs(sv_2mortal(newSVpv(srvmsg->proc, 0)));
	else
	    XPUSHs(&PL_sv_undef);
	XPUSHs(sv_2mortal(newSVpv(srvmsg->text, 0)));
	
	if(imp_dbh->sql)
	    XPUSHs(sv_2mortal(newSVpv(imp_dbh->sql, 0)));
	else 
	    XPUSHs(&PL_sv_undef);
	    
	XPUSHs(sv_2mortal(newSVpv("server", 0)));

	
	PUTBACK;
	if((count = perl_call_sv(imp_dbh->err_handler, G_SCALAR | G_EVAL)) != 1)
	    croak("An error handler can't return a LIST.");
	SPAGAIN;

	if(SvTRUE(ERRSV)) {
	    POPs;
	    retval = 1;
	} else {
	    retval = POPi;
	}
	
	PUTBACK;
	FREETMPS;
	LEAVE;

	/* If the called sub returns 0 then ignore this error */
	if(retval == 0)
	    return CS_SUCCEED;
    }

    
    if(imp_dbh && srvmsg->msgnumber) {
	if(srvmsg->severity > 10) {
	    sv_setiv(DBIc_ERR(imp_dbh), (IV)srvmsg->msgnumber);

	    imp_dbh->lasterr = srvmsg->msgnumber;
	    imp_dbh->lastsev = srvmsg->severity;
	}

	if(SvOK(DBIc_ERRSTR(imp_dbh))) 
	    sv_catpv(DBIc_ERRSTR(imp_dbh), "Server message ");
	else
	    sv_setpv(DBIc_ERRSTR(imp_dbh), "Server message ");
	sprintf(buff, "number=%ld severity=%ld ",
		srvmsg->msgnumber, srvmsg->severity);
	sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	sprintf(buff, "state=%ld line=%ld",
		srvmsg->state, srvmsg->line);
	sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
	if (srvmsg->svrnlen > 0) {
 	    sv_catpv(DBIc_ERRSTR(imp_dbh), " server=");
 	    sv_catpv(DBIc_ERRSTR(imp_dbh), srvmsg->svrname);
	}
	if (srvmsg->proclen > 0) {
 	    sv_catpv(DBIc_ERRSTR(imp_dbh), " procedure=");
 	    sv_catpv(DBIc_ERRSTR(imp_dbh), srvmsg->proc);
	}
	
 	sv_catpv(DBIc_ERRSTR(imp_dbh), " text=");
 	sv_catpv(DBIc_ERRSTR(imp_dbh), srvmsg->text);
	if(imp_dbh->showSql) {
 	    sv_catpv(DBIc_ERRSTR(imp_dbh), " Statement=");
 	    sv_catpv(DBIc_ERRSTR(imp_dbh), imp_dbh->sql);
 	    sv_catpv(DBIc_ERRSTR(imp_dbh), "\n");
	}
	if (imp_dbh->showEed && srvmsg->status & CS_HASEED) {
	    sv_catpv(DBIc_ERRSTR(imp_dbh), "\n[Start Extended Error]\n");
	    if (ct_con_props(connection, CS_GET, CS_EED_CMD,
			     &cmd, CS_UNUSED, NULL) != CS_SUCCEED)
	    {
		warn("servermsg_cb: ct_con_props(CS_EED_CMD) failed");
		return CS_FAIL;
	    }
	    retcode = fetch_data(imp_dbh, cmd);
	    sv_catpv(DBIc_ERRSTR(imp_dbh), "\n[End Extended Error]\n");
	}
	else
	    retcode = CS_SUCCEED;
	
	return retcode;
    } else {
	if(srvmsg->msgnumber) {
	    PerlIO_printf(DBILOGFP, "Server message: number=%ld severity=%ld ",
		    srvmsg->msgnumber, srvmsg->severity);
	    PerlIO_printf(DBILOGFP, "state=%ld line=%ld ",
		    srvmsg->state, srvmsg->line);
	    if (srvmsg->svrnlen > 0) {
		PerlIO_printf(DBILOGFP, "server=%s ", srvmsg->svrname);
	    }
	    if (srvmsg->proclen > 0) {
		PerlIO_printf(DBILOGFP, "procedure=%s ", srvmsg->proc);
	    }
	    PerlIO_printf(DBILOGFP, "text=%s\n", srvmsg->text);
	} else {
	    warn("%s\n", srvmsg->text);
	}

	PerlIO_flush(DBILOGFP);
    }
	    
    return CS_SUCCEED;
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
get_cwidth(column)
CS_DATAFMT *column;
{
    CS_INT		len;

    switch ((int) column->datatype)
    {
      case CS_CHAR_TYPE:
      case CS_VARCHAR_TYPE:
      case CS_TEXT_TYPE:
      case CS_IMAGE_TYPE:
	len = column->maxlength;
	break;

      case CS_BINARY_TYPE:
      case CS_VARBINARY_TYPE:
      case CS_LONGBINARY_TYPE:
	len = (2 * column->maxlength) + 2;
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
#if defined(CS_DATE_TYPE)
      case CS_DATE_TYPE:
      case CS_TIME_TYPE:
#endif
	len = 30;
	break;
	
      case CS_NUMERIC_TYPE:
      case CS_DECIMAL_TYPE:
	len = (CS_MAX_PREC + 2);
	break;

#ifdef CS_UNIQUE_TYPE
      case CS_UNIQUE_TYPE:
	len = 40;
	break;
#endif
	
      default:
	len = column->maxlength;
	break;
    }
    
    return len;
}

static CS_INT
display_dlen(column)
CS_DATAFMT *column;
{
    CS_INT		len;

    len = get_cwidth(column);

    switch ((int) column->datatype)
    {
      case CS_CHAR_TYPE:
      case CS_VARCHAR_TYPE:
      case CS_TEXT_TYPE:
      case CS_IMAGE_TYPE:
      case CS_BINARY_TYPE:
      case CS_VARBINARY_TYPE:
	len = MIN(len, MAX_CHAR_BUF);
	break;
      default:
	break;
    }
    
    return MAX(strlen(column->name) + 1, len);
}


static CS_RETCODE
display_header(imp_dbh, numcols, columns)
imp_dbh_t      *imp_dbh;
CS_INT		numcols;
CS_DATAFMT	columns[];
{
    CS_INT		i;
    CS_INT		l;
    CS_INT		j;
    CS_INT		disp_len;

    sv_catpv(DBIc_ERRSTR(imp_dbh), "\n");
    for (i = 0; i < numcols; i++)
    {
	disp_len = display_dlen(&columns[i]);
	sv_catpv(DBIc_ERRSTR(imp_dbh), columns[i].name);
	l = disp_len - strlen(columns[i].name);
	for (j = 0; j < l; j++)
	{
	    sv_catpv(DBIc_ERRSTR(imp_dbh), " ");
	}
    }
    sv_catpv(DBIc_ERRSTR(imp_dbh), "\n");
    for (i = 0; i < numcols; i++)
    {
	disp_len = display_dlen(&columns[i]);
	l = disp_len - 1;
	for (j = 0; j < l; j++)
	{
	    sv_catpv(DBIc_ERRSTR(imp_dbh), "-");
	}
	sv_catpv(DBIc_ERRSTR(imp_dbh), " ");
    }
    sv_catpv(DBIc_ERRSTR(imp_dbh), "\n");
    
    return CS_SUCCEED;
}


void syb_init(dbistate)
    dbistate_t *dbistate;
{
    SV 		*sv;
    CS_INT	netio_type = CS_SYNC_IO;
    STRLEN      lna;
    CS_INT      outlen;
    CS_RETCODE  retcode = CS_FAIL;
    CS_INT      cs_ver;
    CS_INT      boolean = CS_FALSE;

    DBIS = dbistate;

#if PERL_VERSION >= 8 && defined(_REENTRANT)
    MUTEX_INIT (context_alloc_mutex);
#endif

#if defined(CS_VERSION_150)
    if(retcode != CS_SUCCEED) {
	cs_ver = CS_VERSION_150;
	retcode = cs_ctx_alloc(cs_ver, &context);
    }
#endif
#if defined(CS_VERSION_125)
    if(retcode != CS_SUCCEED) {
	cs_ver = CS_VERSION_125;
	retcode = cs_ctx_alloc(cs_ver, &context);
    }
#endif
#if defined(CS_VERSION_120)
    if(retcode != CS_SUCCEED) {
	cs_ver = CS_VERSION_120;
	retcode = cs_ctx_alloc(cs_ver, &context);
    }
#endif
#if defined(CS_VERSION_110)
    if(retcode != CS_SUCCEED) {
	cs_ver = CS_VERSION_110;
	retcode = cs_ctx_alloc(cs_ver, &context);
    }
#endif

    if(retcode != CS_SUCCEED) {
	cs_ver = CS_VERSION_100;
	retcode = cs_ctx_alloc(cs_ver, &context);
    }

    if(retcode != CS_SUCCEED)
	croak("DBD::Sybase initialize: cs_ctx_alloc(%d) failed", cs_ver);

#if defined(CS_VERSION_150)
    if(cs_ver == CS_VERSION_150)
	BLK_VERSION = BLK_VERSION_150;
#endif
#if defined(CS_VERSION_125)
    if(cs_ver == CS_VERSION_125)
	BLK_VERSION = BLK_VERSION_125;
#endif
#if defined(CS_VERSION_120)
    if(cs_ver == CS_VERSION_120)
	BLK_VERSION = BLK_VERSION_120;
#endif
#if defined(CS_VERSION_110)
    if(cs_ver == CS_VERSION_110)
	BLK_VERSION = BLK_VERSION_110;
#endif
    if(cs_ver == CS_VERSION_100)
	BLK_VERSION = BLK_VERSION_100;

#if USE_CSLIB_CB
    if (cs_config(context, CS_SET, CS_MESSAGE_CB,
		  (CS_VOID *)cslibmsg_cb, CS_UNUSED, NULL) != CS_SUCCEED) {
	/* Release the context structure.      */

	(void)cs_ctx_drop(context);
	croak("DBD::Sybase initialize: cs_config(CS_MESSAGE_CB) failed");
    }
#else
    if(cs_diag(context, CS_INIT, CS_UNUSED, CS_UNUSED, NULL) != CS_SUCCEED)
	warn("cs_diag(CS_INIT) failed");
#endif

#if defined(CS_EXTERNAL_CONFIG)
    if(cs_config(context, CS_SET, CS_EXTERNAL_CONFIG, &boolean, CS_UNUSED, NULL) != CS_SUCCEED) {
	/* Ignore this error... */
	/* warn("Can't set CS_EXTERNAL_CONFIG to false"); */
    }
#endif

    if((retcode = ct_init(context, cs_ver)) != CS_SUCCEED)
    {
#if 1
	cs_ctx_drop(context);
#endif
	context = NULL;
	croak("DBD::Sybase initialize: ct_init(%d) failed", cs_ver);
    }

    if((retcode = ct_callback(context, NULL, CS_SET, CS_CLIENTMSG_CB,
			  (CS_VOID *)clientmsg_cb)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: ct_callback(clientmsg) failed");
    if((retcode = ct_callback(context, NULL, CS_SET, CS_SERVERMSG_CB,
			      (CS_VOID *)servermsg_cb)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: ct_callback(servermsg) failed");

    if((retcode = ct_config(context, CS_SET, CS_NETIO, &netio_type, 
			    CS_UNUSED, NULL)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: ct_config(netio) failed");

#if defined(MAX_CONNECT)
    netio_type = MAX_CONNECT;
    if((retcode = ct_config(context, CS_SET, CS_MAX_CONNECT, &netio_type, 
			    CS_UNUSED, NULL)) != CS_SUCCEED)
	croak("DBD::Sybase initialize: ct_config(max_connect) failed");
#endif

    {
	char out[1024], *p;
	retcode = ct_config(context, CS_GET, CS_VER_STRING,
			    (CS_VOID*)out, 1024, &outlen);
	if((p = strchr(out, '\n')))
	    *p = 0;
	
	ocVersion = my_strdup(out);
    }

    if((sv = perl_get_sv("0", FALSE)))
    {
	char *p;
	strcpy(scriptName, SvPV(sv, lna));
	if((p = strrchr(scriptName, '/')))
	{
	    char tmp[255];
	    ++p;
	    strncpy(tmp, p, 250);
	    strcpy(scriptName, tmp);
	}
	/* PR 506 */
	if(!strcmp(scriptName, "-e")) {
	    strcpy(scriptName, "perl -e");
	}
    }
    /* PR 506 - get hostname */
    if((sv = perl_get_sv("DBD::Sybase::hostname", FALSE)))
    {
	strcpy(hostname, SvPV(sv, lna));
	/*fprintf(stderr, "Got hostname: %s\n", hostname);*/
    }

    if(DBIS->debug >= 2) {
	char *p = "";
	if((sv = perl_get_sv("DBD::Sybase::VERSION", FALSE)))
	    p = SvPV(sv, lna);
	
	PerlIO_printf(DBILOGFP, "    syb_init() -> DBD::Sybase %s initialized\n", p);
	PerlIO_printf(DBILOGFP, "    OpenClient version: %s\n", ocVersion);
    }

    if ((retcode = cs_loc_alloc( context, &locale )) != CS_SUCCEED) {
	warn("cs_loc_alloc failed");
    }
    if(retcode == CS_SUCCEED) {
	if ((retcode = cs_locale( context, CS_SET, locale, CS_LC_ALL, (CS_CHAR*)NULL,
				  CS_UNUSED, (CS_INT*)NULL)) != CS_SUCCEED) 
	{
	    warn("cs_locale(CS_LC_ALL) failed");
	}
    }

    if(retcode == CS_SUCCEED) {
	CS_INT type = CS_DATES_SHORT;
	if((retcode = cs_dt_info(context, CS_SET, locale, CS_DT_CONVFMT, CS_UNUSED, 
				 (CS_VOID*)&type, CS_SIZEOF(CS_INT), NULL)) != CS_SUCCEED)
	    warn("cs_dt_info() failed");
    }
	
    if(retcode == CS_SUCCEED) {
	if((retcode = cs_config(context, CS_SET, CS_LOC_PROP, locale, 
				CS_UNUSED, NULL)) != CS_SUCCEED) {
		warn("cs_config(CS_LOC_PROP) failed");
	}
    }
}

int 
syb_thread_enabled()
{
    int retcode = 0;

#if PERL_VERSION >= 8 && defined(_REENTRANT) && !defined(NO_THREADS)
    retcode = 1;
#endif

    return retcode;
}

int
syb_set_timeout(timeout)
    int		timeout;
{
    CS_RETCODE retcode;
    if(timeout <= 0)
	timeout = CS_NO_LIMIT;	/* set negative or 0 length timeout to 
				   default no limit */
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_set_timeout() -> ct_config(CS_TIMEOUT,%d)\n", timeout);

#if PERL_VERSION >= 8 && defined(_REENTRANT)
    MUTEX_LOCK (context_alloc_mutex);
#endif
    
    if((retcode = ct_config(context, CS_SET, CS_TIMEOUT, &timeout,
			    CS_UNUSED, NULL)) != CS_SUCCEED)
	warn("ct_config(CS_SET, CS_TIMEOUT) failed");

#if PERL_VERSION >= 8 && defined(_REENTRANT)
    MUTEX_UNLOCK (context_alloc_mutex);
#endif

    return retcode;
}


static int
extractFromDsn(tag, source, dest, size)
    char 	*tag;
    char	*source;
    char	*dest;
    int		size;		/* avoid buffer over-runs */
{
    char *p = strstr(source, tag);
    char *q = dest;
    if(!p)
	return 0;
    p += strlen(tag);
    while(p && *p && *p != ';' && --size)
	*q++ = *p++;
    *q = 0;

    return 1;
}

static int
fetchAttrib(attribs, key)
    SV *attribs;
    char *key;
{
/*    warn("fetching attribs for %s", key); */
    if(attribs) {
	SV **svp;
	if((svp = hv_fetch((HV*)SvRV(attribs), key, strlen(key), 0)) != NULL) {
	    return SvIV(*svp);
	}
    }
    return 0;
}

static SV *
fetchSvAttrib(attribs, key)
    SV *attribs;
    char *key;
{
/*    warn("fetching attribs for %s", key); */
    if(attribs) {
	SV **svp;
	if((svp = hv_fetch((HV*)SvRV(attribs), key, strlen(key), 0)) != NULL) {
	    return newSVsv(*svp);
	}
    }
    return NULL;
}

/* side-effect: sets the BCP related flags in imp_sth */
static void
getBcpAttribs(imp_sth_t *imp_sth, SV *attribs)
{
    SV **svp;
#define BCP_ATTRIB "syb_bcp_attribs"
    if(!attribs || !SvOK(attribs)) {
	return;
    }
    if((svp = hv_fetch((HV*)SvRV(attribs), BCP_ATTRIB, strlen(BCP_ATTRIB), 0)) != NULL) {
	imp_sth->bcpFlag = 1;
	imp_sth->bcpIdentityFlag = fetchAttrib(*svp, "identity_flag");
	imp_sth->bcpIdentityCol  = fetchAttrib(*svp, "identity_column");
    }
}
  
int
syb_db_login(dbh, imp_dbh, dsn, uid, pwd, attribs)
    SV         *dbh;
    struct imp_dbh_st *imp_dbh;
    char       *dsn;
    char       *uid;
    char       *pwd;
    SV         *attribs;
{
    dTHR;
    int retval;

    imp_dbh->server[0]     = 0;
    imp_dbh->charset[0]    = 0;
    imp_dbh->packetSize[0] = 0;
    imp_dbh->language[0]   = 0;
    imp_dbh->ifile[0]      = 0;
    imp_dbh->loginTimeout[0] = 0;
    imp_dbh->timeout[0]    = 0;
    imp_dbh->hostname[0]   = 0;
    imp_dbh->scriptName[0] = 0;
    imp_dbh->database[0]   = 0;
    imp_dbh->curr_db[0]    = 0;
    imp_dbh->encryptPassword[0]   = 0;
    imp_dbh->showSql       = 0;
    imp_dbh->showEed       = 0;
    imp_dbh->flushFinish   = FLUSH_FINISH;
    imp_dbh->doRealTran    = NO_CHAINED_TRAN;	/* default to use chained transaction mode */
    imp_dbh->chainedSupported = 1;
    imp_dbh->quotedIdentifier = 0;
    imp_dbh->rowcount      = 0;
    imp_dbh->doProcStatus  = PROC_STATUS;
    imp_dbh->useBin0x      = 0;
    imp_dbh->binaryImage   = 0;   
    imp_dbh->deadlockRetry = 0;
    imp_dbh->deadlockSleep = 0;
    imp_dbh->deadlockVerbose = 0;
    imp_dbh->nsqlNoStatus  = 0;
    imp_dbh->noChildCon    = 0;
    imp_dbh->failedDbUseFatal    = fetchAttrib(attribs, "syb_failed_db_fatal");
    imp_dbh->bindEmptyStringNull = fetchAttrib(attribs, "syb_bind_empty_string_as_null");
    imp_dbh->err_handler         = fetchSvAttrib(attribs, "syb_err_handler");
    imp_dbh->alwaysForceFailure  = 1;
    imp_dbh->kerberosPrincipal[0] = 0;
    imp_dbh->kerbGetTicket       = fetchSvAttrib(attribs, "syb_kerberos_serverprincipal");
    imp_dbh->host[0]             = 0;
    imp_dbh->port[0]             = 0;

    imp_dbh->blkLogin[0]         = 0;

    imp_dbh->dateFmt             = 0;
    
    if(strchr(dsn, '=')) {
	extractFromDsn("server=", dsn, imp_dbh->server, 64);
	extractFromDsn("charset=", dsn, imp_dbh->charset, 64);
	extractFromDsn("database=", dsn, imp_dbh->database, 36);
	extractFromDsn("packetSize=", dsn, imp_dbh->packetSize, 64);
	extractFromDsn("language=", dsn, imp_dbh->language, 64);
	extractFromDsn("interfaces=", dsn, imp_dbh->ifile, 255);
	extractFromDsn("loginTimeout=", dsn, imp_dbh->loginTimeout, 64);
	extractFromDsn("timeout=", dsn, imp_dbh->timeout, 64);
	extractFromDsn("scriptName=", dsn, imp_dbh->scriptName, 255);
	extractFromDsn("hostname=", dsn, imp_dbh->hostname, 255);
	extractFromDsn("tdsLevel=", dsn, imp_dbh->tdsLevel, 30);
	extractFromDsn("encryptPassword=", dsn, imp_dbh->encryptPassword, 10);
	extractFromDsn("kerberos=", dsn, imp_dbh->kerberosPrincipal, 32);
	extractFromDsn("host=", dsn, imp_dbh->host, 64);
	extractFromDsn("port=", dsn, imp_dbh->port, 20);
	extractFromDsn("maxConnect=", dsn, imp_dbh->maxConnect, 25);
	extractFromDsn("sslCAFile=", dsn, imp_dbh->sslCAFile, 255);
	extractFromDsn("bulkLogin=", dsn, imp_dbh->blkLogin, 10);
    } else {
	strncpy(imp_dbh->server, dsn, 64);
	imp_dbh->server[63] = 0;
    }

    strncpy(imp_dbh->uid, uid, 32);
    imp_dbh->uid[31] = 0;
    strncpy(imp_dbh->pwd, pwd, 32);    
    imp_dbh->pwd[31] = 0;

    sv_setpv(DBIc_ERRSTR(imp_dbh), "");

    if(imp_dbh->kerbGetTicket) {
	fetchKerbTicket(imp_dbh);
    }

#if PERL_VERSION >= 8 && defined(_REENTRANT)
    MUTEX_LOCK(context_alloc_mutex);
#endif

    if((imp_dbh->connection = syb_db_connect(imp_dbh)) == NULL)
	retval = 0;
    else
	retval = 1;

#if PERL_VERSION >= 8 && defined(_REENTRANT)
    MUTEX_UNLOCK(context_alloc_mutex);
#endif

    if(!retval)
	return retval;

    DBIc_IMPSET_on(imp_dbh);	/* imp_dbh set up now		*/
    DBIc_ACTIVE_on(imp_dbh);	/* call disconnect before freeing*/

    DBIc_LongReadLen(imp_dbh) = 32768;

    return 1;
}


static CS_CONNECTION *syb_db_connect(imp_dbh)
    struct imp_dbh_st *imp_dbh;
{
    dTHR;
    CS_RETCODE     retcode;
    CS_CONNECTION *connection = NULL;
    CS_LOCALE     *locale = NULL;
    char ofile[255];
    int len;

    /* Allow increase of the max number of connections - patch supplied by
       Ed Avis */
    if (imp_dbh->maxConnect[0]) {
        /* Maximum number of connections. */
	const char *const s = imp_dbh->maxConnect;
        int i;

        i = atoi(s);
	if (i < 1) {
	    warn("maxConnect must be positive, not '%s'", s);
            return 0;
	}
#if defined(CS_MAX_CONNECT)
        if((retcode = ct_config(context, CS_SET, CS_MAX_CONNECT, (CS_VOID*)&i,
				CS_UNUSED, NULL)) != CS_SUCCEED)
	    croak("ct_config(max_connect) failed");
#else
	warn("ct_config(max_connect) not supported");
#endif
    }
    if(imp_dbh->ifile[0]) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> ct_config(CS_IFILE,%s)\n",
		    imp_dbh->ifile);
	if((retcode = ct_config(context, CS_GET, CS_IFILE, ofile, 255, NULL))
	   != CS_SUCCEED)
	    warn("ct_config(CS_GET, CS_IFILE) failed");
	if(retcode == CS_SUCCEED) {
	    if((retcode = ct_config(context, CS_SET, CS_IFILE, imp_dbh->ifile,
				    CS_NULLTERM, NULL)) != CS_SUCCEED)
	    {
		warn("ct_config(CS_SET, CS_IFILE, %s) failed", imp_dbh->ifile);
		return NULL;
	    }
	}
    }
    if(imp_dbh->loginTimeout[0]) {
	int timeout = atoi(imp_dbh->loginTimeout);
	if(timeout <= 0)
	    timeout = 60;	/* set negative or 0 length timeout to 
				   default 60 seconds */
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> ct_config(CS_LOGIN_TIMEOUT,%d)\n", timeout);
	if((retcode = ct_config(context, CS_SET, CS_LOGIN_TIMEOUT, &timeout,
				CS_UNUSED, NULL)) != CS_SUCCEED)
	    warn("ct_config(CS_SET, CS_LOGIN_TIMEOUT) failed");
    }
    if(imp_dbh->timeout[0]) {
	int timeout = atoi(imp_dbh->timeout);
	if(timeout <= 0)
	    timeout = CS_NO_LIMIT;	/* set negative or 0 length timeout to 
					   default no limit */
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> ct_config(CS_TIMEOUT,%d)\n", timeout);
	if((retcode = ct_config(context, CS_SET, CS_TIMEOUT, &timeout,
				CS_UNUSED, NULL)) != CS_SUCCEED)
	    warn("ct_config(CS_SET, CS_TIMEOUT) failed");
     }

#if 0
    if(imp_dbh->locale == NULL) {
	CS_INT type = CS_DATES_SHORT;

	/* Set up the proper locale - to handle character sets, etc. */
	if ((retcode = cs_loc_alloc( context, &locale ) != CS_SUCCEED)) {
	    warn("cs_loc_alloc failed");
	    return 0;
	}
	if (cs_locale( context, CS_SET, locale, CS_LC_ALL, (CS_CHAR*)NULL,
		       CS_UNUSED, (CS_INT*)NULL) != CS_SUCCEED) 
	{
	    warn("cs_locale(CS_LC_ALL) failed");
	    return 0;
	}
	if(imp_dbh->language[0] != 0) {
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_db_login() -> cs_locale(CS_SYB_LANG,%s)\n", imp_dbh->language);
	    if (cs_locale( context, CS_SET, locale, CS_SYB_LANG, 
			   (CS_CHAR*)imp_dbh->language, CS_NULLTERM, 
			   (CS_INT*)NULL) != CS_SUCCEED)
	    {
		warn("cs_locale(CS_SYB_LANG, %s) failed", imp_dbh->language);
		return 0;
	    }
	}
	if(imp_dbh->charset[0] != 0) {
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_db_login() -> cs_locale(CS_SYB_CHARSET,%s)\n", imp_dbh->charset);
	    if (cs_locale( context, CS_SET, locale, CS_SYB_CHARSET, 
			   (CS_CHAR*)imp_dbh->charset, CS_NULLTERM, 
			   (CS_INT*)NULL) != CS_SUCCEED)
	    {
		warn("cs_locale(CS_SYB_CHARSET, %s) failed", imp_dbh->charset);
		return 0;
	    }
	}

	if(cs_dt_info(context, CS_SET, locale, CS_DT_CONVFMT, CS_UNUSED, 
		      (CS_VOID*)&type, CS_SIZEOF(CS_INT), NULL) != CS_SUCCEED)
	    warn("cs_dt_info() failed");

	imp_dbh->locale = locale;
    }
#else
    if(imp_dbh->language[0] != 0 || imp_dbh->charset[0] != 0) {
	CS_INT type = CS_DATES_SHORT;

	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> using private CS_LOCALE data\n");
	/* Set up the proper locale - to handle character sets, etc. */
	if ((retcode = cs_loc_alloc( context, &imp_dbh->locale ) != CS_SUCCEED)) {
	    warn("cs_loc_alloc failed");
	    return 0;
	}
	if (cs_locale( context, CS_SET, imp_dbh->locale, CS_LC_ALL, (CS_CHAR*)NULL,
		       CS_UNUSED, (CS_INT*)NULL) != CS_SUCCEED) 
	{
	    warn("cs_locale(CS_LC_ALL) failed");
	    return 0;
	}
	if(imp_dbh->language[0] != 0) {
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_db_login() -> cs_locale(CS_SYB_LANG,%s)\n", imp_dbh->language);
	    if (cs_locale( context, CS_SET, imp_dbh->locale, CS_SYB_LANG, 
			   (CS_CHAR*)imp_dbh->language, CS_NULLTERM, 
			   (CS_INT*)NULL) != CS_SUCCEED)
	    {
		warn("cs_locale(CS_SYB_LANG, %s) failed", imp_dbh->language);
		return 0;
	    }
	}
	if(imp_dbh->charset[0] != 0) {
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_db_login() -> cs_locale(CS_SYB_CHARSET,%s)\n", imp_dbh->charset);
	    if (cs_locale( context, CS_SET, imp_dbh->locale, CS_SYB_CHARSET, 
			   (CS_CHAR*)imp_dbh->charset, CS_NULLTERM, 
			   (CS_INT*)NULL) != CS_SUCCEED)
	    {
		warn("cs_locale(CS_SYB_CHARSET, %s) failed", imp_dbh->charset);
		return 0;
	    }
	}

	if(cs_dt_info(context, CS_SET, imp_dbh->locale, CS_DT_CONVFMT, CS_UNUSED, 
		      (CS_VOID*)&type, CS_SIZEOF(CS_INT), NULL) != CS_SUCCEED)
	    warn("cs_dt_info() failed");

    } else {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> using global CS_LOCALE data\n");
    }
#endif

    if((retcode = ct_con_alloc(context, &connection)) != CS_SUCCEED) {
	warn("ct_con_alloc failed");
	return 0;
    }

    if (imp_dbh->locale) {
	if(ct_con_props( connection, CS_SET, CS_LOC_PROP, 
			 (CS_VOID*)imp_dbh->locale,
			 CS_UNUSED, (CS_INT*)NULL) != CS_SUCCEED) {
	    
	    warn("ct_con_props(CS_LOC_PROP) failed");
	    return 0;
	}
    }

    if((retcode = ct_con_props(connection, CS_SET, CS_USERDATA,
			       &imp_dbh, CS_SIZEOF(imp_dbh), NULL)) != CS_SUCCEED)
    {
	warn("ct_con_props(CS_USERDATA) failed");
	return 0;
    }
    if(imp_dbh->tdsLevel[0] != 0) {
	CS_INT value = 0;
	if(strEQ(imp_dbh->tdsLevel, "CS_TDS_40")) 
	    value = CS_TDS_40;
	else if(strEQ(imp_dbh->tdsLevel, "CS_TDS_42"))
	    value = CS_TDS_42;
	else if(strEQ(imp_dbh->tdsLevel, "CS_TDS_46"))
	    value = CS_TDS_46;
	else if(strEQ(imp_dbh->tdsLevel, "CS_TDS_495"))
	    value = CS_TDS_495;
	else if(strEQ(imp_dbh->tdsLevel, "CS_TDS_50"))
	    value = CS_TDS_50;

	if(value) {
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_db_login() -> ct_con_props(CS_TDS_VERSION,%s)\n", imp_dbh->tdsLevel);

	    if (ct_con_props( connection, CS_SET, CS_TDS_VERSION, (CS_VOID*)&value,
			      CS_UNUSED, (CS_INT*)NULL) != CS_SUCCEED) {
		warn("ct_con_props(CS_TDS_VERSION, %s) failed",
		     imp_dbh->tdsLevel);
	    }
	} else {
	    warn("Unkown tdsLevel value %s found", imp_dbh->tdsLevel);
	}
    }

    if (imp_dbh->packetSize[0] != 0) {
	int i = atoi(imp_dbh->packetSize);
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> ct_con_props(CS_PACKETSIZE,%d)\n", i);
	if (ct_con_props( connection, CS_SET, CS_PACKETSIZE, (CS_VOID*)&i,
			  CS_UNUSED, (CS_INT*)NULL) != CS_SUCCEED)
	{
	    warn("ct_con_props(CS_PACKETSIZE, %d) failed", i);
	    return 0;
	}
    }

#if defined(CS_SEC_NETWORKAUTH)
    if(imp_dbh->kerberosPrincipal[0] == 0) {
#endif
	if(retcode == CS_SUCCEED && *imp_dbh->uid) {
	    if((retcode = ct_con_props(connection, CS_SET, CS_USERNAME, 
				       imp_dbh->uid, CS_NULLTERM, NULL)) != CS_SUCCEED)
	    {
		warn("ct_con_props(CS_USERNAME) failed");
		return 0;
	    }
	}
	if(retcode == CS_SUCCEED && *imp_dbh->pwd) {
	    if((retcode = ct_con_props(connection, CS_SET, CS_PASSWORD, 
				       imp_dbh->pwd, CS_NULLTERM, NULL)) != CS_SUCCEED)
	    {
		warn("ct_con_props(CS_PASSWORD) failed");
		return 0;
	    }
	}
#if defined(CS_SEC_NETWORKAUTH)
    } else {
	/*
	** If we're using Kerberos, set the appropriate connection properties
	** (which requires the Sybase Kerberos principal name).  
	*/
	CS_INT i = CS_TRUE;
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> ct_con_props(CS_SERVERPRINCIPAL,%s)\n",
		    imp_dbh->kerberosPrincipal);
	/*warn( imp_dbh->kerberosPrincipal);*/
	if ((retcode = ct_con_props(connection, CS_SET, CS_SEC_NETWORKAUTH,
				    (CS_VOID *) &i, CS_UNUSED, NULL)) != CS_SUCCEED) 
	{
	    warn("ct_con_props(CS_SEC_NETWORKAUTH) failed");
	    return 0;
	}
        
	if ((retcode = ct_con_props(connection, CS_SET, CS_SEC_SERVERPRINCIPAL, 
				    imp_dbh->kerberosPrincipal, CS_NULLTERM, NULL)) != CS_SUCCEED) 
	{
	    warn("ct_con_props(CS_SEC_SERVERPRINCIPAL) failed");
	    return 0;
	}
    }
#endif
    if(retcode == CS_SUCCEED)
    {
	if((retcode = ct_con_props(connection, CS_SET, CS_APPNAME, 
	    *imp_dbh->scriptName ? imp_dbh->scriptName :
				   scriptName, CS_NULLTERM, NULL)) != CS_SUCCEED)
	{
	    warn("ct_con_props(CS_APPNAME, %s) failed", imp_dbh->scriptName);
	    return 0;
	}
	if((retcode = ct_con_props(connection, CS_SET, CS_HOSTNAME, 
		   *imp_dbh->hostname ? imp_dbh->hostname : hostname,
				   CS_NULLTERM, NULL)) != CS_SUCCEED)
	{
	    warn("ct_con_props(CS_HOSTNAME, %s) failed", imp_dbh->hostname);
	    return 0;
	}
    }
    if(retcode == CS_SUCCEED)
    {
	if(imp_dbh->encryptPassword[0] != 0) {
	    int i = CS_TRUE;
	    if((retcode = ct_con_props(connection, CS_SET, CS_SEC_ENCRYPTION, 
				       (CS_VOID*)&i,
				       CS_UNUSED, (CS_INT*)NULL)) != CS_SUCCEED)
	    {
		warn("ct_con_props(CS_SEC_ENCRYPTION, true) failed");
		return 0;
	    }
	}
    }
#if defined(CS_PROP_SSL_CA)
    if(retcode == CS_SUCCEED)
    {
	if(imp_dbh->sslCAFile[0] != 0) {
	    if((retcode = ct_con_props(connection, CS_SET, CS_PROP_SSL_CA, 
				       imp_dbh->sslCAFile,
				       CS_NULLTERM, (CS_INT*)NULL)) != CS_SUCCEED)
	    {
	      warn("ct_con_props(CS_PROP_SSL_CA, %s) failed", imp_dbh->sslCAFile);
		return 0;
	    }
	}
    }
#endif
    
    if (retcode == CS_SUCCEED && imp_dbh->host[0] && imp_dbh->port[0]) {
#if defined(CS_SERVERADDR)
	char buff[255];
	sprintf(buff, "%.64s %.20s", imp_dbh->host, imp_dbh->port);
	if((retcode = ct_con_props(connection, CS_SET, CS_SERVERADDR, 
				   (CS_VOID*)buff,
				   CS_NULLTERM, (CS_INT*)NULL)) != CS_SUCCEED)
	{
	    warn("ct_con_props(CS_SERVERADDR) failed");
	    return 0;
	}
#else
	croak("This version of OpenClient doesn't support CS_SERVERADDR");
#endif
    }

    if (retcode == CS_SUCCEED && imp_dbh->blkLogin[0] != 0) {
        CS_INT flag = CS_TRUE;
	if((retcode = ct_con_props(connection, CS_SET, CS_BULK_LOGIN, 
				   (CS_VOID*)&flag,
				   CS_UNUSED, (CS_INT*)NULL)) != CS_SUCCEED)
	{
	    warn("ct_con_props(CS_BULK_LOGIN) failed");
	    return 0;
	}
    }


    if (retcode == CS_SUCCEED)
    {
	len = *imp_dbh->server == 0 ? 0 : CS_NULLTERM;
	if((retcode = ct_connect(connection, imp_dbh->server, len)) 
	   != CS_SUCCEED) 
	{
	    if(locale != NULL)
		cs_loc_drop(context, locale);
	    ct_con_drop(connection);
	    return 0;
	}
    }
    if(imp_dbh->ifile[0]) {
	if((retcode = ct_config(context, CS_SET, CS_IFILE, ofile, CS_NULLTERM, NULL))
	   != CS_SUCCEED)
	    warn("ct_config(CS_SET, CS_IFILE, %s) failed", ofile);
    }

    if(imp_dbh->database[0] || imp_dbh->curr_db[0]) {
	int ret = syb_db_use(imp_dbh, connection);
	if(imp_dbh->failedDbUseFatal && ret < 0) {
	    /* cleanup, and return NULL */
	    ct_close(connection, CS_FORCE_CLOSE);
	    if(locale != NULL)
		cs_loc_drop(context, locale);
	    ct_con_drop(connection);

	    return 0;
	}
    }

    if(imp_dbh->chainedSupported) {
	CS_BOOL value = CS_FALSE;

	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> checking for chained transactions\n");
	retcode = ct_options(connection, CS_SET, CS_OPT_CHAINXACTS, 
			     &value, CS_UNUSED, NULL);
	if(retcode == CS_FAIL) {
	    imp_dbh->doRealTran = 1;
	    imp_dbh->chainedSupported = 0;
	}
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_login() -> chained transactions are %s supported\n", retcode == CS_FAIL ? "not" : "");
    }

    if(imp_dbh->connection) {
	/* we're setting a sub-connection, so make sure that any attributes
	   such as syb_quoted_identifier and syb_rowcount are set here too */

	if(imp_dbh->quotedIdentifier) {
	    CS_INT value = 1;
	    retcode = ct_options(connection, CS_SET, CS_OPT_QUOTED_IDENT, 
				 &value, CS_UNUSED, NULL);
	    if(retcode != CS_SUCCEED) {
		warn("Setting of CS_OPT_QUOTED_IDENT failed.");
	    }
	}
#if defined(CS_OPT_ROWCOUNT)
	if(imp_dbh->rowcount) {
	    CS_INT value = imp_dbh->rowcount;
	    retcode = ct_options(connection, CS_SET, CS_OPT_ROWCOUNT, 
				 &value, CS_UNUSED, NULL);
	    if(retcode != CS_SUCCEED) {
		warn("Setting of CS_OPT_ROWCOUNT failed.");
	    }
	}
#endif
    }

    return connection;
}

static int
syb_db_use(imp_dbh, connection)
    imp_dbh_t *imp_dbh;
    CS_CONNECTION *connection;
{
    CS_COMMAND *cmd = syb_alloc_cmd(imp_dbh, connection);
    CS_RETCODE ret;
    CS_INT     restype;
    char       statement[255];
    int        retval = 0;
    char       *db;

    if(!cmd)
	return -1;

    if(DBIc_ACTIVE(imp_dbh) && imp_dbh->curr_db[0])
	db = imp_dbh->curr_db;
    else
	db = imp_dbh->database;

    sprintf(statement, "use %s", db);

    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_db_use() -> ct_command(%s)\n", statement);
    ret = ct_command(cmd, CS_LANG_CMD, statement, CS_NULLTERM, CS_UNUSED);
    if(ret != CS_SUCCEED) {
	warn("ct_command failed for '%s'", statement);
	return -1;
    }
    ret = ct_send(cmd);
    if(ret != CS_SUCCEED) {
	warn("ct_send failed for '%s'", statement);
	return -1;
    }
    while((ret = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_use() -> ct_results(%d)\n", 
			  restype);
	if(restype == CS_CMD_FAIL) {
	    warn("DBD::Sybase - can't change context to database %s\n",
		 imp_dbh->database);
	    retval = -1;
	}
    }
    ct_cmd_drop(cmd);

    return retval;
}

int syb_db_date_fmt(dbh, imp_dbh, fmt)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    char *fmt;
{
    CS_INT type;

    if(!strncmp(fmt, "ISO_strict", 10)) {
	imp_dbh->dateFmt = 2;
	return 1;
    }
    if(!strcmp(fmt, "ISO")) {
	imp_dbh->dateFmt = 1;
	return 1;
    }
    
    imp_dbh->dateFmt = 0;

    if(!strcmp(fmt, "LONG")) {
	type = CS_DATES_LONG;
    } else if(!strcmp(fmt, "SHORT")) {
	type = CS_DATES_SHORT;
    } else if(!strcmp(fmt, "DMY4_YYYY")) {
	type = CS_DATES_DMY4_YYYY;
    } else if(!strcmp(fmt, "MDY1_YYYY")) {
	type = CS_DATES_MDY1_YYYY;
    } else if(!strcmp(fmt, "DMY1_YYYY")) {
	type = CS_DATES_DMY1_YYYY;
    } else if(!strcmp(fmt, "DMY2_YYYY")) {
	type = CS_DATES_DMY2_YYYY;
    } else if(!strcmp(fmt, "YMD3_YYYY")) {
	type = CS_DATES_YMD3_YYYY;
    } else if(!strcmp(fmt, "HMS")) {
	type = CS_DATES_HMS;
    } else {
	warn("Invalid format %s in _date_fmt", fmt);
	return 0;
    }
    if(cs_dt_info(context, CS_SET, LOCALE(imp_dbh), CS_DT_CONVFMT, CS_UNUSED, 
		  (CS_VOID*)&type, CS_SIZEOF(CS_INT), NULL) != CS_SUCCEED) {
	warn("cs_dt_info() failed");
	
	return 0;
    }

    return 1;
}

static int syb_get_date_fmt(imp_dbh_t *imp_dbh, char *fmt)
{
    CS_INT type;
    char *p;

    if(imp_dbh->dateFmt == 2) {
	strcpy(fmt, "ISO_strict");
	return 1;
    }
    if(imp_dbh->dateFmt == 1) {
	strcpy(fmt, "ISO");
	return 1;
    }

    if(cs_dt_info(context, CS_GET, LOCALE(imp_dbh), CS_DT_CONVFMT, CS_UNUSED, 
		  (CS_VOID*)&type, CS_SIZEOF(CS_INT), NULL) != CS_SUCCEED) {
	warn("cs_dt_info() failed");
	
	return 0;
    }
    switch(type) {
      case CS_DATES_LONG: p = "LONG";   break;
      case CS_DATES_SHORT: p = "SHORT"; break;
      case CS_DATES_DMY4_YYYY: p = "DMY4_YYYY"; break;
      case CS_DATES_MDY1_YYYY: p = "MDY1_YYYY"; break;
      case CS_DATES_DMY1_YYYY: p = "DMY1_YYYY"; break;
      case CS_DATES_DMY2_YYYY: p = "DMY2_YYYY"; break;
      case CS_DATES_YMD3_YYYY: p = "YMD3_YYYY"; break;
      case CS_DATES_HMS: p = "HMS"; break;
      default: p = "Unknown"; break;
    }
    strcpy(fmt, p);

    return 1;
}


int      syb_discon_all(drh, imp_drh)
    SV *drh;
    imp_drh_t *imp_drh;
{
    /* disconnect_all is not implemented */
    return 1;
}

static int syb_blk_done(imp_sth_t *imp_sth, CS_INT type)
{
    CS_RETCODE ret;

    /* if $dbh->commit is called but no rows have been successfully
       sent to the server then blk_done(CS_BLK_BATCH) fails. Avoid
       the failure by simply not calling blk_done() in that situation. */
    if(type == CS_BLK_BATCH && !imp_sth->bcpRows) {
	return 1;
    }
    ret = blk_done(imp_sth->bcp_desc, type, &imp_sth->numRows);

    /* reset row counter if blk_done was successful */
    if(ret == CS_SUCCEED) {
	if(type == CS_BLK_CANCEL)
	    imp_sth->bcpRows = -1;
	else
	    imp_sth->bcpRows = 0;
    }


    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_blk_done(%d) -> ret = %d, rows = %d\n", type, ret, imp_sth->numRows);
    
    return ret == CS_SUCCEED;
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

    if(imp_dbh->imp_sth && imp_dbh->imp_sth->bcpFlag) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_commit() -> bcp op, calling syb_blk_done()\n");
	return syb_blk_done(imp_dbh->imp_sth, CS_BLK_BATCH);
    }

    if(imp_dbh->doRealTran && !imp_dbh->inTransaction)
	return 1;


    if(DBIc_is(imp_dbh, DBIcf_AutoCommit)) {
	warn("commit ineffective with AutoCommit");
	return 1;
    }

    cmd = syb_alloc_cmd(imp_dbh, imp_dbh->connection);
    if(imp_dbh->doRealTran)
	sprintf(buff, "\nCOMMIT TRAN %s\n", imp_dbh->tranName);
    else
	strcpy(buff, "\nCOMMIT TRAN\n");
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_db_commit() -> ct_command(%s)\n", buff);
    retcode = ct_command(cmd, CS_LANG_CMD, buff,
		     CS_NULLTERM, CS_UNUSED);
    if(retcode != CS_SUCCEED) 
	return 0;

    if(ct_send(cmd) != CS_SUCCEED)
	return 0;

    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_db_commit() -> ct_send() OK\n");

    while((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_commit() -> ct_results(%d) == %d\n",
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

    if(imp_dbh->imp_sth && imp_dbh->imp_sth->bcpFlag) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_rollback() -> bcp op, calling syb_blk_done()\n");
	return syb_blk_done(imp_dbh->imp_sth, CS_BLK_CANCEL);
    }

    if(imp_dbh->doRealTran && !imp_dbh->inTransaction)
	return 1;

    if(DBIc_is(imp_dbh, DBIcf_AutoCommit)) {
	warn("rollback ineffective with AutoCommit");
	return 1;
    }

    cmd = syb_alloc_cmd(imp_dbh, imp_dbh->connection);
    if(imp_dbh->doRealTran)
	sprintf(buff, "\nROLLBACK TRAN %s\n", imp_dbh->tranName);
    else
	strcpy(buff, "\nROLLBACK TRAN\n");
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_db_rollback() -> ct_command(%s)\n", buff);
    retcode = ct_command(cmd, CS_LANG_CMD, buff,
		     CS_NULLTERM, CS_UNUSED);
    if(retcode != CS_SUCCEED) 
	return 0;

    if(ct_send(cmd) != CS_SUCCEED)
	return 0;

    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_db_rollback() -> ct_send() OK\n");

    while((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_rollback() -> ct_results(%d) == %d\n",
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

    cmd = syb_alloc_cmd(imp_dbh, imp_dbh->connection);
    sprintf(imp_dbh->tranName, "DBI%x", imp_dbh);
    sprintf(buff, "\nBEGIN TRAN %s\n", imp_dbh->tranName);
    retcode = ct_command(cmd, CS_LANG_CMD, buff,
			 CS_NULLTERM, CS_UNUSED);
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_db_opentran() -> ct_command(%s) = %d\n", 
		buff, retcode);
    if(retcode != CS_SUCCEED) 
	return 0;
    retcode = ct_send(cmd);
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_db_opentran() -> ct_send() = %d\n",
		retcode);

    if(retcode != CS_SUCCEED)
	return 0;

    while((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_db_opentran() -> ct_results(%d) == %d\n",
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
    dTHR;
    CS_RETCODE retcode;

    /* rollback if we get disconnected and no explicit commit
       has been called (when in non-AutoCommit mode) */
    if(imp_dbh->isDead == 0) { /* only call if connection still active */
	if(!DBIc_is(imp_dbh, DBIcf_AutoCommit))
	    syb_db_rollback(dbh, imp_dbh);
    }

    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_db_disconnect() -> ct_close()\n");
    if((retcode = ct_close(imp_dbh->connection, CS_FORCE_CLOSE)) != CS_SUCCEED)
	PerlIO_printf(DBILOGFP, "    syb_db_disconnect(): ct_close() failed\n");

    if(imp_dbh->locale && 
       (retcode = cs_loc_drop(context, imp_dbh->locale)) != CS_SUCCEED)
	PerlIO_printf(DBILOGFP, "    syb_db_disconnect(): cs_loc_drop() failed\n");
    if((retcode = ct_con_drop(imp_dbh->connection)) != CS_SUCCEED)
	PerlIO_printf(DBILOGFP, "    syb_db_disconnect(): ct_con_drop() failed\n");

    DBIc_ACTIVE_off(imp_dbh);

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


/* NOTE: if you set any new attributes here that need to be passed on
   to Sybase (for example via ct_options()) then make sure that you 
   also code the same thing in syb_db_connect() so that connections
   opened for nested statement handles correctly handle this issue */

int
syb_db_STORE_attrib(dbh, imp_dbh, keysv, valuesv)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    SV *keysv;
    SV *valuesv;
{
    STRLEN kl;
    int on;
    char *key = SvPV(keysv,kl);

    if (kl == 15 && strEQ(key, "syb_chained_txn")) {
	on = SvTRUE(valuesv);
	if(imp_dbh->chainedSupported) {
	    int autocommit = DBIc_is(imp_dbh, DBIcf_AutoCommit);
	    if(!autocommit)
		syb_db_commit(dbh, imp_dbh);
	    if(on) {
		imp_dbh->doRealTran = 0;
	    } else {
		imp_dbh->doRealTran = 1;
	    }
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_db_STORE() -> syb_chained_txn => %d\n", on);
	    if(!autocommit) {
		CS_BOOL    value = on ? CS_TRUE : CS_FALSE;
		CS_RETCODE ret;
		ret = ct_options(imp_dbh->connection, CS_SET,
				 CS_OPT_CHAINXACTS, 
				 &value, CS_UNUSED, NULL);
		if(DBIS->debug >= 2)
		    PerlIO_printf(DBILOGFP, "    syb_db_STORE() -> syb_chained_txn AutoCommit off CS_OPT_CHAINXACTS(%d) => %d\n", value, ret);
	    }

	} else {
	    /* XXX - should this issue a warning???? */
	}

	return TRUE;
    }
    if (kl == 10 && strEQ(key, "AutoCommit")) {
	int ret;

	if (DBIc_ACTIVE_KIDS(imp_dbh)) {
	    croak("panic: can't set AutoCommit with active statement handles");
	}

	on = SvTRUE(valuesv);
	ret = toggle_autocommit(dbh, imp_dbh, on);
	DBIc_set(imp_dbh, DBIcf_AutoCommit, SvTRUE(valuesv));
	return TRUE;
    }
    if(kl == 11 && strEQ(key, "LongTruncOK")) {
	DBIc_set(imp_dbh, DBIcf_LongTruncOk, SvTRUE(valuesv));
	return TRUE;
    }

    if(kl == 11 && strEQ(key, "LongReadLen")) {
	CS_INT value = SvIV(valuesv);
	CS_RETCODE ret;
	ret = ct_options(imp_dbh->connection, CS_SET, CS_OPT_TEXTSIZE, 
			 &value, CS_UNUSED, NULL);
	if(ret != CS_SUCCEED) {
	    warn("Setting of CS_OPT_TEXTSIZE failed.");
	    return FALSE;
	}
	DBIc_LongReadLen(imp_dbh) = value;
	
	return TRUE;
    }

    if(kl == 21 && strEQ(key, "syb_quoted_identifier")) {
	CS_INT value = SvIV(valuesv);
	CS_RETCODE ret;
	ret = ct_options(imp_dbh->connection, CS_SET, CS_OPT_QUOTED_IDENT, 
			 &value, CS_UNUSED, NULL);
	if(ret != CS_SUCCEED) {
	    warn("Setting of CS_OPT_QUOTED_IDENT failed.");
	    return FALSE;
	}
	imp_dbh->quotedIdentifier = value;
	
	return TRUE;
    }

    if (kl == 12 && strEQ(key, "syb_show_sql")) {
	on = SvTRUE(valuesv);
	if(on) {
	    imp_dbh->showSql = 1;
	} else {
	    imp_dbh->showSql = 0;
	}
	return TRUE;
    }
    if (kl == 12 && strEQ(key, "syb_show_eed")) {
	on = SvTRUE(valuesv);
	if(on) {
	    imp_dbh->showEed = 1;
	} else {
	    imp_dbh->showEed = 0;
	}
	return TRUE;
    }
    if (kl == 15 && strEQ(key, "syb_err_handler")) {
	if(valuesv == &PL_sv_undef) {
	    imp_dbh->err_handler = NULL;
	} else if(imp_dbh->err_handler == (SV*)NULL) {
	    imp_dbh->err_handler = newSVsv(valuesv);
	} else {
	    sv_setsv(imp_dbh->err_handler, valuesv);
	}
	return TRUE;
    }
    if (kl == 16 && strEQ(key, "syb_row_callback")) {
	if(valuesv == &PL_sv_undef) {
	    imp_dbh->row_cb = NULL;
	} else if(imp_dbh->row_cb == (SV*)NULL) {
	    imp_dbh->row_cb = newSVsv(valuesv);
	} else {
	    sv_setsv(imp_dbh->row_cb, valuesv);
	}
	return TRUE;
    }
    if (kl == 16 && strEQ(key, "syb_flush_finish")) {
	on = SvTRUE(valuesv);
	if(on) {
	    imp_dbh->flushFinish = 1;
	} else {
	    imp_dbh->flushFinish = 0;
	}
	return TRUE;
    }
    if (kl == 12 && strEQ(key, "syb_rowcount")) {
#if defined(CS_OPT_ROWCOUNT)
	CS_INT value = SvIV(valuesv);
	CS_RETCODE ret;
	ret = ct_options(imp_dbh->connection, CS_SET, CS_OPT_ROWCOUNT, 
			 &value, CS_UNUSED, NULL);
	if(ret != CS_SUCCEED) {
	    warn("Setting of CS_OPT_ROWCOUNT failed.");
	    return FALSE;
	}
	imp_dbh->rowcount = value;
	return TRUE;
#else
	return FALSE;
#endif
    }
    if (kl == 21 && strEQ(key, "syb_dynamic_supported")) {
	warn("'syb_dynamic_supported' is a read-only attribute");
	return TRUE;
    }
    if (kl == 18 && strEQ(key, "syb_do_proc_status")) {
	on = SvTRUE(valuesv);
	if(on) {
	    imp_dbh->doProcStatus = 1;
	} else {
	    imp_dbh->doProcStatus = 0;
	}
	return TRUE;
    }	
    if (kl == 14 && strEQ(key, "syb_use_bin_0x")) {
	on = SvTRUE(valuesv);
	if(on) {
	    imp_dbh->useBin0x = 1;
	} else {
	    imp_dbh->useBin0x = 0;
	}
	return TRUE;
    }
    if (kl == 17 && strEQ(key, "syb_binary_images")) {
	on = SvTRUE(valuesv);
	if(on) {
	    imp_dbh->binaryImage = 1;
	} else {
	    imp_dbh->binaryImage = 0;
	}
	return TRUE;
    }
    if (kl == 18 && strEQ(key, "syb_deadlock_retry")) {
	int value = SvIV(valuesv);
	imp_dbh->deadlockRetry = value;

	return TRUE;
    }
    if (kl == 18 && strEQ(key, "syb_deadlock_sleep")) {
	int value = SvIV(valuesv);
	imp_dbh->deadlockSleep = value;

	return TRUE;
    }
    if (kl == 20 && strEQ(key, "syb_deadlock_verbose")) {
	int value = SvIV(valuesv);
	imp_dbh->deadlockVerbose = value;

	return TRUE;
    }

    if (kl == 17 && strEQ(key, "syb_nsql_nostatus")) {
	int value = SvIV(valuesv);
	imp_dbh->nsqlNoStatus = value;

	return TRUE;
    }

    if (kl == 16 && strEQ(key, "syb_no_child_con")) {
	imp_dbh->noChildCon = SvIV(valuesv);

	return TRUE;
    }
    if (kl == 19 && strEQ(key, "syb_failed_db_fatal")) {
	imp_dbh->failedDbUseFatal = SvIV(valuesv);

	return TRUE;
    }
    if (kl == 29 && strEQ(key, "syb_bind_empty_string_as_null")) {
	imp_dbh->bindEmptyStringNull = SvIV(valuesv);

	return TRUE;
    }

    if (kl == 27 && strEQ(key, "syb_cancel_request_on_error")) {
	imp_dbh->alwaysForceFailure = SvIV(valuesv);

	return TRUE;
    }

    if (kl == 18 && strEQ(key, "syb_server_version")) {
	strncpy(imp_dbh->serverVersion, SvPV(valuesv, na), 15);

	return TRUE;
    }

    if (kl == 12 && strEQ(key, "syb_date_fmt")) {
	syb_db_date_fmt(dbh, imp_dbh, SvPV(valuesv, na));

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
    char *key = SvPV(keysv,kl);
    SV *retsv = NULL;

    if (kl == 10 && strEQ(key, "AutoCommit")) {
	if(DBIc_is(imp_dbh, DBIcf_AutoCommit)) 
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if(kl == 11 && strEQ(key, "LongTruncOK")) {
	if(DBIc_is(imp_dbh, DBIcf_LongTruncOk))
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if (kl == 11 && strEQ(key, "LongReadLen")) {
	retsv = newSViv(DBIc_LongReadLen(imp_dbh));
    }
    if (kl == 12 && strEQ(key, "syb_show_sql")) {
	if(imp_dbh->showSql) 
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if (kl == 12 && strEQ(key, "syb_show_eed")) {
	if(imp_dbh->showEed) 
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if (kl == 8 && strEQ(key, "syb_dead")) {
	if(imp_dbh->isDead)
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if (kl == 15 && strEQ(key, "syb_err_handler")) {
	if(imp_dbh->err_handler) {
	    retsv = newSVsv(imp_dbh->err_handler);
	} else {
	    retsv = &PL_sv_undef;
	}
    }
    if (kl == 16 && strEQ(key, "syb_row_callback")) {
	if(imp_dbh->row_cb) {
	    retsv = newSVsv(imp_dbh->row_cb);
	} else {
	    retsv = &PL_sv_undef;
	}
    }
    if (kl == 15 && strEQ(key, "syb_chained_txn")) {
	if(imp_dbh->doRealTran)
	    retsv = newSViv(0);
	else
	    retsv = newSViv(1);
    }
    if (kl == 18 && strEQ(key, "syb_check_tranmode")) {
	CS_INT value;
	CS_RETCODE ret;
	ret = ct_options(imp_dbh->connection, CS_GET, CS_OPT_CHAINXACTS, 
			 &value, CS_UNUSED, NULL);
	if(ret != CS_SUCCEED)
	    value = 0;
	retsv = newSViv(value);
    }
    if (kl == 16 && strEQ(key, "syb_flush_finish")) {
	if(imp_dbh->flushFinish)
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if (kl == 21 && strEQ(key, "syb_dynamic_supported")) {
	CS_BOOL val;
	CS_RETCODE ret = ct_capability(imp_dbh->connection, CS_GET, CS_CAP_REQUEST,
			    CS_REQ_DYN, (CS_VOID*)&val);
	if(ret != CS_SUCCEED || val == CS_FALSE)
	    retsv = newSViv(0);
	else
	    retsv = newSViv(1);
    }

    if(kl == 21 && strEQ(key, "syb_quoted_identifier")) {
	if(imp_dbh->quotedIdentifier)
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if(kl == 12 && strEQ(key, "syb_rowcount")) {
	retsv = newSViv(imp_dbh->rowcount);
    }

    if(kl == 14 && strEQ(key, "syb_oc_version")) {
	retsv = newSVpv(ocVersion, strlen(ocVersion));
    }
    if (kl == 18 && strEQ(key, "syb_do_proc_status")) {
	retsv = newSViv(imp_dbh->doProcStatus);
    }	
    if (kl == 14 && strEQ(key, "syb_use_bin_0x")) {
	if(imp_dbh->useBin0x) 
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if (kl == 17 && strEQ(key, "syb_binary_images")) {
	if(imp_dbh->binaryImage) 
	    retsv = newSViv(1);
	else
	    retsv = newSViv(0);
    }
    if (kl == 18 && strEQ(key, "syb_deadlock_retry")) {
	retsv = newSViv(imp_dbh->deadlockRetry);
    }
    if (kl == 18 && strEQ(key, "syb_deadlock_sleep")) {
	retsv = newSViv(imp_dbh->deadlockSleep);
    }
    if (kl == 20 && strEQ(key, "syb_deadlock_verbose")) {
	retsv = newSViv(imp_dbh->deadlockVerbose);
    }
    if (kl == 17 && strEQ(key, "syb_nsql_nostatus")) {
	retsv = newSViv(imp_dbh->nsqlNoStatus);
    }

    if (kl == 16 && strEQ(key, "syb_no_child_con")) {
	retsv = newSViv(imp_dbh->noChildCon);
    }
    if (kl == 19 && strEQ(key, "syb_failed_db_fatal")) {
	retsv = newSViv(imp_dbh->failedDbUseFatal);
    }
    if (kl == 29 && strEQ(key, "syb_bind_empty_string_as_null")) {
	retsv = newSViv(imp_dbh->bindEmptyStringNull);
    }
    if (kl == 27 && strEQ(key, "syb_cancel_request_on_error")) {
	retsv = newSViv(imp_dbh->alwaysForceFailure);
    }
    if (kl == 18 && strEQ(key, "syb_server_version")) {
	retsv = newSVpv(imp_dbh->serverVersion, 0);
    }

    if (kl == 12 && strEQ(key, "syb_date_fmt")) {
	char buff[50];
	syb_get_date_fmt(imp_dbh, buff);
	retsv = newSVpv(buff, 0);
    }

    if (retsv == &sv_yes || retsv == &sv_no || retsv == &PL_sv_undef)
	return retsv;

    return sv_2mortal(retsv);
}

static CS_COMMAND *
syb_alloc_cmd(imp_dbh, connection) 
    imp_dbh_t *imp_dbh;
    CS_CONNECTION *connection;
{
    CS_COMMAND *cmd;
    CS_RETCODE retcode;

    if((retcode = ct_cmd_alloc(connection, &cmd)) != CS_SUCCEED) {
	syb_set_error(imp_dbh, -1, "ct_cmd_alloc failed");
	return NULL;
    }
    if(DBIS->debug >= 3)
	PerlIO_printf(DBILOGFP, "    syb_alloc_cmd() -> CS_COMMAND %x for CS_CONNECTION %x\n", cmd, connection);

    return cmd;
}

static void
dbd_preparse(imp_sth, statement)
    imp_sth_t *imp_sth;
    char *statement;
{
    enum { DEFAULT, LITERAL, COMMENT, LINE_COMMENT, VARIABLE } STATES;
    int state = DEFAULT;
    int next_state;
    char last_literal = 0;
    char *src, *start, *dest;
    phs_t phs_tpl;
    SV *phs_sv;
    int idx=0;
    STRLEN namelen;
    char varname[34];
    int pos;

    /* allocate room for copy of statement with spare capacity	*/
    imp_sth->statement = (char*)safemalloc(strlen(statement) * 3);

    /* initialise phs ready to be cloned per placeholder	*/
    memset(&phs_tpl, 0, sizeof(phs_tpl));
    phs_tpl.ftype = CS_VARCHAR_TYPE;
    varname[0] = 0;

    /* check for a leading EXEC. If it is present then set imp_sth->type
       to 1 to indicate that we are doing an RPC call.
    */

    src  = statement;
    while(isspace(*src) && *src) /* skip over leading whitespace */
	++src;
    if(!strncasecmp(src, "exec", 4))
	imp_sth->type = 1;
    else if(imp_sth->bcpFlag) 
	imp_sth->type = 2;
    else
	imp_sth->type = 0;

    src  = statement;
    dest = imp_sth->statement;
    while(*src) {
	next_state = state;	/* default situation */
	switch(state) {
	  case DEFAULT:
	    if(*src == '\'' || *src == '"') {
		last_literal = *src;
		next_state = LITERAL;
	    } else if(*src == '/' && *(src+1) == '*') {
		next_state = COMMENT;
	    } else if(*src == '-' && *(src+1) == '-') {
		next_state = LINE_COMMENT;
	    } else if(*src == '@') {
		varname[0] = '@';
		pos = 1;
		next_state = VARIABLE;
	    }
	    break;
	  case LITERAL:
	    if(*src == last_literal) {
		next_state = DEFAULT;
	    }
	    break;
	  case COMMENT:
	    if(*(src-1) == '*' && *src == '/') {
		next_state = DEFAULT;
	    }
	    break;
	  case LINE_COMMENT:
	    if(*src == '\n') {
		next_state = DEFAULT;
	    }
	    break;
	  case VARIABLE:
	    if(!isalnum(*src) && *src != '_') {
		varname[pos] = 0;
		next_state = DEFAULT;
	    } else if(pos < 33) {
		varname[pos++] = *src;
	    }
	}
/*	printf("state = %d, *src = %c, next_state = %d\n", state, *src, next_state); */

	if(state != DEFAULT || *src != '?') {
	    *dest++ = *src++;
	    state = next_state;
	    continue;
	}
	state = next_state;
	start = dest;			/* save name inc colon	*/ 
	*dest++ = *src++;
	if (*start == '?') {		/* X/Open standard	*/
	    sprintf(start,":p%d", ++idx); /* '?' -> ':p1' (etc)	*/
	    dest = start+strlen(start);
	} else {			/* not a placeholder, so just copy */
	    continue;
	}
	*dest = '\0';			/* handy for debugging	*/
	namelen = (dest-start);
	if (imp_sth->all_params_hv == NULL)
	    imp_sth->all_params_hv = newHV();
	phs_tpl.sv = &PL_sv_undef;
	phs_sv = newSVpv((char*)&phs_tpl, sizeof(phs_tpl)+namelen+1);
	hv_store(imp_sth->all_params_hv, start, namelen, phs_sv, 0);
	strcpy( ((phs_t*)(void*)SvPVX(phs_sv))->name, start);
	strcpy( ((phs_t*)(void*)SvPVX(phs_sv))->varname, varname);
	if(imp_sth->type == 1) { /* if it's an EXEC call, check for OUTPUT */
	    char *p = src;
	    do {
		if(*p == ',')
		    break;
		if(isspace(*p))
		    continue;
		if(isalpha(*p)) {
		    if(!strncasecmp(p, "out", 3)) {
			((phs_t*)(void*)SvPVX(phs_sv))->is_inout = 1;
		    }
		}
	    } while(*(++p));
	}
	if (DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    dbd_preparse parameter %s (%s)\n",
			  ((phs_t*)(void*)SvPVX(phs_sv))->name,
			  ((phs_t*)(void*)SvPVX(phs_sv))->varname);
	/* warn("params_hv: '%s'\n", start);	*/
    }
    *dest = '\0';
    if (imp_sth->all_params_hv) {
	DBIc_NUM_PARAMS(imp_sth) = (int)HvKEYS(imp_sth->all_params_hv);
	if (DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    dbd_preparse scanned %d distinct placeholders\n",
		(int)DBIc_NUM_PARAMS(imp_sth));
    }
}


static CS_RETCODE 
dyn_prepare(imp_dbh_t *imp_dbh, imp_sth_t *imp_sth, char* statement)
{
    CS_INT restype;
    static int tt = 1; 
    int failed = 0;
    CS_BOOL val;
    CS_RETCODE ret;
    
    ret = ct_capability(imp_dbh->connection, CS_GET, CS_CAP_REQUEST,
			CS_REQ_DYN, (CS_VOID*)&val);
    if(ret != CS_SUCCEED || val == CS_FALSE)
	croak("Panic: dynamic SQL (? placeholders) are not supported by the server you are connecting to");
	    
    sprintf(imp_sth->dyn_id, "DBD%d", (int)tt++);
    
    if (DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, 
		      "    dyn_prepare: ct_dynamic(CS_PREPARE) for %s\n",
		      imp_sth->dyn_id);
    
    imp_sth->dyn_execed = 0;
    
    imp_sth->cmd = syb_alloc_cmd(imp_dbh, imp_sth->connection ? 
				 imp_sth->connection :
				 imp_dbh->connection);
    
    ret = ct_dynamic(imp_sth->cmd, CS_PREPARE, imp_sth->dyn_id,
		     CS_NULLTERM, statement, CS_NULLTERM);
    if(ret != CS_SUCCEED) {
	warn("ct_dynamic(CS_PREPARE) returned %d", ret);
	return ret;
    }
    ret = ct_send(imp_sth->cmd);
    if(ret != CS_SUCCEED) {
	warn("ct_send(ct_dynamic(CS_PREPARE)) returned %d", ret);
	return ret;
    }
    while((ret = ct_results(imp_sth->cmd, &restype)) == CS_SUCCEED)
	if(restype == CS_CMD_FAIL)
	    failed = 1;
    
    if(ret == CS_FAIL || failed) {
	warn("ct_result(ct_dynamic(CS_PREPARE)) returned %d", ret);
	return ret;
    }
    ret = ct_dynamic(imp_sth->cmd, CS_DESCRIBE_INPUT, imp_sth->dyn_id,
		     CS_NULLTERM, NULL, CS_UNUSED);
    if(ret != CS_SUCCEED)
	warn("ct_dynamic(CS_DESCRIBE_INPUT) returned %d", ret);
    ret = ct_send(imp_sth->cmd);
    if(ret != CS_SUCCEED)
	warn("ct_send(CS_DESCRIBE_INPUT) returned %d", ret);
    if (DBIS->debug >= 3)
	PerlIO_printf(DBILOGFP, 
		      "    dyn_prepare: ct_dynamic(CS_DESCRIBE_INPUT) for %s\n",
		      imp_sth->dyn_id);
    while((ret = ct_results(imp_sth->cmd, &restype)) == CS_SUCCEED) {
	if (DBIS->debug >= 3)
	    PerlIO_printf(DBILOGFP, 
			  "    dyn_prepare: ct_results(CS_DESCRIBE_INPUT) for %s - restype %d\n",
			  imp_sth->dyn_id, restype);
	if(restype == CS_DESCRIBE_RESULT) {
	    CS_INT num_param, outlen;
	    int i;
	    char name[50];
	    SV **svp;
	    phs_t *phs;
	    int ret;
	    
	    ret = ct_res_info(imp_sth->cmd, CS_NUMDATA, &num_param, 
			      CS_UNUSED,
			      &outlen);
	    if(ret != CS_SUCCEED)
		warn("ct_res_info(CS_DESCRIBE_INPUT) returned %d",
		     ret);
	    if (DBIS->debug >= 3)
		PerlIO_printf(DBILOGFP, "    dyn_prepare: ct_res_info(CS_DESCRIBE_INPUT) statement has %d parameters\n", num_param);
	    for(i = 1; i <= num_param; ++i) {
		sprintf(name, ":p%d", i);
		svp = hv_fetch(imp_sth->all_params_hv, name, strlen(name), 0);
		phs = ((phs_t*)(void*)SvPVX(*svp));
		ct_describe(imp_sth->cmd, i, &phs->datafmt);
		if (DBIS->debug >= 3)
		    PerlIO_printf(DBILOGFP, "    dyn_prepare: ct_describe(CS_DESCRIBE_INPUT) col %d, type %d, status %d, length %d\n",
				  i, phs->datafmt.datatype, 
				  phs->datafmt.status, phs->datafmt.maxlength);
	    }
	}
    }
    if(ct_dynamic(imp_sth->cmd, CS_EXECUTE, imp_sth->dyn_id, CS_NULLTERM,
		  NULL, CS_UNUSED) != CS_SUCCEED)
	ret = CS_FAIL;
    else {
	ret = CS_SUCCEED;
	imp_sth->dyn_execed = 1;
    }

    return ret;
}

int      
syb_st_prepare(sth, imp_sth, statement, attribs)
    SV *sth;
    imp_sth_t *imp_sth;
    char *statement;
    SV *attribs;
{
    dTHR;
    D_imp_dbh_from_sth;
    CS_RETCODE ret;

/*    PerlIO_printf(DBILOGFP, "st_prepare on %x\n", imp_sth); */

    sv_setpv(DBIc_ERRSTR(imp_dbh), "");

    /* Don't try to initiate a new command if the connection isn't active! */
    if (!DBIc_ACTIVE(imp_dbh)) {
	syb_set_error(imp_dbh, -1, "Database disconnected");
        return 0;
    }

    /* Check to see if the syb_bcp_attribs flag is set */
    getBcpAttribs(imp_sth, attribs);

    if(DBIc_ACTIVE_KIDS(DBIc_PARENT_COM(imp_sth))) {
	int retval = 1;

	if(imp_dbh->noChildCon) { /* inhibit child connections to be created */
	    syb_set_error(imp_dbh, -1, "DBD::Sybase error: Can't create child connections when syb_no_chld_con is set");
	    return 0;
	}
	if(!DBIc_is(imp_dbh, DBIcf_AutoCommit))
	    croak("Panic: Can't have multiple statement handles on a single database handle when AutoCommit is OFF");
	if (DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_st_prepare() parent has active kids - opening new connection\n");


#if PERL_VERSION >= 8 && defined(_REENTRANT)
	MUTEX_LOCK(context_alloc_mutex);
#endif
	if((imp_sth->connection = syb_db_connect(imp_dbh)) == NULL)
	    retval = 0;

#if PERL_VERSION >= 8 && defined(_REENTRANT)
	MUTEX_UNLOCK(context_alloc_mutex);
#endif

	if(!retval)
	    return retval;
    }

#if 0
    if(imp_dbh->sql != NULL)
        safefree(imp_dbh->sql);
    imp_dbh->sql = (char*)safemalloc(strlen(statement)+1);
    strcpy(imp_dbh->sql,statement);
#endif

    if(imp_sth->statement != NULL)
	safefree(imp_sth->statement);
    imp_sth->statement = NULL;
    dbd_preparse(imp_sth, statement);
    imp_dbh->sql = imp_sth->statement;
	
    if((int)DBIc_NUM_PARAMS(imp_sth)) {
	/* regular dynamic sql */
	if(imp_sth->type == 0) {
	    ret = dyn_prepare(imp_dbh, imp_sth, statement);
	    if(ret != CS_SUCCEED) {
		return 0;
	    }
	} else if(imp_sth->type == 1) {
	    /* RPC call - get the proc name */
	    /* We could possibly get the proc params from syscolumns, but
	        there are a lot of issues with that which will break it */
	    if(!syb_st_describe_proc(imp_sth, statement)) {
		croak("DBD::Sybase: describe_proc failed!\n");
	    }
	    if (DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    describe_proc: procname = %s\n", 
			imp_sth->proc);

	    imp_sth->cmd = syb_alloc_cmd(imp_dbh, imp_sth->connection ? 
					 imp_sth->connection :
					 imp_dbh->connection);
	    ret = CS_SUCCEED;
	    imp_sth->dyn_execed = 0;
	} else {
	    /* BLK operation! */
	    ret = syb_blk_init(imp_dbh, imp_sth);
	}
    } else {
	/* If this is a blk request (i.e. the syb_bcp_attribs hash is set
	   in the prepare() call, then force a failure, because no 
	   parameters (placeholders) have been defined. */
	if(imp_sth->type == 2) {
	    syb_set_error(imp_dbh, -1, "The syb_bcp_attribs attribute is set, but no placeholders found in the query");
	    return 0;
	}
	    
	/* delay the ct_command() to the syb_st_execute() call */
	imp_sth->cmd = NULL;
	ret = CS_SUCCEED;
    }    

    if(ret != CS_SUCCEED) 
	return 0;

    imp_sth->doProcStatus = imp_dbh->doProcStatus;

    DBIc_on(imp_sth, DBIcf_IMPSET);
/*    DBIc_ACTIVE_on(imp_sth); */

    return 1;
}

static int syb_st_describe_proc(imp_sth, statement)
    imp_sth_t *imp_sth;
    char *statement;
{ 
    char *buff = my_strdup(statement);
    char *tok;

    tok = strtok(buff, " \n\t");
    if(strncasecmp(tok, "exec", 4)) {
	safefree(buff);
	return 0;		/* it's gotta start with exec(ute) */
    }
    tok = strtok(NULL, " \n\t"); /* this is the proc name */
    if(!tok || !*tok) {
	warn("DBD::Sybase: describe_proc: didn't get a proc name in EXEC statement\n");
	safefree(buff);
	return 0;
    }
    strcpy(imp_sth->proc, tok);
    safefree(buff);
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
	   imp_sth->coldata[i].type == CS_TEXT_TYPE ||
	   imp_sth->coldata[i].type == CS_IMAGE_TYPE)
	    Safefree(imp_sth->coldata[i].value.c);
    
    if(imp_sth->datafmt)
	Safefree(imp_sth->datafmt);
    if(imp_sth->coldata)
	Safefree(imp_sth->coldata);
    imp_sth->numCols = 0;
    imp_sth->coldata = NULL;
    imp_sth->datafmt = NULL;

    /* clear any cached data in the handle */
    
}

static CS_RETCODE
describe(imp_sth, restype)
    imp_sth_t *imp_sth;
    int restype;
{
    D_imp_dbh_from_sth;
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
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    ct_res_info() returns %d columns\n", numCols);

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

	if(DBIS->debug >= 3)
	    PerlIO_printf(DBILOGFP, "    ct_describe(%d): type = %d, maxlen = %d\n",
		    i, imp_sth->datafmt[i].datatype,
		    imp_sth->datafmt[i].maxlength);

	imp_sth->coldata[i].realType = imp_sth->datafmt[i].datatype;
	imp_sth->coldata[i].realLength = imp_sth->datafmt[i].maxlength;

	imp_sth->datafmt[i].locale = LOCALE(imp_dbh);

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
	    
	  case CS_MONEY_TYPE:
	  case CS_MONEY4_TYPE:
	  case CS_REAL_TYPE:
	  case CS_FLOAT_TYPE:
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
	    if(imp_dbh->binaryImage)
		imp_sth->coldata[i].type     = imp_sth->datafmt[i].datatype;
	    else {
		imp_sth->coldata[i].type     = CS_TEXT_TYPE;
		imp_sth->datafmt[i].datatype = CS_TEXT_TYPE;
	    }
	    if(!imp_sth->noBindBlob) {
		retcode = ct_bind(imp_sth->cmd, (i + 1), &imp_sth->datafmt[i],
				  imp_sth->coldata[i].value.c,
				  &imp_sth->coldata[i].valuelen,
				  &imp_sth->coldata[i].indicator);
	    }
	    break;
	    
	  case CS_DATETIME_TYPE:
	  case CS_DATETIME4_TYPE:
#if defined(CS_DATE_TYPE)
	  case CS_DATE_TYPE:
	  case CS_TIME_TYPE:
#endif
	    imp_sth->datafmt[i].maxlength = sizeof(CS_DATETIME);
	    imp_sth->datafmt[i].format    = CS_FMT_UNUSED;
	    imp_sth->coldata[i].type      = CS_DATETIME_TYPE;
	    imp_sth->datafmt[i].datatype  = CS_DATETIME_TYPE;
	    retcode = ct_bind(imp_sth->cmd, (i + 1), &imp_sth->datafmt[i],
			      &imp_sth->coldata[i].value.dt,
			      &imp_sth->coldata[i].valuelen,
			      &imp_sth->coldata[i].indicator);
	    break;


	  case CS_CHAR_TYPE:
	  case CS_VARCHAR_TYPE:
	  case CS_BINARY_TYPE:
	  case CS_VARBINARY_TYPE:
	  case CS_NUMERIC_TYPE:
	  case CS_DECIMAL_TYPE:
	  default:
	    imp_sth->datafmt[i].maxlength = 
		get_cwidth(&imp_sth->datafmt[i]) + 1;
	    /*display_dlen(&imp_sth->datafmt[i]) + 1;*/
	    imp_sth->datafmt[i].format   = CS_FMT_UNUSED;
	    New(902, imp_sth->coldata[i].value.c,
		imp_sth->datafmt[i].maxlength, char);
	    imp_sth->coldata[i].type      = CS_CHAR_TYPE;
	    imp_sth->datafmt[i].datatype  = CS_CHAR_TYPE;
	    retcode = ct_bind(imp_sth->cmd, (i + 1), &imp_sth->datafmt[i],
			      imp_sth->coldata[i].value.c,
			      &imp_sth->coldata[i].valuelen,
			      &imp_sth->coldata[i].indicator);
           /* Now that we've accomplished the CHAR actions, set the type back
              to BINARY if appropriate, so the useBin0x actions work later. */
	    if (imp_sth->coldata[i].realType == CS_BINARY_TYPE ||
		imp_sth->coldata[i].realType == CS_VARBINARY_TYPE) {
		imp_sth->coldata[i].type = imp_sth->datafmt[i].datatype =
		    imp_sth->coldata[i].realType;
	    }
	    break;
	}	
	/* check the return code of the call to ct_bind in the
	   switch above: */
	if (retcode != CS_SUCCEED) {
	    warn("ct_bind() failed");
	    cleanUp(imp_sth);
	    break;
	}
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    describe() -> col %d, type %d, realtype %d\n", i, imp_sth->coldata[i].type, imp_sth->coldata[i].realType);
	
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
    D_imp_dbh_from_sth;
    CS_COMMAND *cmd = imp_sth->cmd;
    CS_INT      restype;
    CS_RETCODE  retcode;
    int         failFlag = 0;

    imp_sth->numRows = -1;

    while((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    st_next_result() -> ct_results(%d) == %d\n",
		    restype, retcode);

	if(restype == CS_CMD_FAIL)
	    failFlag = 1;
	if((restype == CS_CMD_DONE || restype == CS_CMD_SUCCEED) 
	   && !failFlag) 
	{
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
		  HV *hv = (HV*)SvRV(sth);
		  HE *he;

		  cleanUp(imp_sth);
		  
		  hv_iterinit(hv);
		  while((he = hv_iternext(hv))) {
		      I32 i;
		      char *key = hv_iterkey(he, &i);
		      SV *sv = hv_iterval(hv, he);

		      if(strnEQ(key, "NAME", 4)) {
			  if(DBIS->debug >= 3)
			      PerlIO_printf(DBILOGFP, 
			       "    st_next_result() -> delete key %s (%s) from cache\n",
					    key, neatsvpv(sv, 0));

			  hv_delete(hv, key, i, G_DISCARD);
		      }
		  }
	      }
	      retcode = describe(imp_sth, restype);
	      if(DBIS->debug >= 2)
		  PerlIO_printf(DBILOGFP, "describe() retcode = %d\n", retcode);

	      if(restype == CS_STATUS_RESULT && 
		 (imp_sth->doProcStatus ||
		  (imp_sth->dyn_execed && imp_sth->type == 0))) 
	      {
		  CS_INT rows_read;
		  retcode = ct_fetch(cmd, CS_UNUSED, CS_UNUSED, CS_UNUSED, &rows_read);
		  if(retcode == CS_SUCCEED) {
		      imp_sth->lastProcStatus = imp_sth->coldata[0].value.i;
		      if(DBIS->debug >= 2)
			  PerlIO_printf(DBILOGFP, "describe() proc status code = %d\n", imp_sth->lastProcStatus);
		      if(imp_sth->lastProcStatus != 0) {
			  failFlag = 2;
		      }
		  } else {
		      croak("ct_fetch() for proc status failed!");
		  }
		  while((retcode = ct_fetch(cmd, CS_UNUSED, CS_UNUSED, CS_UNUSED, &rows_read))) {
		      if(retcode == CS_END_DATA || retcode == CS_FAIL)
			  break;
		  }
	      } else
		  goto Done;	/* exit from the ct_results() loop here if we
				   are *NOT* in doProcStatus mode, and this is
				   *NOT* a status result set */
	}
    }
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "ct_results(%d) final retcode = %d\n", 
		      restype, retcode);
  Done:

    /* The lasterr/lastsev is a hack to work around Sybase OpenClient, which
       does NOT return CS_CMD_FAIL for constraint errors when
       inserting/updating data using ?-style placeholders. */

    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    st_next_result() -> lasterr = %d, lastsev = %d\n", imp_dbh->lasterr, imp_dbh->lastsev);

    /* Only force a failure if there are no rows to be fetched (ie on a
       normal insert/update/delete operation */
    if(!failFlag && imp_dbh->lasterr != 0 && imp_dbh->lastsev > 10) {
	if(imp_dbh->alwaysForceFailure ||
	   (restype != CS_STATUS_RESULT &&
	    restype != CS_ROW_RESULT    &&
	    restype != CS_PARAM_RESULT  &&
	    restype != CS_CURSOR_RESULT &&
	    restype != CS_COMPUTE_RESULT )) {

	    failFlag = 3;
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    st_next_result() -> restype is not data result or syb_cancel_request_on_error is TRUE, force failFlag\n");
	} else {
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    st_next_result() -> restype is data result, do NOT force failFlag\n");
	}
    }

    /* Cancel the whole thing if we force a failure */
    /* Blaise Lepeuple, 9/26/02 */
    /* Only do the flush if the failure was forced rather than "normal".
       In the normal case the connection is in a stable/idle state */
    /* XXX */
    if(failFlag && (restype != CS_CMD_DONE && restype != CS_CMD_FAIL) && retcode != CS_FAIL) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    st_next_result() -> failFlag set - clear request\n");
	syb_st_finish(sth, imp_sth);
    }

    /* FreeTDS added a result code CS_END_RESULTS */
    /* Do the right thing with it Frederick Staats, 6/26/03 */
    if(retcode == CS_END_RESULTS)
	restype = CS_CMD_DONE;

    if(failFlag || retcode == CS_FAIL || retcode == CS_CANCELED) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    st_next_result() -> force CS_CMD_FAIL return\n");
	restype = CS_CMD_FAIL;
    }

    imp_sth->lastResType = restype;

    /* clear the handle here - to be sure to always have a consistent 
       handle view after command completion. */
    if(restype == CS_CMD_DONE || restype == CS_CMD_FAIL) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, 
			  "    st_next_result() -> got %s: resetting ACTIVE, moreResults, dyn_execed, exec_done\n",
			  restype == CS_CMD_DONE ? "CS_CMD_DONE" : "CS_CMD_FAIL");
	imp_sth->moreResults = 0;
	imp_sth->dyn_execed  = 0;
	imp_sth->exec_done   = 0;
	DBIc_ACTIVE_off(imp_sth);
    } else {
	DBIc_ACTIVE_on(imp_sth);
    }
  
    return restype;
}

static int
_convert(void *ptr, char *str, CS_LOCALE *locale, CS_DATAFMT *datafmt, 
	 CS_INT *len)
{
  CS_DATAFMT srcfmt;
  CS_INT retcode;
  CS_INT reslen;

  memset(&srcfmt, 0, sizeof(srcfmt));
  srcfmt.datatype  = CS_CHAR_TYPE;
  srcfmt.maxlength = strlen(str);
  srcfmt.format    = CS_FMT_NULLTERM;
  srcfmt.locale    = locale;
  
  retcode = cs_convert(context, &srcfmt, str, datafmt,
		       ptr, &reslen);

  if(DBIS->debug >= 2
     && retcode != CS_SUCCEED || reslen == CS_UNUSED)
      PerlIO_printf(DBILOGFP, "cs_convert failed (_convert(%s, %d))", str, datafmt->datatype);

  if(len) {
    *len = reslen;
  }

  return retcode;
}

static CS_RETCODE
    get_cs_msg(CS_CONTEXT *context, CS_CONNECTION *connection, char *msg, 
	       SV *sth, imp_sth_t *imp_sth)
{
    CS_CLIENTMSG errmsg;
    CS_INT lastmsg = 0;
    CS_RETCODE ret;

    memset((void*)&errmsg, 0, sizeof(CS_CLIENTMSG));
    ret = cs_diag(context, CS_STATUS, CS_CLIENTMSG_TYPE, CS_UNUSED,
		  &lastmsg);
    if(DBIS->debug >= 3)
	PerlIO_printf(DBILOGFP, "get_cs_msg -> cs_diag(CS_STATUS): lastmsg = %d (ret = %d)\n", 
		      lastmsg, ret);
    if(ret != CS_SUCCEED) {
	warn("cs_diag(CS_STATUS) failed");
	return ret;
    }
    ret = cs_diag(context, CS_GET, CS_CLIENTMSG_TYPE, lastmsg, &errmsg);
    if(DBIS->debug >= 3)
	PerlIO_printf(DBILOGFP, "get_cs_msg -> cs_diag(CS_GET) ret = %d\n", 
		      ret);
    if(ret != CS_SUCCEED) {
	warn("cs_diag(CS_GET) failed");
	return ret;
    }

    DBIh_SET_ERR_CHAR(sth, (imp_xxh_t *)imp_sth, NULL, CS_NUMBER(errmsg.msgnumber),
		      errmsg.msgstring, NULL, NULL);
    
    if(cslib_cb) {
	dSP;
	int retval, count;
	
	ENTER;
	SAVETMPS;
	PUSHMARK(sp);
	
	XPUSHs(sv_2mortal(newSViv(CS_LAYER(errmsg.msgnumber))));
	XPUSHs(sv_2mortal(newSViv(CS_ORIGIN(errmsg.msgnumber))));
	XPUSHs(sv_2mortal(newSViv(CS_SEVERITY(errmsg.msgnumber))));
	XPUSHs(sv_2mortal(newSViv(CS_NUMBER(errmsg.msgnumber))));
	XPUSHs(sv_2mortal(newSVpv(errmsg.msgstring, 0)));
	if (errmsg.osstringlen > 0)
	    XPUSHs(sv_2mortal(newSVpv(errmsg.osstring, 0)));
	else
	    XPUSHs(&PL_sv_undef);
	if (msg)
	    XPUSHs(sv_2mortal(newSVpv(msg, 0)));
	else
	    XPUSHs(&PL_sv_undef);
	
	PUTBACK;
	if((count = perl_call_sv(cslib_cb, G_SCALAR)) != 1)
	    croak("A cslib handler cannot return a LIST");
	SPAGAIN;
	retval = POPi;
	
	PUTBACK;
	FREETMPS;
	LEAVE;
	
	return retval == 1 ? CS_SUCCEED : CS_FAIL;
    }
#if 0    
    PerlIO_printf(DBILOGFP, "\nCS Library Message:\n");
    PerlIO_printf(DBILOGFP, "Message number: LAYER = (%ld) ORIGIN = (%ld) ",
		  CS_LAYER(errmsg.msgnumber), CS_ORIGIN(errmsg.msgnumber));
    PerlIO_printf(DBILOGFP, "SEVERITY = (%ld) NUMBER = (%ld)\n",
		  CS_SEVERITY(errmsg.msgnumber), CS_NUMBER(errmsg.msgnumber));
    PerlIO_printf(DBILOGFP, "Message String: %s\n", errmsg.msgstring);
    if(msg)
	PerlIO_printf(DBILOGFP, "User Message: %s\n", msg);
    //fflush(stderr);
#endif
    return CS_FAIL;
}

/* Allocate a buffer of the appropriate size for "datatype". Only
   works for fixed-size datatypes */
static void * alloc_datatype(CS_INT datatype, int *len)
{
    void *ptr;
    int bytes;

    switch(datatype) {
    case CS_TINYINT_TYPE: bytes = sizeof(CS_TINYINT); break;
    case CS_SMALLINT_TYPE: bytes = sizeof(CS_SMALLINT); break;
    case CS_INT_TYPE: bytes = sizeof(CS_INT); break;
    case CS_REAL_TYPE: bytes = sizeof(CS_REAL); break;
    case CS_FLOAT_TYPE: bytes = sizeof(CS_FLOAT); break;
    case CS_BIT_TYPE: bytes = sizeof(CS_BIT); break;
    case CS_DATETIME_TYPE: bytes = sizeof(CS_DATETIME); break;
    case CS_DATETIME4_TYPE: bytes = sizeof(CS_DATETIME4); break;
    case CS_MONEY_TYPE: bytes = sizeof(CS_MONEY); break;
    case CS_MONEY4_TYPE: bytes = sizeof(CS_MONEY4); break;
    case CS_NUMERIC_TYPE: bytes = sizeof(CS_NUMERIC); break;
    case CS_DECIMAL_TYPE: bytes = sizeof(CS_DECIMAL); break;
    case CS_LONG_TYPE: bytes = sizeof(CS_LONG); break;
#if 0
    case CS_SENSITIVITY_TYPE: bytes = sizeof(CS_SENSITIVITY); break;
    case CS_BOUNDARY_TYPE: bytes = sizeof(CS_BOUNDARY); break;
#endif
    case CS_USHORT_TYPE: bytes = sizeof(CS_USHORT); break;
#if defined(CS_DATE_TYPE)
    case CS_DATE_TYPE: bytes = sizeof(CS_DATE); break;
    case CS_TIME_TYPE: bytes = sizeof(CS_TIME); break;
#endif
    default: warn("alloc_datatype: unkown type: %d", datatype); return NULL;
    }

    Newz(902, ptr, bytes, char);

    return ptr;
}


static int syb_blk_execute(imp_dbh_t *imp_dbh, imp_sth_t *imp_sth, SV *sth)
{
    int i;
    char name[32];
    void *ptr;
    CS_CONNECTION *con = imp_sth->connection ? 
	imp_sth->connection : imp_dbh->connection;
    STRLEN slen;
    CS_INT vlen;
    SV **svp;
    phs_t *phs;
    CS_RETCODE ret;

#if !defined(USE_CSLIB_CB)
    if(cs_diag(context, CS_CLEAR, CS_CLIENTMSG_TYPE, CS_UNUSED, NULL) != CS_SUCCEED)
	warn("cs_diag(CS_CLEAR) failed");
#endif

    for(i = 0; i < imp_sth->numCols; ++i) {
	sprintf(name, ":p%d", i+1);
	svp = hv_fetch(imp_sth->all_params_hv, name, strlen(name), 0);
	phs = ((phs_t*)(void*)SvPVX(*svp));
	phs->datafmt.format = CS_FMT_UNUSED;
	phs->datafmt.count = 1;
	if(!phs->sv || !SvOK(phs->sv) || phs->sv == &PL_sv_undef) {
	    imp_sth->coldata[i].indicator = 0;
	    ptr = "";
	    imp_sth->coldata[i].valuelen = 0;
	    if(!imp_sth->bcpIdentityFlag && imp_sth->bcpIdentityCol == i + 1)
		continue;
	} else {
	    imp_sth->coldata[i].ptr = SvPV(phs->sv, slen);
	    imp_sth->coldata[i].indicator = 0;

	    switch(phs->datafmt.datatype) {
#if 0
	      case CS_NUMERIC_TYPE:
	      case CS_DECIMAL_TYPE:
		if(_convert(&imp_sth->coldata[i].value.num,  
			    imp_sth->coldata[i].ptr, LOCALE(imp_dbh), 
			    &phs->datafmt, &vlen) != CS_SUCCEED) {
		  /* If the error handler returns CS_FAIL, then FAIL this
		     row! */
#if !defined(USE_CSLIB_CB)
		    if(get_cs_msg(context, con) != CS_SUCCEED)
			goto FAIL;
#else
		    warn("BLK _convert(CS_NUMERIC, %s) failed - see cslib error.", imp_sth->coldata[i].ptr);
#endif
		}
		imp_sth->coldata[i].valuelen = (vlen != CS_UNUSED ? vlen : sizeof(imp_sth->coldata[i].value.num));
		ptr = &imp_sth->coldata[i].value.num;
		break;
#endif
	      case CS_BINARY_TYPE:
	      case CS_LONGBINARY_TYPE:
	      case CS_LONGCHAR_TYPE:
	      case CS_TEXT_TYPE:
	      case CS_IMAGE_TYPE:
 	      case CS_CHAR_TYPE:
#if defined(CS_UNICHAR_TYPE)
	      case CS_UNICHAR_TYPE:
#endif
		/* For these types send data "as is" */
		ptr = imp_sth->coldata[i].ptr;
		imp_sth->coldata[i].valuelen = slen;
		break;
	      default:
		/* for all others, call cs_convert() before sending */
		if(!imp_sth->coldata[i].v_alloc) {
		  imp_sth->coldata[i].value.p = 
		    alloc_datatype(phs->datafmt.datatype, &imp_sth->coldata[i].v_alloc);
		}
		if(_convert(imp_sth->coldata[i].value.p,  
			    imp_sth->coldata[i].ptr, LOCALE(imp_dbh), 
			    &phs->datafmt, &vlen) != CS_SUCCEED) {
		  char msg[255];
		  /* If the error handler returns CS_FAIL, then FAIL this
		     row! */
#if !defined(USE_CSLIB_CB)
		  sprintf(msg, 
			  "cs_convert failed: column %d: (_convert(%s, %d))", 
			  i + 1, (char *)imp_sth->coldata[i].ptr, 
			  phs->datafmt.datatype);
		  ret = get_cs_msg(context, con, msg, sth, imp_sth);
		  if(ret == CS_FAIL)
		     goto FAIL;
#else
		  warn("cs_convert failed: column %d: (_convert(%s, %d))", 
		       i + 1, imp_sth->coldata[i].ptr, phs->datafmt.datatype);
		  ret = CS_FAIL;
		  goto FAIL;
#endif
		}
		imp_sth->coldata[i].valuelen = 
		  (vlen != CS_UNUSED ? vlen : imp_sth->coldata[i].v_alloc);
		ptr = imp_sth->coldata[i].value.p;
		break;
	    }
	}
	ret = blk_bind(imp_sth->bcp_desc,
		       i + 1,
		       &phs->datafmt,
		       ptr,
		       &imp_sth->coldata[i].valuelen,
		       &imp_sth->coldata[i].indicator);
	if(DBIS->debug >= 4)
	    PerlIO_printf(DBILOGFP, "blk_bind %d -> '%s' (ret = %d)\n", i+1, imp_sth->coldata[i].ptr, ret);
	if(ret != CS_SUCCEED)
	    goto FAIL;
    }

    ret = blk_rowxfer(imp_sth->bcp_desc);
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "blk_rowxfer() -> %d\n", ret);

    if(ret == CS_SUCCEED)
	imp_sth->bcpRows++;

 FAIL:;
    return (ret == CS_SUCCEED ? -1 : -2);
}

int      
syb_st_execute(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    dTHR;
    D_imp_dbh_from_sth;
    int restype;

    imp_dbh->lasterr = 0;
    imp_dbh->lastsev = 0;

    if(imp_sth->type == 2) {
	return syb_blk_execute(imp_dbh, imp_sth, sth);
    }

    if(!DBIc_is(imp_dbh, DBIcf_AutoCommit) && imp_dbh->doRealTran)
	if(syb_db_opentran(NULL, imp_dbh) == 0)
	    return -2; 

    if(!imp_sth->dyn_execed) {
	if(!imp_sth->cmd) {
	    /* only allocate a CS_COMMAND struct if there isn't one already
	       bug# 461 */
	    imp_sth->cmd = syb_alloc_cmd(imp_dbh, imp_sth->connection ? 
					 imp_sth->connection :
					 imp_dbh->connection);
	}
	if(ct_command(imp_sth->cmd, CS_LANG_CMD, imp_sth->statement,
		      CS_NULLTERM, CS_UNUSED) != CS_SUCCEED) {
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_st_execute() -> ct_command() failed (cmd=%x, statement=%s, imp_sth=%x)\n", imp_sth->cmd, imp_sth->statement, imp_sth);
	    return -2;
	}
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_st_execute() -> ct_command() OK\n");
    }

    if(ct_send(imp_sth->cmd) != CS_SUCCEED) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_st_execute() -> ct_send() failed\n");
	
	return -2;
    }
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_st_execute() -> ct_send() OK\n");

    imp_sth->exec_done = 1;

    restype = st_next_result(sth, imp_sth);

#if 0
    if(restype == CS_CMD_DONE || restype == CS_CMD_FAIL) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, 
			  "    syb_st_execute() -> got %s: resetting ACTIVE, moreResults, dyn_execed\n",
			  restype == CS_CMD_DONE ? "CS_CMD_DONE" : "CS_CMD_FAIL");
	imp_sth->moreResults = 0;
	imp_sth->dyn_execed  = 0;
	imp_sth->exec_done   = 0;
	DBIc_ACTIVE_off(imp_sth);
    } else {
	DBIc_ACTIVE_on(imp_sth);
    }
#endif

    if(restype == CS_CMD_FAIL)
	return -2;


    return imp_sth->numRows;
}

int
syb_st_cancel(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;
    CS_CONNECTION *connection = imp_sth->connection ? imp_sth->connection : 
	imp_dbh->connection;

    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_st_cancel() -> ct_cancel(CS_CANCEL_ATTN)\n");

    if(ct_cancel(connection, NULL, CS_CANCEL_ATTN) == CS_FAIL) {
	ct_close(connection, CS_FORCE_CLOSE);
	imp_dbh->isDead = 1;
    }	  
    
    return 1;
}

static int fix_fbav(imp_sth_t *imp_sth, int num_fields, AV *av)
{
    int clear_cache = 0;
    int i;

    /* XXX
       The code in the if() below is likely to break with new versions
       of DBI!!! */
    if(num_fields < imp_sth->numCols) {
	int isReadonly = SvREADONLY(av);
	++clear_cache;
	if(isReadonly)
	    SvREADONLY_off(av);		/* DBI sets this readonly  */
	i = imp_sth->numCols - 1;
	while(i >= num_fields)
	    av_store(av, i--, newSV(0));
	num_fields = AvFILL(av)+1;
	if(isReadonly)
	    SvREADONLY_on(av);		/* protect against shift @$row etc */
    } else if(num_fields > imp_sth->numCols) {
	int isReadonly = SvREADONLY(av);
	if(isReadonly)
	    SvREADONLY_off(av);		/* DBI sets this readonly  */
	av_fill(av, imp_sth->numCols - 1);
	num_fields = AvFILL(av)+1;
	if(isReadonly)
	    SvREADONLY_on(av);		/* protect against shift @$row etc */
	++clear_cache;
    }

    return clear_cache;
}

static void clear_cache(SV *sth)
{
    /* Clear cached statement handle attributes, if necessary */
    SV *value;
    char *key;
    I32 keylen;
    while ( (value = hv_iternextsv((HV*)SvRV(sth), &key, &keylen)) ) {
	if (strncmp(key, "NAME_", 5) == 0 ||
	    strncmp(key, "TYPE", 4) == 0 ||
	    strncmp(key, "PRECISION", 9) == 0 ||
	    strncmp(key, "SCALE", 5) == 0 ||
	    strncmp(key, "NULLABLE", 8) == 0               ) {
	    hv_delete((HV*)SvRV(sth), key, keylen, G_DISCARD);
	}
    }
}

AV *
syb_st_fetch(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    dTHR;
    D_imp_dbh_from_sth;
    CS_COMMAND *cmd = imp_sth->cmd;
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
    if ( !DBIc_is(imp_sth, DBIcf_ACTIVE) || !imp_sth->exec_done ) {
	return Nullav;
    }

    av = DBIS->get_fbav(imp_sth);
    num_fields = AvFILL(av)+1;

    if(fix_fbav(imp_sth, num_fields, av))
	clear_cache(sth);

    ChopBlanks = DBIc_has(imp_sth, DBIcf_ChopBlanks);

  TryAgain:
    retcode = ct_fetch(cmd, CS_UNUSED, CS_UNUSED, CS_UNUSED, &rows_read);

    if(DBIS->debug >= 3) {
	PerlIO_printf(DBILOGFP, "    syb_st_fetch() -> ct_fetch() = %d (%d rows)\n",
		      retcode, rows_read);
    }

    switch(retcode)
    {
      case CS_ROW_FAIL:
	  /* if LongTruncOK is off, then discard this row */
        if(!DBIc_is(imp_sth, DBIcf_LongTruncOk)) 
	    goto TryAgain;
      case CS_SUCCEED:
	for(i = 0; i < num_fields; ++i)
	{
	    SV *sv = AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV   */
	    len = 0;

	    if(DBIS->debug >= 4) {
		/*char *text = neatsvpv(phs->sv,0);*/
		PerlIO_printf(DBILOGFP, "    syb_st_fetch() -> %d/%d/%d\n",
			      i, imp_sth->coldata[i].valuelen, 
			      imp_sth->coldata[i].type);
	    }
	    /* If we're beyond the number of items in this result set
	       or: the data is null
	       or: noBindBlob is set and the data type is IMAGE or TEXT
	       then: set sv to undef */
	    if(i >= imp_sth->numCols || 
	       imp_sth->coldata[i].indicator == CS_NULLDATA ||
	       (imp_sth->noBindBlob && (imp_sth->datafmt[i].datatype == CS_TEXT_TYPE
					|| imp_sth->datafmt[i].datatype == CS_IMAGE_TYPE))) {
		/* NULL data */
		(void)SvOK_off(sv);
	    } else {
#define DATE_BUFF_LEN 50
		char buff[DATE_BUFF_LEN];	/* used for date conversions */

		switch(imp_sth->coldata[i].type) {
		  case CS_IMAGE_TYPE:
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
		  case CS_BINARY_TYPE:
		  case CS_VARBINARY_TYPE:
		    if (imp_dbh->useBin0x) {
			/* Add 0x to the front */
			sv_setpv(sv, "0x");
		    } else {
			/* stick in empty string so the concat works */
			sv_setpv(sv, "");
		    }
		    len = imp_sth->coldata[i].valuelen;
		    sv_catpvn(sv, imp_sth->coldata[i].value.c, len);
		    break;
		  case CS_DATETIME_TYPE:
		    len = date2str(&imp_sth->coldata[i].value.dt, 
				   &imp_sth->datafmt[i],
				   buff, DATE_BUFF_LEN,
				   imp_dbh->dateFmt, LOCALE(imp_dbh));
		    sv_setpvn(sv, buff, len);
		    break;
		  default:
		    croak("syb_st_fetch: unknown datatype: %d, column %d",
			  imp_sth->datafmt[i].datatype, i + 1);
		}
	    }
	}
	break;
      case CS_FAIL:		/* ohmygod */
	/* FIXME: Should we call ct_cancel() here, or should we let
	   the programmer handle it? */
	  if(ct_cancel(imp_dbh->connection, NULL, CS_CANCEL_ALL) == CS_FAIL) {
	      ct_close(imp_dbh->connection, CS_FORCE_CLOSE);
	      imp_dbh->isDead = 1;
	  }
	  return Nullav;
	break;
      case CS_END_DATA:		/* we've seen all the data for this result
				   set. So see if this is the end of the
				   result sets */

	  restype = st_next_result(sth, imp_sth);
	  if(DBIS->debug >= 2)
	      PerlIO_printf(DBILOGFP, "    syb_st_fetch() -> st_next_results() == %d\n",
		      restype);

	  if(restype == CS_CMD_DONE || restype == CS_CMD_FAIL) {
	      return Nullav;
	  } else {		/* XXX What to do here??? */
	      if(fix_fbav(imp_sth, num_fields, av))
		  clear_cache(sth);

	      if(restype == CS_COMPUTE_RESULT)
		  goto TryAgain;
	      imp_sth->moreResults = 1;
	  }
	  return Nullav;
	break;
      default:
	warn("ct_fetch() returned an unexpected retcode");
    }

    if(imp_dbh->row_cb) {
	dSP;
	int retval, count;
	
	ENTER;
	SAVETMPS;
	PUSHMARK(sp);
	
	
	XPUSHs(sv_2mortal(newRV((SV*)av)));
	
	PUTBACK;
	if((count = perl_call_sv(imp_dbh->row_cb, G_SCALAR)) != 1)
	    croak("An error handler can't return a LIST.");
	SPAGAIN;
	retval = POPi;
	
	PUTBACK;
	FREETMPS;
	LEAVE;
	
	/* If the called sub returns 0 then we don't return the result set
	   to the caller, so instead try to fetch the next row... */
	if(retval == 0)
	    goto TryAgain;
    }

    return av;
}

static int sth_blk_finish(imp_dbh_t *imp_dbh, imp_sth_t *imp_sth)
{
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    sth_blk_finish() -> Checking for pending rows\n");
    /* If there are any pending rows they should be rolled back, based
       on the principle that only *explicitly* commited data should be
       kept. */
    if(imp_sth->bcpRows > 0) {
	if(DBIc_WARN(imp_dbh)) {
	    warn("finish: %d uncommited rows will be rolled back",
		 imp_sth->bcpRows);
	}
	syb_blk_done(imp_sth, CS_BLK_CANCEL);
    } else if (imp_sth->bcpRows == 0)
	syb_blk_done(imp_sth, CS_BLK_ALL);

    blkCleanUp(imp_sth);
    /* Reset autocommit for this handle (see syb_blk_init()) */
    DBIc_set(imp_dbh, DBIcf_AutoCommit, imp_sth->bcpAutoCommit);
    toggle_autocommit(NULL, imp_dbh, imp_sth->bcpAutoCommit);

    imp_dbh->imp_sth = NULL;
    
    return 1;
}

int      syb_st_finish(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    dTHR;
    D_imp_dbh_from_sth;
    CS_CONNECTION *connection;

    if(imp_sth->bcp_desc) {
	return sth_blk_finish(imp_dbh, imp_sth);
    }

    connection = imp_sth->connection ? imp_sth->connection : 
	imp_dbh->connection;

    /* The SvOK() test is from Henry Asseily. It is there to
       avoid a possible infinite loop in the case where the handle
       is active, but has been invalidated by OPenSwitch. */
    /* Changed to check imp_dbh->lasterr instead */
    /*    if (imp_dbh->flushFinish && !(SvTRUE(DBIc_ERR(imp_dbh)))) { */
/*    if (imp_dbh->flushFinish && !imp_dbh->lasterr) { */
/* It is believed that the fixes applied to st_next_result() makes the
   imp_dbh->lasterr check unnecessary */
    if (imp_dbh->flushFinish) {
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_st_finish() -> flushing\n");
	while (DBIc_ACTIVE(imp_sth) && !imp_dbh->isDead && imp_sth->exec_done) {
	    AV *retval;
	    do {
		retval = syb_st_fetch(sth, imp_sth);
	    } while(retval && retval != Nullav);
	}
    }
    else {
	if (DBIc_ACTIVE(imp_sth)) {
#if defined(ROGUE)
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_st_finish() -> ct_cancel(CS_CANCEL_CURRENT)\n");
	    if(ct_cancel(NULL, imp_sth->cmd, CS_CANCEL_CURRENT) == CS_FAIL) {
		  ct_close(connection, CS_FORCE_CLOSE);
		  imp_dbh->isDead = 1;
	    }	  
#else  
	    if(DBIS->debug >= 2)
		PerlIO_printf(DBILOGFP, "    syb_st_finish() -> ct_cancel(CS_CANCEL_ALL)\n");
	    if(ct_cancel(connection, NULL, CS_CANCEL_ALL) == CS_FAIL) {
		  ct_close(connection, CS_FORCE_CLOSE);
		  imp_dbh->isDead = 1;
	    }	  
#endif
	}
    }
    if(DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_st_finish() -> resetting ACTIVE, moreResults, dyn_execed\n");
    imp_sth->moreResults = 0;
    imp_sth->dyn_execed = 0;
    DBIc_ACTIVE_off(imp_sth);
    return 1;
}

static void dealloc_dynamic(imp_sth)
    imp_sth_t *imp_sth;
{
    CS_RETCODE ret;
    CS_INT restype;
	
    if (DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    dealloc_dynamic: ct_dynamic(CS_DEALLOC) for %s\n",
		imp_sth->dyn_id);
    
    ret = ct_dynamic(imp_sth->cmd, CS_DEALLOC, imp_sth->dyn_id,
		     CS_NULLTERM, NULL, CS_UNUSED);
    if(ret != CS_SUCCEED) {
	if (DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    dealloc_dynamic: ct_dynamic(CS_DEALLOC) for %s FAILED\n",
			  imp_sth->dyn_id);
	return;
    }
    ret = ct_send(imp_sth->cmd);
    if(ret != CS_SUCCEED) {
	if (DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    dealloc_dynamic: ct_send(CS_DEALLOC) for %s FAILED\n",
			  imp_sth->dyn_id);
	return;
    }

    while(ct_results(imp_sth->cmd, &restype) == CS_SUCCEED)
	;

    if (imp_sth->all_params_hv) {
	HV *hv = imp_sth->all_params_hv;
	SV *sv;
	char *key;
	I32 retlen;
	hv_iterinit(hv);
	while( (sv = hv_iternextsv(hv, &key, &retlen)) != NULL ) {
	    if (sv != &PL_sv_undef) {
		phs_t *phs_tpl = (phs_t*)(void*)SvPVX(sv);
		sv_free(phs_tpl->sv);
	    }
	}
	sv_free((SV*)imp_sth->all_params_hv);
    }
 
    if (imp_sth->out_params_av)
	sv_free((SV*)imp_sth->out_params_av);

    imp_sth->all_params_hv = NULL;
    imp_sth->out_params_av = NULL;
}


void     syb_st_destroy(sth, imp_sth) 
    SV *sth;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;
    CS_RETCODE ret;
    dTHR;

    if (DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "    syb_st_destroy: called on %x...\n", imp_sth);

    if (PL_dirty) {
	DBIc_IMPSET_off(imp_sth);	/* let DBI know we've done it	*/
	if (DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_st_destroy: dirty set, skipping\n");
	return;
    }

    if (DBIc_ACTIVE(imp_dbh))
	if(!strncmp(imp_sth->dyn_id, "DBD", 3)) {
	    dealloc_dynamic(imp_sth);
	}

    /* moved from the prepare() call - as we need to have this around
       to re-execute non-dynamic statements... */
    if(imp_sth->statement != NULL) {
	if(DBIS->debug >= 2) {
	    PerlIO_printf(DBILOGFP, "    syb_st_destroy(): freeing imp_sth->statement\n");
	}
	safefree(imp_sth->statement);
	imp_sth->statement = NULL;
	imp_dbh->sql       = NULL;
    }

    cleanUp(imp_sth);

    if(imp_sth->cmd) {
	/* Gene Ressler says that this call can fail because we've already 
	   dropped the connection. I'm not sure if this is really a problem
	   or if it can be ignored. XXX */
	if(DBIS->debug >= 3)
	    PerlIO_printf(DBILOGFP, "    ct_cmd_drop() -> CS_COMMAND %x\n", imp_sth->cmd);

	ret = ct_cmd_drop(imp_sth->cmd);
	if(DBIS->debug >= 2) {
	    PerlIO_printf(DBILOGFP, "    syb_st_destroy(): cmd dropped: %d\n", ret);
	}
    }
    /* reset BLK data, if needed */
    if(imp_sth->bcp_desc) {
	/* XXX Should we call blk_done(CS_BLK_ALL) here??? */
	if(DBIS->debug >= 2)
	    PerlIO_printf(DBILOGFP, "    syb_st_destroy(): blkCleanUp()\n");
	
	sth_blk_finish(imp_dbh, imp_sth);
    }
    if(imp_sth->connection) {
	ret = ct_close(imp_sth->connection, CS_FORCE_CLOSE);
	if(DBIS->debug >= 2) {
	    PerlIO_printf(DBILOGFP, "    syb_st_destroy(): connection closed: %d\n", ret);
	}
	ct_con_drop(imp_sth->connection);
    }

    DBIc_ACTIVE_off(imp_sth);	/* Don't want DBI warning about freeing active handle */
    DBIc_IMPSET_off(imp_sth);	/* let DBI know we've done it	*/
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

int      syb_ct_get_data(sth, imp_sth, column, bufrv, buflen)
    SV *sth;
    imp_sth_t *imp_sth;
    int column;
    SV *bufrv;
    int buflen;
{
    CS_COMMAND *cmd = imp_sth->cmd;
    CS_VOID *buffer; 
/*    CS_INT buflen = imp_sth->datafmt[column-1].maxlength; */
    CS_INT outlen;
    CS_RETCODE ret;
    SV *bufsv;

    if(buflen == 0)
	buflen = imp_sth->datafmt[column-1].maxlength;

    if(DBIS->debug >= 3)
	PerlIO_printf(DBILOGFP, "    ct_get_data(%d): buflen = %d\n",
		column, buflen);

    /* Fix PR/444: segfault if passed a non-reference SV for buffer */
    if(!SvROK(bufrv)) {
	warn("ct_get_data: buffer parameter is not a reference!");
	return 0;
    }
    bufsv = SvRV(bufrv);
    Newz(902, buffer, buflen, char);

    ret = ct_get_data(cmd, column, (CS_VOID*)buffer, buflen, &outlen);
    if(outlen) {
	sv_setpvn(bufsv, buffer, outlen);
    } else {
	sv_setsv(bufsv, &PL_sv_undef);
    }
    if(DBIS->debug >= 3)
	PerlIO_printf(DBILOGFP, "    ct_get_data(%d): got %d bytes (ret = %d)\n",
		column, outlen, ret);

    Safefree(buffer);

    return outlen;
}

int      syb_ct_prepare_send(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    return ct_command(imp_sth->cmd, CS_SEND_DATA_CMD, NULL, CS_UNUSED, CS_COLUMN_DATA) == CS_SUCCEED;
}

int      syb_ct_finish_send(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    CS_RETCODE retcode;
    CS_INT restype;
    D_imp_dbh_from_sth;
     

    retcode = ct_send(imp_sth->cmd);
    if(DBIS->debug >= 3)
	PerlIO_printf(DBILOGFP, "    ct_finish_send(): ct_send() = %d\n",
		retcode);
    if(retcode != CS_SUCCEED) {
	return 0;
    }

    while((retcode = ct_results(imp_sth->cmd, &restype)) == CS_SUCCEED) {
	if(DBIS->debug >= 3)
	    PerlIO_printf(DBILOGFP, "    ct_finish_send(): ct_results(%d) = %d\n",
		    restype, retcode);
	if(restype == CS_PARAM_RESULT) {
	    CS_DATAFMT datafmt;
	    CS_INT count;

	    retcode = ct_describe(imp_sth->cmd, 1, &datafmt);
	    if(retcode != CS_SUCCEED) {
		if(DBIS->debug >= 3)
		    PerlIO_printf(DBILOGFP, "    ct_finish_send(): ct_describe() failed\n");
		return 0;
	    }
	    datafmt.maxlength = sizeof(imp_dbh->iodesc.timestamp);
	    datafmt.format     = CS_FMT_UNUSED;
	    if((retcode = ct_bind(imp_sth->cmd, 1, &datafmt, 
				  (CS_VOID *)imp_dbh->iodesc.timestamp,
				  &imp_dbh->iodesc.timestamplen,
				  NULL)) != CS_SUCCEED) {
		if(DBIS->debug >= 3)
		    PerlIO_printf(DBILOGFP, "    ct_finish_send(): ct_bind() failed\n");
		return 0;
	    }
	    retcode = ct_fetch(imp_sth->cmd, CS_UNUSED, CS_UNUSED, 
			       CS_UNUSED, &count);
	    if(retcode != CS_SUCCEED) {
		if(DBIS->debug >= 3)
		    PerlIO_printf(DBILOGFP, "    ct_finish_send(): ct_fetch() failed\n");
		return 0;
	    }
	    /* success... so cancel the rest of this result set */

	    retcode = ct_cancel(NULL, imp_sth->cmd, CS_CANCEL_CURRENT);
	    if(retcode != CS_SUCCEED) {
		if(DBIS->debug >= 3)
		    PerlIO_printf(DBILOGFP, "    ct_finish_send(): ct_fetch() failed\n");
		return 0;
	    }
	}
    }

    return 1;
}		

int      syb_ct_send_data(sth, imp_sth, buffer, size)
    SV *sth;
    imp_sth_t *imp_sth;
    char *buffer;
    int size;
{
    if(DBIS->debug >= 3)
	PerlIO_printf(DBILOGFP, "    ct_send_data(): sending buffer size %d bytes\n",
		size);
    return ct_send_data(imp_sth->cmd, buffer, size) == CS_SUCCEED;
}

int      syb_ct_data_info(sth, imp_sth, action, column, attr)
    SV *sth;
    imp_sth_t *imp_sth;
    int column;
    int action;
    SV *attr;
{
    D_imp_dbh_from_sth;
    CS_COMMAND *cmd = imp_sth->cmd;
    CS_RETCODE ret;

    if(action == CS_SET) {
	/* we expect the app to maybe modify certain fields of the CS_IODESC
	   struct. This is done via the attr hash that is passed in here */
	if(attr && attr != &PL_sv_undef && SvROK(attr)) {
	    SV **svp;

	    svp = hv_fetch((HV*)SvRV(attr), "total_txtlen", 12, 0);
	    if (svp && SvGMAGICAL(*svp))   /* eg if from tainted expression */
		mg_get(*svp);
	    if(svp && SvIOK(*svp))
		imp_dbh->iodesc.total_txtlen = SvIV(*svp);

	    if(DBIS->debug >= 3)
		PerlIO_printf(DBILOGFP, "    ct_data_info(): set total_txtlen to %d\n",
			imp_dbh->iodesc.total_txtlen);


	    svp = hv_fetch((HV*)SvRV(attr), "log_on_update", 13, 0);
	    if (svp && SvGMAGICAL(*svp))   /* eg if from tainted expression */
		mg_get(*svp);
	    if(svp && SvIOK(*svp))
		imp_dbh->iodesc.log_on_update = SvIV(*svp);
	    if(DBIS->debug >= 3)
		PerlIO_printf(DBILOGFP, "    ct_data_info(): set log_on_update to %d\n",
			imp_dbh->iodesc.log_on_update);
	}
    }

    if(action == CS_SET) {
	column = CS_UNUSED;
    } else {
	if(DBIS->debug >= 3)
	    PerlIO_printf(DBILOGFP, "    ct_data_info(): get IODESC for column %d\n",
		    column);
    }

    ret = ct_data_info(cmd, action, column, &imp_dbh->iodesc);

    if(action == CS_GET && DBIS->debug >= 3) {
	PerlIO_printf(DBILOGFP, "    ct_data_info(): ret = %d, total_txtlen = %d\n",
		ret, imp_dbh->iodesc.total_txtlen);
    } else if(DBIS->debug >= 3) {
	PerlIO_printf(DBILOGFP, "    ct_data_info(): ret = %d\n",
		ret);
    }

    return ret == CS_SUCCEED;
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
    s_A("LENGTH"),		/* 8 */
    s_A("syb_types"),		/* 9 */
    s_A("syb_result_type"),	/* 10 */
    s_A("LongReadLen"),         /* 11 */
    s_A("syb_proc_status"),     /* 12 */
    s_A("syb_do_proc_status"),  /* 13 */
    s_A("syb_no_bind_blob"),    /* 14 */
    s_A("CursorName"),          /* 15 - PR/394 */
    s_A(""),			/* END */
};

static T_st_params S_st_store_params[] = 
{
    s_A("syb_do_proc_status"),  /* 0 */
    s_A("syb_no_bind_blob"),    /* 1 */
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

    for (par = S_st_fetch_params; par->len > 0; par++)
	if (par->len == kl && strEQ(key, par->str))
	    break;

    if (par->len <= 0)
	return Nullsv;

    if (!imp_sth->done_desc && (par - S_st_fetch_params) < 10) {
	/* Because of the way Sybase returns information on returned values
	   in a SELECT statement we can't call describe() here. */
	/* Changed Nullsv to PL_sv_undef here to fix PR 541. */
	return &PL_sv_undef; 
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
		     ? newSViv(1) : newSViv(0));
	break;
      case 4:			/* TYPE */
	av = newAV();
	retsv = newRV(sv_2mortal((SV*)av));
	while(--i >= 0) 
	    av_store(av, i, newSViv(map_syb_types(imp_sth->coldata[i].realType)));
	break;
      case 5:			/* PRECISION */
	av = newAV();
	retsv = newRV(sv_2mortal((SV*)av));
	while(--i >= 0) 
	    av_store(av, i, newSViv(imp_sth->datafmt[i].precision ?
				    imp_sth->datafmt[i].precision :
				    imp_sth->coldata[i].realLength));
	break;
      case 6:			/* SCALE */
	av = newAV();
	retsv = newRV(sv_2mortal((SV*)av));
	while(--i >= 0) {
	    switch(imp_sth->coldata[i].realType) {
	      case CS_NUMERIC_TYPE:
	      case CS_DECIMAL_TYPE:
		av_store(av, i, newSViv(imp_sth->datafmt[i].scale));
		break;
	      default:
		av_store(av, i, newSVsv(&PL_sv_undef));
	    }
	}
	break;
      case 7:
	retsv = newSViv(imp_sth->moreResults);
	break;
      case 8:
	av = newAV();
	retsv = newRV(sv_2mortal((SV*)av));
	while(--i >= 0)
	    av_store(av, i, newSViv(imp_sth->coldata[i].realLength));
	break;
      case 9:			/* syb_types: native datatypes */
	av = newAV();
	retsv = newRV(sv_2mortal((SV*)av));
	while(--i >= 0) 
	    av_store(av, i, newSViv(imp_sth->coldata[i].realType));
	break;
      case 10:
	retsv = newSViv(imp_sth->lastResType);
	break;
      case 11:
	retsv = newSViv(DBIc_LongReadLen(imp_sth));
	break;
      case 12:
	retsv = newSViv(imp_sth->lastProcStatus);
	break;
      case 13:
	retsv = newSViv(imp_sth->doProcStatus);
	break;
      case 14:
	retsv = newSViv(imp_sth->noBindBlob);
	break;
      case 15:
	retsv = &PL_sv_undef;    /* fix for PR/394 */
	break;
      default:
	return Nullsv;
    }

    if(retsv == &sv_no || retsv == &sv_yes || retsv == &PL_sv_undef)
	return retsv;

    return sv_2mortal(retsv);
}


int
syb_st_STORE_attrib(sth, imp_sth, keysv, valuesv)
    SV *sth;
    imp_sth_t *imp_sth;
    SV *keysv;
    SV *valuesv;
{
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    T_st_params *par;

    if(DBIS->debug >= 2) {
	PerlIO_printf(DBILOGFP, "    syb_st_STORE(): key = %s\n", key);
    }

 
    for (par = S_st_store_params; par->len > 0; par++)
	if (par->len == kl && strEQ(key, par->str))
	    break;

    if (par->len <= 0)
	return FALSE;
    
    if(DBIS->debug >= 2) {
	PerlIO_printf(DBILOGFP, "    syb_st_STORE(): storing %d for key = %s\n", SvTRUE(valuesv), key);
    }
    switch(par - S_st_store_params)
    {
      case 0 :
	if(SvTRUE(valuesv)) {
	    imp_sth->doProcStatus = 1;
	} else {
	    imp_sth->doProcStatus = 0;
	}
	return TRUE;
      case 1:
	if(SvTRUE(valuesv)) {
	    imp_sth->noBindBlob = 1;
	} else {
	    imp_sth->noBindBlob = 0;
	}
	return TRUE;
    }
    return FALSE;
}

static int
date2str(CS_DATETIME *dt, CS_DATAFMT *srcfmt, 
	 char *buff, CS_INT len, int type, CS_LOCALE *locale)
{
    if(type == 0) {
	CS_DATAFMT dstfmt;

	memset(&dstfmt, 0, sizeof(dstfmt));
	dstfmt.datatype  = CS_CHAR_TYPE;
	dstfmt.maxlength = len;
	dstfmt.format    = CS_FMT_NULLTERM;
	dstfmt.locale    = locale;
	cs_convert(context, srcfmt, dt, &dstfmt, buff, &len);

	return len - 1;
    } else {
	CS_DATEREC rec;
	cs_dt_crack(context, CS_DATETIME_TYPE, dt, &rec);
	if(type == 2) {
	    sprintf(buff, "%4.4d-%2.2d-%2.2dT%2.2d:%2.2d:%2.2d.%3.3dZ",
		    rec.dateyear,
		    rec.datemonth,
		    rec.datedmonth,
		    rec.datehour,
		    rec.dateminute,
		    rec.datesecond,
		    rec.datemsecond);
	} else {
	    sprintf(buff, "%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d.%3.3d",
		    rec.dateyear,
		    rec.datemonth,
		    rec.datedmonth,
		    rec.datehour,
		    rec.dateminute,
		    rec.datesecond,
		    rec.datemsecond);
	}

	return strlen(buff);
    }

    return 0;
}

static CS_NUMERIC
to_numeric(str, locale, datafmt, type)
    char *str;
    CS_LOCALE *locale;
    CS_DATAFMT *datafmt;
    int type;
{
    CS_NUMERIC mn;
    CS_DATAFMT srcfmt;
    CS_INT reslen;
    char *p;

    memset(&mn, 0, sizeof(mn));

    if(!str || !*str)
	str = "0";

/*    warn("to_money(%s)\n", str); */
    
    memset(&srcfmt, 0, sizeof(srcfmt));
    srcfmt.datatype  = CS_CHAR_TYPE;
    srcfmt.maxlength = strlen(str);
    srcfmt.format    = CS_FMT_NULLTERM;
    srcfmt.locale    = locale;

    if(type) {			/* RPC call */
	if((p = strchr(str, '.')))
	    datafmt->scale = strlen(p+1);
	else
	    datafmt->scale = 0;
	datafmt->precision = strlen(str);
    } else { /* dynamic SQL */
    /* If the number of digits after the . is larger than
       the 'scale' value in datafmt, then we need to adjust it. Otherwise
       the conversion fails */
	if((p = strchr(str, '.'))) {
	    int len = strlen(++p);
	    if(len > datafmt->scale) {
		if(p[datafmt->scale] < '5')
		    p[datafmt->scale] = 0;
		else {
		    p[datafmt->scale] = 0;
		    len = strlen(str);
		    while(len--) {
			if(str[len] == '.')
			    continue;
			if(str[len] < '9') {
			    str[len]++;
			    break;
			}
			str[len] = '0';
			if(len == 0) {
			    char buf[64];
			    buf[0] = '1';
			    buf[1] = 0;
			    strcat(buf, str);
			    strcpy(str, buf);
			    break;
			}
		    }
		}
	    }
	}
    }
	    
    if (cs_convert(context, &srcfmt, str, datafmt,
		   &mn, &reslen) != CS_SUCCEED)
	warn("cs_convert failed (to_numeric(%s))", str);

    if(reslen == CS_UNUSED)
	warn("conversion failed: to_numeric(%s)", str);


    return mn;
}

static CS_MONEY
to_money(str, locale)
    char *str;
    CS_LOCALE *locale;
{
    CS_MONEY mn;
    CS_DATAFMT srcfmt, destfmt;
    CS_INT reslen;

    memset(&mn, 0, sizeof(mn));

    if(!str)
	return mn;
    
    memset(&srcfmt, 0, sizeof(srcfmt));
    srcfmt.datatype  = CS_CHAR_TYPE;
    srcfmt.maxlength = strlen(str);
    srcfmt.format    = CS_FMT_NULLTERM;
    srcfmt.locale    = locale;
	    
    memset(&destfmt, 0, sizeof(destfmt));
	    
    destfmt.datatype  = CS_MONEY_TYPE;
    destfmt.locale    = locale;
    destfmt.maxlength = sizeof(CS_MONEY);
    destfmt.format    = CS_FMT_UNUSED;
    
    if (cs_convert(context, &srcfmt, str, &destfmt,
		   &mn, &reslen) != CS_SUCCEED)
	warn("cs_convert failed (to_money(%s))", str);

    if(reslen == CS_UNUSED)
	warn("conversion failed: to_money(%s)", str);

    return mn;
}

static CS_BINARY *
to_binary(str)
    char *str;
{
    CS_BINARY *b, *b_ptr;
    char s[3], *strtol_end;
    int i, b_len;
    long int x;
    
    /* Advance past the 0x. We could use the value of syb_use_bin_0x 
       to infer whether to advance or not, but it's just as easy to 
       explicitly check. */
     if (str[0] == '0' && str[1] == 'x') 
	 str+=2;

     /* The length of 'str' _should_ be even, but we go thru some acrobatics
        to handle an odd length. We won't flag it as invalid, just pretend
        it's okay. */
     b_len = (strlen(str)+1) / 2;
     b = (CS_BINARY *)safemalloc(b_len);
     memset(b, 0, b_len);
     memset(&s, '\0', 3);
  
     /* Pack the characters */
     b_ptr = b;
     for (i=0; i<b_len; i++, str+=2) {
	 strncpy(s, str, 2);
	 x = strtol(s, &strtol_end, 16);
	 if (*strtol_end != '\0') {
	     warn("conversion failed: invalid char '%c'", *strtol_end);
	     break;
	 }
	 *b_ptr++ = x;
     }
     return b;
}



static int 
_dbd_rebind_ph(sth, imp_sth, phs, maxlen) 
    SV *sth;
    imp_sth_t *imp_sth;
    phs_t *phs;
    int maxlen;
{
    D_imp_dbh_from_sth;
    CS_RETCODE rc;
    STRLEN value_len;
    int i_value;
    double d_value;
    void *value;
    CS_NUMERIC n_value;
    CS_MONEY m_value;
    CS_INT datatype;
    int free_value = 0;

    if (DBIS->debug >= 2) {
        char *text = neatsvpv(phs->sv,0);
 	PerlIO_printf(DBILOGFP, "       bind %s (%s) <== %s (", 
		      phs->name, phs->varname, text);
 	if (SvOK(phs->sv)) 
 	     PerlIO_printf(DBILOGFP, "size %ld/%ld/%ld, ",
 		(long)SvCUR(phs->sv),(long)SvLEN(phs->sv),phs->maxlen);
	else PerlIO_printf(DBILOGFP, "NULL, ");
 	PerlIO_printf(DBILOGFP, "ptype %d, otype %d%s)\n",
 	    (int)SvTYPE(phs->sv), phs->ftype,
 	    (phs->is_inout) ? ", inout" : "");
    }
 
    /* At the moment we always do sv_setsv() and rebind.        */
    /* Later we may optimise this so that more often we can     */
    /* just copy the value & length over and not rebind.        */
#if 0 
    if (phs->is_inout) {        /* XXX */
        if (SvREADONLY(phs->sv))
            croak(no_modify);
        /* phs->sv _is_ the real live variable, it may 'mutate' later   */
        /* pre-upgrade high to reduce risk of SvPVX realloc/move        */
        (void)SvUPGRADE(phs->sv, SVt_PVNV);
        /* ensure room for result, 28 is magic number (see sv_2pv)      */
        SvGROW(phs->sv, (phs->maxlen < 28) ? 28 : phs->maxlen+1);
    }
    else {
        /* phs->sv is copy of real variable, upgrade to at least string */
        (void)SvUPGRADE(phs->sv, SVt_PV);
    }
#else
    /* phs->sv is copy of real variable, upgrade to at least string */
    (void)SvUPGRADE(phs->sv, SVt_PV);
#endif
 
    /* At this point phs->sv must be at least a PV with a valid buffer, */
    /* even if it's undef (null)                                        */
    /* Here we set phs->sv_buf, and value_len.                */

    /* determine the value, and length that we wish to pass to
       ct_param() */
    datatype = phs->datafmt.datatype;

    if (SvOK(phs->sv)) {
        phs->sv_buf = SvPV(phs->sv, value_len);

	switch(phs->datafmt.datatype) {
	  case CS_INT_TYPE:
	  case CS_SMALLINT_TYPE:
	  case CS_TINYINT_TYPE:
	  case CS_BIT_TYPE:
	    phs->datafmt.datatype = CS_INT_TYPE;
	    i_value = atoi(phs->sv_buf);
	    value = &i_value;
	    value_len = 4;
	    break;
	  case CS_NUMERIC_TYPE:
	  case CS_DECIMAL_TYPE:
	    n_value = to_numeric(phs->sv_buf, LOCALE(imp_dbh), &phs->datafmt,
				 imp_sth->type);
	    phs->datafmt.datatype = CS_NUMERIC_TYPE;
	    value = &n_value;
	    value_len = sizeof(n_value);
	    break;
	  case CS_MONEY_TYPE:
	  case CS_MONEY4_TYPE:
	    m_value = to_money(phs->sv_buf, LOCALE(imp_dbh));
	    phs->datafmt.datatype = CS_MONEY_TYPE;
	    value = &m_value;
	    value_len = sizeof(m_value);
	    break;
	  case CS_REAL_TYPE:
	  case CS_FLOAT_TYPE:
	    phs->datafmt.datatype = CS_FLOAT_TYPE;
	    d_value = atof(phs->sv_buf);
	    value = &d_value;
	    value_len = sizeof(double);
	    break;
	  case CS_BINARY_TYPE:
	    /* If this binary value is in hex format, with or without the
	       leading 0x, then convert to actual binary value.
	       Fix contributed by Tim Ayers */
	    phs->datafmt.datatype = CS_BINARY_TYPE;
	    if((phs->sv_buf[0] == '0' && phs->sv_buf[1] == 'x') ||
	       strspn(phs->sv_buf, "abcdefABCDEF0123456789") == value_len) 
	    {
		value = to_binary(phs->sv_buf);
		++free_value;
	    } else {
		value = phs->sv_buf;
	    } 
	    /* value_len = SvCUR(phs->sv_buf); */
	    break;
	  case CS_DATETIME_TYPE:
	  case CS_DATETIME4_TYPE:
	    phs->datafmt.datatype = CS_CHAR_TYPE;
	    value = phs->sv_buf;
	    value_len = CS_NULLTERM;
	    /* PR/464: datetime values get converted to "jan 1 1900" if turned
	       into a single space */
	    if(*(char*)value == 0) {
		value = NULL;
		value_len = CS_UNUSED;
	    }
	    break;

	  default:
	    phs->datafmt.datatype = CS_CHAR_TYPE;
	    value = phs->sv_buf;
	    /*value_len = CS_NULLTERM;*/ /*Allow embedded NUL bytes in strings?*/
	    /* PR/446: should an empty string cause a NULL, or not? */
	    if(*(char*)value == 0) {
		if(imp_dbh->bindEmptyStringNull) {
		    value = NULL;
		    value_len = CS_UNUSED;
		} else {
		    value = " ";
		}
	    }
	    break;
	}
    } else {      /* it's null but point to buffer incase it's an out var */
        phs->sv_buf = SvPVX(phs->sv);
        value_len   = 0;
	value       = NULL;
    }
    phs->sv_type = SvTYPE(phs->sv);     /* part of mutation check       */
    phs->maxlen  = SvLEN(phs->sv)-1;    /* avail buffer space   */
    /* value_len has current value length */

    if (DBIS->debug >= 3) {
        PerlIO_printf(DBILOGFP, "       bind %s <== '%.100s' (size %d, ok %d)\n",
            phs->name, phs->sv_buf, (long)phs->maxlen, SvOK(phs->sv)?1:0);
    }
    if (DBIS->debug >= 3) {
        PerlIO_printf(DBILOGFP, "       datafmt: type=%d, status=%d, len=%d\n",
		phs->datafmt.datatype, phs->datafmt.status, value_len);
        PerlIO_printf(DBILOGFP, "       saved type: %d\n", datatype);
    }

#if 1
    /* If this handle is still active call finish()... */
    if(DBIc_ACTIVE(imp_sth) && imp_sth->exec_done) {
	int finish = imp_dbh->flushFinish;
	imp_dbh->flushFinish = 1;
	syb_st_finish(sth, imp_sth);
	imp_dbh->flushFinish = finish;
    }
#endif

    if(imp_sth->dyn_execed == 0) {
	if(imp_sth->type == 0) {
	    if(ct_dynamic(imp_sth->cmd, CS_EXECUTE, imp_sth->dyn_id, CS_NULLTERM,
			  NULL, CS_UNUSED) != CS_SUCCEED)
		return 0;
	} else if(imp_sth->type == 1) {
	    if(ct_command(imp_sth->cmd, CS_RPC_CMD, imp_sth->proc, 
			  CS_NULLTERM, CS_NO_RECOMPILE) != CS_SUCCEED) {
		char errbuf[1024];
		sprintf(errbuf, "ct_command(CS_RPC_CMD, %s) failed\n", imp_sth->proc);
		syb_set_error(imp_dbh, -1, errbuf);
		return 0;
	    }
	}
	imp_sth->dyn_execed = 1;
    }

    if((rc = ct_param(imp_sth->cmd, &phs->datafmt, value, value_len, 0)) 
       != CS_SUCCEED)
	syb_set_error(imp_dbh, -1, "ct_param() failed!");

    phs->datafmt.datatype = datatype;

    if(free_value && value != NULL)
	safefree(value);

    return (rc == CS_SUCCEED);
}
 
int      syb_bind_ph(sth, imp_sth, ph_namesv, newvalue, sql_type,
		     attribs, is_inout, maxlen)
    SV *sth;
    imp_sth_t *imp_sth;
    SV *ph_namesv;
    SV *newvalue;
    IV sql_type;
    SV *attribs;
    int is_inout;
    IV maxlen;
{
    SV **phs_svp;
    STRLEN name_len;
    char *name;
    char namebuf[30];
    phs_t *phs;
    STRLEN lna;

    /* This is the way Tim does it in DBD::Oracle to get around the
       tainted issue. */
    if (SvGMAGICAL(ph_namesv))	/* eg if from tainted expression */
	mg_get(ph_namesv);
    if (!SvNIOKp(ph_namesv)) {
	name = SvPV(ph_namesv, name_len);
    }
    if (SvNIOKp(ph_namesv) || (name && isDIGIT(name[0]))) {
	sprintf(namebuf, ":p%d", (int)SvIV(ph_namesv));
	name = namebuf;
	name_len = strlen(name);
    }

    if (SvTYPE(newvalue) > SVt_PVLV)    /* hook for later array logic   */
        croak("Can't bind non-scalar value (currently)");
#if 0
    if (SvTYPE(newvalue) == SVt_PVLV && is_inout)       /* may allow later */
	croak("Can't bind ``lvalue'' mode scalar as inout parameter (currently)");
#endif

    if (DBIS->debug >= 2)
	PerlIO_printf(DBILOGFP, "bind %s <== '%.200s' (attribs: %s)\n",
		name, SvPV(newvalue,lna), attribs ? SvPV(attribs,lna) : "" );

    phs_svp = hv_fetch(imp_sth->all_params_hv, name, name_len, 0);
    if (phs_svp == NULL)
	croak("Can't bind unknown placeholder '%s'", name);
    phs = (phs_t*)SvPVX(*phs_svp);	/* placeholder struct	*/

    if (phs->sv == &PL_sv_undef) { /* first bind for this placeholder      */
	phs->sql_type = (sql_type) ? sql_type : SQL_CHAR;
        phs->ftype    = map_sql_types(phs->sql_type);
	if(imp_sth->type == 1) { /* RPC call, must set up the datafmt struct */
	    if(phs->varname[0] == '@') {
		strcpy(phs->datafmt.name, phs->varname);
		phs->datafmt.namelen = strlen(phs->varname);
	    } else
		phs->datafmt.namelen = 0;
	    phs->datafmt.datatype = phs->ftype;
	    phs->datafmt.status = phs->is_inout? CS_RETURN : CS_INPUTVALUE;
	    phs->datafmt.maxlength = 0;
	}
        phs->maxlen   = maxlen;         /* 0 if not inout               */
/*        phs->is_inout = is_inout; */
#if 0
        if (is_inout) {
            phs->sv = SvREFCNT_inc(newvalue);   /* point to live var    */
            ++imp_sth->has_inout_params;
            /* build array of phs's so we can deal with out vars fast   */
            if (!imp_sth->out_params_av)
                imp_sth->out_params_av = newAV();
            av_push(imp_sth->out_params_av, SvREFCNT_inc(*phs_svp));
        }
#endif
 
        /* some types require the trailing null included in the length. */
        phs->alen_incnull = 0;  
    }
#if 0
        /* check later rebinds for any changes */
    else if (is_inout || phs->is_inout) {
        croak("Can't rebind or change param %s in/out mode after first bind", phs->name);
    }
#endif
    else if (maxlen && maxlen != phs->maxlen) {
        croak("Can't change param %s maxlen (%ld->%ld) after first bind",
                        phs->name, phs->maxlen, maxlen);
    }
 
    if (!is_inout) {    /* normal bind to take a (new) copy of current value    */
        if (phs->sv == &PL_sv_undef)       /* (first time bind) */
            phs->sv = newSV(0);
        sv_setsv(phs->sv, newvalue);
    }

    /* BLK binding done at execute time, in a loop */
    if(imp_sth->type == 2)
	return 1;

    return _dbd_rebind_ph(sth, imp_sth, phs, 0);
}


static CS_RETCODE
fetch_data(imp_dbh, cmd)
imp_dbh_t       *imp_dbh;
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

    char buff[1024];

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

    display_header(imp_dbh, num_cols, datafmt);

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
	    sprintf(buff, "Error on row %ld.\n", row_count);
	    sv_catpv(DBIc_ERRSTR(imp_dbh), buff);
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
	    sv_catpv(DBIc_ERRSTR(imp_dbh), coldata[i].value.c);

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
		    sv_catpv(DBIc_ERRSTR(imp_dbh), " ");
		}
	    }
	} 
	sv_catpv(DBIc_ERRSTR(imp_dbh), "\n");
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

static int map_sql_types(sql_type)
    int sql_type;
{
    int ret;
    switch(sql_type) {
      case SQL_NUMERIC:
      case SQL_DECIMAL:
	ret = CS_NUMERIC_TYPE;
	break;
      case SQL_BIT:
      case SQL_INTEGER:
      case SQL_SMALLINT:
	ret = CS_INT_TYPE;
	break;
      case SQL_FLOAT:
      case SQL_REAL:
      case SQL_DOUBLE:
	ret = CS_FLOAT_TYPE;
	break;
      case SQL_BINARY:
	return CS_BINARY_TYPE;
	break;
      default:
	ret = CS_CHAR_TYPE;
    }

    return ret;
}

static int map_syb_types(syb_type)
    int syb_type;
{
    switch(syb_type) {
    case CS_CHAR_TYPE:		return 1;
    case CS_BINARY_TYPE:	return -2;
/*    case CS_LONGCHAR_TYPE:	return SQL_CHAR; * XXX */
/*    case CS_LONGBINARY_TYPE:	return SQL_BINARY; * XXX */
    case CS_TEXT_TYPE:		return -1; /* XXX */
    case CS_IMAGE_TYPE:		return -4; /* XXX */
    case CS_BIT_TYPE:           return -7;
    case CS_TINYINT_TYPE:       return -6;
    case CS_SMALLINT_TYPE:      return 5; 
    case CS_INT_TYPE:		return 4;
    case CS_REAL_TYPE:		return 7; 
    case CS_FLOAT_TYPE:		return 6;
#if defined(CS_DATE_TYPE)
      case CS_DATE_TYPE:
#endif
    case CS_DATETIME_TYPE:
    case CS_DATETIME4_TYPE:	return 9;
#if defined(CS_TIME_TYPE)
      case CS_TIME_TYPE:        return 10;
#endif
    case CS_MONEY_TYPE:
    case CS_MONEY4_TYPE:	return 3;
    case CS_NUMERIC_TYPE:	return 2;
    case CS_DECIMAL_TYPE:	return 3;
    case CS_VARCHAR_TYPE:	return 12;
    case CS_VARBINARY_TYPE:	return -3;
/*    case CS_TIMESTAMP_TYPE:     return -3;  */

    default:
	return SQL_CHAR;
    }
}

static char *my_strdup(string)
    char *string;
{
    char *buff = safemalloc(strlen(string) + 1);
    strcpy(buff, string);

    return buff;
}

static void fetchKerbTicket(imp_dbh_t *imp_dbh)
{
    if(imp_dbh->kerbGetTicket) {
	dSP;
	SV *retval;
	int count;
	char *server = imp_dbh->server;

	if(!*server) {
	    char *s = getenv("DSQUERY");
	    if(s && *s) {
		server = s;
	    } else {
		server = "SYBASE";
	    }
	}
	
	ENTER;
	SAVETMPS;
	PUSHMARK(sp);
	
	XPUSHs(sv_2mortal(newSVpv(server, 0)));
	
	PUTBACK;
	if((count = perl_call_sv(imp_dbh->kerbGetTicket, G_SCALAR)) != 1)
	    croak("A Kerberos Ticket handler can't return a LIST.");
	SPAGAIN;
	retval = POPs;
	
	PUTBACK;
	FREETMPS;
	LEAVE;

	if(SvPOK(retval)) {
	    strncpy(imp_dbh->kerberosPrincipal, SvPVX(retval), 31);
	    imp_dbh->kerberosPrincipal[31] = 0;
	}
    }
}

static CS_RETCODE syb_blk_init(imp_dbh_t *imp_dbh, imp_sth_t *imp_sth)
{
    CS_RETCODE ret;
    char table[256];
    int i, num_cols;
    SV **svp;
    phs_t *phs;
    char name[32];

    if(!getTableName(imp_sth->statement, table, 256)) {
	char str[512];
	sprintf(str, "Can't get table name from '%.256s'", imp_sth->statement);
	syb_set_error(imp_dbh, -1, str);
	return CS_FAIL;
    }
    if (DBIS->debug >= 3) {
        PerlIO_printf(DBILOGFP, 
		      "       syb_blk_init(): table=%s\n",
		      table);
    }

    /* If AutoCommit is "officially" off here, then we need to make sure
       that Sybase thinks that it is *on*, otherwise the blk_init() call
       below will fail. */

    if(!DBIc_is(imp_dbh, DBIcf_AutoCommit)) {
	toggle_autocommit(NULL, imp_dbh, 1);
    }

    ret = blk_alloc(imp_sth->connection ? imp_sth->connection : imp_dbh->connection,
		    BLK_VERSION, 
		    &imp_sth->bcp_desc);
    if(ret != CS_SUCCEED)
	goto FAIL;
    ret = blk_props(imp_sth->bcp_desc,
		    CS_SET, BLK_IDENTITY,
		    (CS_VOID*)&imp_sth->bcpIdentityFlag,
		    CS_UNUSED, NULL);
    if(ret != CS_SUCCEED)
	goto FAIL;

    ret = blk_init(imp_sth->bcp_desc,
		   CS_BLK_IN, table, strlen(table));
    if(ret != CS_SUCCEED)
	goto FAIL;
    
    num_cols = DBIc_NUM_PARAMS(imp_sth);

    if (DBIS->debug >= 3) {
        PerlIO_printf(DBILOGFP, 
		      "       syb_blk_init(): num_cols=%d, identityFlag=%d\n",
		      num_cols, imp_sth->bcpIdentityFlag);
    }

    imp_sth->numCols = num_cols;
    /*Newz(902, imp_sth->datafmt, num_cols, CS_DATAFMT); */
    Newz(902, imp_sth->coldata, num_cols, ColData);
    for(i = 1; i <= num_cols; ++i) {
	sprintf(name, ":p%d", i);
	svp = hv_fetch(imp_sth->all_params_hv, name, strlen(name), 0);
	phs = ((phs_t*)(void*)SvPVX(*svp));
	memset(&phs->datafmt, 0, sizeof(CS_DATAFMT));
	ret = blk_describe(imp_sth->bcp_desc, i, &phs->datafmt);

	if (DBIS->debug >= 3)
	    PerlIO_printf(DBILOGFP, "    syb_blk_init: blk_describe()==%d col %d, type %d, status %d, length %d\n",
			  ret,
			  i, phs->datafmt.datatype, 
			  phs->datafmt.status, phs->datafmt.maxlength);

	if(ret != CS_SUCCEED)
	    goto FAIL;
    }

 FAIL:;
    if(ret != CS_SUCCEED)
	blkCleanUp(imp_sth);
    else {
	imp_dbh->imp_sth = imp_sth; /* hack! */
	/* Turn off autocommit for this handle, mainly to silence
	   warnings from Sybase.xsi's commit() implementation */
	imp_sth->bcpAutoCommit = DBIc_is(imp_dbh, DBIcf_AutoCommit);
	DBIc_set(imp_dbh, DBIcf_AutoCommit, 0);
    }

    return ret;
}

static void blkCleanUp(imp_sth_t *imp_sth)
{
    int i;

    for(i = 0; i < imp_sth->numCols; ++i)
      if(imp_sth->coldata[i].value.p && imp_sth->coldata[i].v_alloc)
	Safefree(imp_sth->coldata[i].value.p);

    if(imp_sth->coldata)
	Safefree(imp_sth->coldata);
    imp_sth->numCols = 0;
    imp_sth->coldata = NULL;
    imp_sth->datafmt = NULL;

    if(imp_sth->bcp_desc) {
	blk_drop(imp_sth->bcp_desc);
	imp_sth->bcp_desc = NULL;
    }
}

static int getTableName(char *statement, char *table, int maxwidth)
{
    char *ptr = safemalloc(strlen(statement) + 1);
    char *p;

    strcpy(ptr, statement);
    p = strtok(ptr, " ");
    if(!p || !*p || strncasecmp(p, "insert", 7))
	goto FAIL;
    p = strtok(NULL, " (");
    if(!p || !*p)
	goto FAIL;
    if(!strncasecmp(p, "into", 4))
	p = strtok(NULL, " (");
    if(!p || !*p)
	goto FAIL;
    strncpy(table, p, maxwidth);
    safefree(ptr);

    return 1;

    FAIL:
    safefree(ptr);
    return 0;
}

SV *syb_set_cslib_cb(SV *cb)
{
#if 0
    //!defined(USE_CSLIB_CB)
    warn("Can't set a CS-Lib callback: DBD::Sybase was not built with -DUSE_CSLIB_CB");
    return &PL_sv_undef;
#else
    SV *old = cslib_cb;

    if(cslib_cb == (SV*) NULL)
	cslib_cb = newSVsv(cb);
    else
	sv_setsv(cslib_cb, cb);

    return old ? old : &PL_sv_undef;
#endif
}

/* WARNING - dbh passed in here is in some cases NULL */
static int toggle_autocommit(SV *dbh, imp_dbh_t *imp_dbh, int flag)
{
    CS_BOOL    value;
    CS_RETCODE ret;

    if(flag) {
	/* Going from OFF to ON - so force a COMMIT on any open 
	   transaction */
	syb_db_commit(dbh, imp_dbh);
	if(!imp_dbh->doRealTran) {
	    value = CS_FALSE;
	    ret = ct_options(imp_dbh->connection, CS_SET, CS_OPT_CHAINXACTS, 
			     &value, CS_UNUSED, NULL);
	}
    } else {
	if(!imp_dbh->doRealTran) {
	    value = CS_TRUE;
	    ret = ct_options(imp_dbh->connection, CS_SET, CS_OPT_CHAINXACTS, 
			     &value, CS_UNUSED, NULL);
	}
    }
    if(!imp_dbh->doRealTran && ret != CS_SUCCEED) {
	warn("Setting of CS_OPT_CHAINXACTS failed.");
	return FALSE;
    }
    
    return TRUE;
}
