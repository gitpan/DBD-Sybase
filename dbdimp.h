/*
   $Id: dbdimp.h,v 1.2 1997/09/26 23:26:47 mpeppler Exp $

   Copyright (c) 1997  Michael Peppler

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.

   Based on DBD::Oracle dbdimp.h, Copyright (c) 1994,1995 Tim Bunce

*/

typedef struct imp_fbh_st imp_fbh_t;

/*
** Maximum character buffer for displaying a column
*/
#define MAX_CHAR_BUF	1024


typedef struct _col_data
{
    CS_SMALLINT	indicator;
    CS_INT	type;
    CS_INT      realType;
    union {
	CS_CHAR	*c;
	CS_INT i;
	CS_FLOAT f;
/*	CS_DATETIME dt;
	CS_MONEY mn;
	CS_NUMERIC num; */
    } value;
    CS_INT	valuelen;
} ColData;


struct imp_drh_st {
    dbih_drc_t com;		/* MUST be first element in structure	*/
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
   dbih_dbc_t com;		/* MUST be first element in structure	*/

   CS_CONNECTION *connection;
   char tranName[32];
   int inTransaction;   
};


/* Define sth implementor data structure */
struct imp_sth_st {
    dbih_stc_t com;		/* MUST be first element in structure	*/

    CS_COMMAND *cmd;
    ColData    *coldata;
    CS_DATAFMT *datafmt;
    int         numCols;
    CS_INT      lastResType;
    CS_INT      numRows;
    int         moreResults;

    /* Input Details	*/
    char      *statement;	/* sql (see sth_scan)		*/
    HV        *all_params_hv;	/* all params, keyed by name	*/
    AV        *out_params_av;	/* quick access to inout params	*/
    int        syb_pad_empty;	/* convert ""->" " when binding	*/

    /* Select Column Output Details	*/
    int        done_desc;   /* have we described this sth yet ?	*/

    /* (In/)Out Parameter Details */
    int  has_inout_params;
};
#define IMP_STH_EXECUTING	0x0001
