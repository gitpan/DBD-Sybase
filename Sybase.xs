/* -*-C-*- */

/* $Id: Sybase.xs,v 1.7 1999/10/01 17:29:56 mpeppler Exp $
   Copyright (c) 1997-1999 Michael Peppler

   Uses from Driver.xst
   Copyright (c) 1994,1995,1996,1997  Tim Bunce

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.

*/

#include "Sybase.h"

DBISTATE_DECLARE;

MODULE = DBD::Sybase    PACKAGE = DBD::Sybase

I32
constant()
    PROTOTYPE:
    ALIAS:
    CS_ROW_RESULT           = 4040
    CS_CURSOR_RESULT        = 4041
    CS_PARAM_RESULT         = 4042
    CS_STATUS_RESULT        = 4043
    CS_MSG_RESULT           = 4044
    CS_COMPUTE_RESULT       = 4045
    CODE:
    if (!ix) {
	char *what = GvNAME(CvGV(cv));
	croak("Unknown DBD::Sybase constant '%s'", what);
    }
    else RETVAL = ix;
    OUTPUT:
    RETVAL


void
timeout(value)
    int		value
    CODE:
    ST(0) = sv_2mortal(newSViv(syb_set_timeout(value)));


MODULE = DBD::Sybase    PACKAGE = DBD::Sybase::db

void
_isdead(dbh)
    SV *	dbh
    CODE:
    D_imp_dbh(dbh);
    ST(0) = sv_2mortal(newSViv(imp_dbh->isDead));

void
_date_fmt(dbh, fmt)
    SV *	dbh
    char *	fmt
    CODE:
    D_imp_dbh(dbh);
    ST(0) = syb_db_date_fmt(dbh, imp_dbh, fmt) ? &sv_yes : &sv_no;

MODULE = DBD::Sybase    PACKAGE = DBD::Sybase::st

void
cancel(sth)
    SV *	sth
    CODE:
    D_imp_sth(sth);
    ST(0) = syb_st_cancel(sth, imp_sth) ? &sv_yes : &sv_no;


MODULE = DBD::Sybase	PACKAGE = DBD::Sybase

INCLUDE: Sybase.xsi
