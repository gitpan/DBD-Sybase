/* -*-C-*- */

/* $Id: Sybase.xs,v 1.5 1999/05/16 18:29:21 mpeppler Exp $
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



MODULE = DBD::Sybase	PACKAGE = DBD::Sybase

INCLUDE: Sybase.xsi
