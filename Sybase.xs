/* -*-C-*- */

/* $Id: Sybase.xs,v 1.2 1997/09/26 21:28:18 mpeppler Exp $
   Copyright (c) 1997 Michael Peppler

   Uses from Driver.xst
   Copyright (c) 1994,1995,1996,1997  Tim Bunce

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.

*/

#include "Sybase.h"

DBISTATE_DECLARE;


MODULE = DBD::Sybase	PACKAGE = DBD::Sybase

INCLUDE: Sybase.xsi
