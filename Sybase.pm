# -*-Perl-*-
# $Id: Sybase.pm,v 1.3 1997/10/07 00:51:57 mpeppler Exp $

# Copyright (c) 1996, 1997   Michael Peppler
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.
#
# Based on DBD::Oracle Copyright (c) 1994,1995,1996,1997 Tim Bunce

{
    package DBD::Sybase;

    use DBI ();
    use DynaLoader ();
    @ISA = qw(DynaLoader);

    $VERSION = '0.04';
    my $Revision = substr(q$Revision: 1.3 $, 10);

    require_version DBI 0.89;

    bootstrap DBD::Sybase $VERSION;


    $drh = undef;	# holds driver handle once initialised
    $err = 0;		# The $DBI::err value
    $errstr = '';
    $sqlstate = "00000";

    sub driver{
	return $drh if $drh;
	my($class, $attr) = @_;
	$class .= "::dr";
	($drh) = DBI::_new_drh($class, {
	    'Name' => 'Sybase',
	    'Version' => $VERSION,
	    'Err'     => \$DBD::Sybase::err,
	    'Errstr'  => \$DBD::Sybase::errstr,
	    'State'   => \$DBD::Sybase::sqlstate,
	    'Attribution' => 'Sybase DBD by Michael Peppler',
	    });
	$drh;
    }


    1;
}


{   package DBD::Sybase::dr; # ====== DRIVER ======
    use strict;
    
    sub connect { 
        my($drh, $dbase, $user, $auth, $attr)= @_;
	my $ifile = '';
	my $server = $dbase || $ENV{DSQUERY} || 'SYBASE';

	# The default according to the DBI spec is to set AutoCommit
	# to on.
	if(!exists($attr->{AutoCommit})) {
	    $attr->{AutoCommit} = 1;
	}

	# If the interfaces file is located in a non-standard place:
#	if($attr->{IFILE}) {
#	    ct_config(CS_GET, CS_IFILE, $ifile);
#	    ct_config(CS_SET, CS_IFILE, $attr->{IFILE});
#	}
	# If the server name is set in $attr
        my($this) = DBI::_new_dbh($drh, {
	    'Name' => $server,
	    'User' => $user,
	    });

	DBD::Sybase::db::_login($this, $server, $user, $auth);

	# Reset the interfaces file location if necessary
#	if($ifile ne '') {
#	    ct_config(CS_SET, CS_IFILE, $ifile);
#	}

	$this;
    }
}


{   package DBD::Sybase::db; # ====== DATABASE ======
    use strict;

    sub prepare {
	my($dbh, $statement, @attribs)= @_;

	# create a 'blank' sth

	my $sth = DBI::_new_sth($dbh, {
	    'Statement' => $statement,
	    });


	DBD::Sybase::st::_prepare($sth, $statement, @attribs)
	    or return undef;

	$sth;
    }
}


{   package DBD::Sybase::st; # ====== STATEMENT ======
    use strict;
}

1;
