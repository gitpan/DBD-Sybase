# -*-Perl-*-
# $Id: Sybase.pm,v 1.4 1997/10/31 19:02:23 mpeppler Exp $

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

    $VERSION = '0.05';
    my $Revision = substr(q$Revision: 1.4 $, 10);

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
	if(!defined($attr) || ref($attr) ne 'HASH') {
	    $attr = {};
	}
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

	DBD::Sybase::db::_login($this, $server, $user, $auth) or return undef;

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

__END__

=head1 NAME

DBD::Sybase - Sybase database driver for the DBI module

=head1 SYNOPSIS

    use DBI;

    $dbh = DBI->connect("dbi:Sybase:", $user, $passwd);

    # See the DBI module documentation for full details

=head1 DESCRIPTION

DBD::Sybase is a Perl module which works with the DBI module to provide
access to Sybase databases.

=head1 Connecting to Sybase

=head2 The interfaces file

The DBD::Sybase module is built on top of the Sybase I<Open Client Client 
Library> API. This library makes use of the Sybase I<interfaces> file
(I<sql.ini> on Win32 machines) to make a link between a logical
server name (e.g. SYBASE) and the physical machine / port number that
the server is running on. The OpenClient library uses the environment
variable B<SYBASE> to find the location of the I<interfaces> file,
as well as other files that it needs (such as locale files). The B<SYBASE>
environment is the path to the Sybase installation (eg '/usr/local/sybase').
If you need to set it in your scripts, then you I<must> set it in a
C<BEGIN{}> block:

   BEGIN {
       $ENV{SYBASE} = '/opt/sybase/11.0.2';
   }

   $dbh = DBI->connect('dbi:Sybase', $user, $passwd);


=head2 Specifying the server name

The server that DBD::Sybase connects to defaults to I<SYBASE>, but
can be specified in two ways.

You can set the I<DSQUERY> environement variable:

    $ENV{DSQUERY} = "ENGINEERING";
    $dbh = DBI->connect('dbi:Sybase:', $user, $passwd);

Or you can pass the server name in the first argument to connect():

    $dbh = DBI->connect("dbi:Sybase:server=ENGINEERING", $user, $passwd);

=head2 Specifying other connection specific parameters

It is sometimes necessary (or beneficial) to specify other connection
properties. Currently the following are supported:

=over 4

=item charset

Specify the character set that the client uses.

     $dbh = DBI->connect("dbi:Sybase:charset=iso_1",
			 $user, $passwd);

=item language

Specify the language that the client uses.

     $dbh = DBI->connect("dbi:Sybase:language=us_english",
			 $user, $passwd);

=item packetSize

Specify the network packet size that the connection should use. Using a
larger packet size can increase performance for certain types of queries.
See the Sybase documentation on how to enable this feature on the server.

     $dbh = DBI->connect("dbi:Sybase:packetSize=8192",
			 $user, $passwd);

=back

These different parameters (as well as the server name) can be strung
together by separating each entry with a semi-colon:

    $dbh = DBI->connect("dbi:Sybase:server=ENGINEERING;packetSize=8192;language=us_english;charset=iso_1",
			$user, $pwd);

=head1 Handling Multiple Result Sets

Sybase's Transact SQL has the ability to return multiple result sets
from a single SQL statement. For example the query:

    select b.title, b.author, s.amount
      from books b, sales s
     where s.authorID = b.authorID
     order by b.author, b.title
    compute sum(s.amount) by b.author

which lists sales by author and title and also computes the total sales
by author returns two types of rows. The DBI spec doesn't really 
handle this situation, nor the more hairy

    exec my_proc @p1='this', @p2='that', @p3 out

where C<my_proc> could return any number of result sets (ie it could
perform an unknown number of C<select> statements.

I've decided to handle this by returning an empty row at the end
of each result set - but it you suspect that there may be more data
from your current query, then you can call any of the C<fetch> routines
again, and they will return the first row in the next result set if there
is one, or an empty row it this B<really> was the last row.

=head1 BUGS

Placeholders (ie setting up a query with '?' parameters to be filled
in later) are not supported at the moment, so $sth->bind_param() is 
not implemented, the @bind_values argument to $sth->execute is ignored,
and the %attr and @bind_values parameters to $dbh->do are also
ignored. Placeholders will be implemented in a future version.

=head1 SEE ALSO

L<DBI>

=head1 AUTHOR

DBD::Sybase by Michael Peppler

=head1 COPYRIGHT

The DBD::Sybase module is Copyright (c) 1997 Michael Peppler.
The DBD::Sybase module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself with the exception that it
cannot be placed on a CD-ROM or similar media for commercial distribution
without the prior approval of the author.

=head1 ACKNOWLEDGEMENTS

Tim Bunce for DBI, obviously.

See also L<DBI/ACKNOWLEDGEMENTS>.

=cut
