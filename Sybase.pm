# -*-Perl-*-
# $Id: Sybase.pm,v 1.12 1998/11/17 16:11:06 mpeppler Exp $

# Copyright (c) 1996, 1997, 1998   Michael Peppler
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

    $VERSION = '0.12';
    my $Revision = substr(q$Revision: 1.12 $, 10);

    require_version DBI 1.02;

    bootstrap DBD::Sybase $VERSION;


    $drh = undef;	# holds driver handle once initialised
    $err = 0;		# The $DBI::err value
    $errstr = '';
    $sqlstate = "00000";

    sub driver {
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


        my($this) = DBI::_new_dbh($drh, {
	    'Name' => $server,
	    'User' => $user,	
	    'CURRENT_USER' => $user,
	    });

	DBD::Sybase::db::_login($this, $server, $user, $auth) or return undef;

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

    sub tables {
	my $dbh = shift;

	my $sth = $dbh->prepare("select name from sysobjects where type in ('V', 'U')");
	$sth->execute;
	my @names;
	my $dat;
	while($dat = $sth->fetch) {
	    push(@names, $dat->[0]);
	}
	@names;
    }

    sub do {
	my($dbh, $statement, $attr, @params) = @_;
	my $sth = $dbh->prepare($statement, $attr) or return undef;
	$sth->execute(@params) or return undef;
	my $rows = $sth->rows;
#	print STDERR "$rows $sth->{syb_more_results}\n";
	if(defined($sth->{syb_more_results})) {
	    do {
		while(my $dat = $sth->fetch) {
		    # do something intelligent here...
		}
	    } while($sth->{syb_more_results});
	}

	($rows == 0) ? "0E0" : $rows;
    }

    sub table_info {
	my $dbh = shift;

	my $sth = $dbh->prepare("
           select TABLE_QUALIFIER = NULL
                , TABLE_OWNER     = u.name
                , TABLE_NAME      = o.name
                , TABLE_TYPE      = o.type  -- XXX
                , REMARKS         = NULL
             from sysobjects o
                , sysusers   u
            where o.type in ('U', 'V', 'S')
              and o.uid = u.uid");

	$sth->execute;
	$sth;
    }

    sub ping {
	my $dbh = shift;
	my $sth = $dbh->prepare("select * from sysusers where 1=2");
	
	if($sth->execute < 0) {
	    return 0 if(DBD::Sybase::db::_isdead($dbh));
	}
	$sth->finish;
	return 1;
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

=item interfaces

Specify the location of an alternate I<interfaces> file:

     $dbh = DBI->connect("dbi:Sybase:interfaces=/usr/local/sybase/interfaces",
			 $user, $passwd);

=item loginTimeout

Specify the number of seconds that DBI->connect() will wait for a 
response from the Sybase server. If the server fails to respond before the
specified number of seconds the DBI->connect() call fails with a timeout
error. The default value is 60 seconds, which is usually enough, but on a busy
server it is sometimes necessary to increase this value:

     $dbh = DBI->connect("dbi:Sybase:loginTimeout=240", # wait up to 4 minutes
			 $user, $passwd);

=item scriptName

Specify the name for this connection that will be displayed in sp_who
(ie in the sysprocesses table in the I<program_name> column).

    $dbh->DBI->connect("dbi:Sybase:scriptName=myScript", $user, $password);

=item hostname

Specify the hostname that will be displayed by sp_who (and will be stored
in the hostname column of sysprocesses)..

    $dbh->DBI->connect("dbi:Sybase:hostname=kiruna", $user, $password);

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
of each result set, and by setting a special Sybase attribute in $sth
which you can check to see if there is more data to be fetched. The 
attribute is B<syb_more_results> which you should check to see if you
need to re-start the C<fetch()> loop.

To make sure all results are fetched, the basic C<fetch> loop can be 
written like this:

     do {
         while($d = $sth->fetch) {
            ... do something with the data
         }
     } while($sth->{syb_more_results});
     $sth->finish;

You can get the type of the current result set with 
$sth->{syb_result_type}. This returns a numerical value, as defined in 
$SYBASE/include/cspublic.h:

	#define CS_ROW_RESULT		(CS_INT)4040
	#define CS_CURSOR_RESULT	(CS_INT)4041
	#define CS_PARAM_RESULT		(CS_INT)4042
	#define CS_STATUS_RESULT	(CS_INT)4043
	#define CS_MSG_RESULT		(CS_INT)4044
	#define CS_COMPUTE_RESULT	(CS_INT)4045

In particular, the return status of a stored procedure is returned
as CS_STATUS_RESULT (4043), and is normally the last result set that is 
returned in a stored proc execution.

This should be compatible with other DBI drivers.

=head1 Sybase Specific Attributes

There are a number of handle  attributes that are specific to this driver.
These attributes all start with B<syb_> so as to not clash with any
normal DBI attributes.

=head2 Database Handle Attributes

The following Sybase specific attributes can be set at the Database handle
level:

=over 4

=item syb_show_sql

If set then the current statement is included in the string returned by 
$dbh->errstr.

=item syb_show_eed

If set, then extended error information is included in the string returned 
by $dbh->errstr. Extended error information include the index causing a
duplicate insert to fail, for example.

=back

=head2 Statement Handle Attributes

The following read-only attributes are available at the statement level:

=over 4

=item syb_more_results

See the discussion on handling multiple result sets above.

=item syb_result_type

Returns the numeric result type of the current result set. Useful when 
executing stored procedurs to determine what type of information is
currently fetchable (normal select rows, output parameters, status results,\
etc...).

=back

=head1 Controlling DATETIME output formats

By default DBD::Sybase will return I<DATETIME> and I<SMALLDATETIME>
columns in the I<Nov 15 1998 11:13AM> format. This can be changed
via a special B<_date_fmt()> function that is accessed via the $dbh->func()
method.

The syntax is

    $dbh->func($fmt, '_date_fmt');

where $fmt is a string representing the format that you want to apply.

The formats are based on Sybase's standard conversion routines. The following
subset of available formats has been implemented:

=over 4

=item LONG

Nov 15 1998 11:30:11:496AM

=item SHORT

Nov 15 1998 11:30AM

=item DMY4_YYYY

15 Nov 1998

=item MDY1_YYYY

11/15/1998

=item DMY1_YYYY

15/11/1998

=item HMS

11:30:11

=back

=head1 Multiple active statements on one $dbh

It is now possible to open multiple active statements on a single database 
handle. This is done by openeing a new physical connection in $dbh->prepare()
if there is already an active statement handle for this $dbh.

This feature has been implemented to improve compatibility with other
drivers, but should not be used if you are coding directly to the 
Sybase driver.

B<WARNING>: This feature should be used with care. In particular, because
the SQL statements executed on the second (and subsequent) statement
handles are sent over a different physical connection DBD::Sybase
cannot guarantee a complete rollback if you have B<AutoCommit> set to B<OFF>.

A future version may make a better attempt at getting this particular 
problem right.


=head1 IMAGE and TEXT datatypes

DBD::Sybase uses the standard OpenClient conversion routines to convert
data retrieved from the server into either string or numeric format.

The conversion routines convert IMAGE datatypes to a hexadecimal string.
If you need the binary representation you can use something like

    $binary = pack("H*", $hex_string);

to do the conversion. Note that TEXT columns are not treated this way
and will be returned exactly as they were stored. Internally Sybase
makes no distinction between TEXT and IMAGE columns - both can be
used to store either text or binary data.



=head1 Transactions and Transact-SQL

When $h->{AutoCommit} is I<off> (ie I<0>) the DBD::Sybase driver
will send a B<BEGIN TRAN> before the first $dbh->prepare(), and
after each call to $dbh->commit() or $dbh->rollback(). This works
fine, but will cause any SQL that contains any I<CREATE TABLE>
statements to fail. These I<CREATE TABLE> statements can be
burried in a stored procedure somewhere (for example,
C<sp_helprotect> creates two templ tables when it is run).

If you absolutely want to have manual commits (ie have
B<AutoCommit> set to 0) and be able to run any arbitrary SQL, then
you can use C<sp_dboption> to set the C<ddl in tran> option to C<TRUE>.
However, the Sybase documentation warns that this can cause the system
to seriouslys slow down as this causes locks to be set on certain
system tables, and these locks will be held for the duration of the 
transaction.

=head1 Using ? Placeholders & bind parameters to $sth->execute

This version supports the use of ? placeholders in SQL statements. It does 
this by using what Sybase calls I<Dynamic SQL>. The ? placeholders allow
you to write something like:

	$sth = $dbh->prepare("select * from employee where empno = ?");

        # Retrieve rows from employee where empno == 1024:
	$sth->execute(1024);
	while($data = $sth->fetch) {
	    print "@$data\n";
	}

       # Now get rows where empno = 2000:
	
	$sth->execute(2000);
	while($data = $sth->fetch) {
	    print "@$data\n";
	}

When you use ? placeholders Sybase goes and creates a temporary stored 
procedure that corresponds to your SQL statement. You then pass variables
to $sth->execute or $dbh->do, which get inserted in the query, and any rows
are returned.

For those of you who are used to Transact-SQL there are some limitations
to using this feature: In particular you can only pass a simple I<exec proc>
call, or a simple I<select> statement (ie a statement that only returns a
single result set). In addition, the ? placeholders can only appear in a 
B<WHERE> clause, in the B<SET> clause of an B<UPDATE> statement, or in the
B<VALUES> list of an B<INSERT> statement. In particular you can't pass ?
as a parameter to a stored procedure.

Please see the discussion on Dynamic SQL in the 
OpenClient C Programmer's Guide for details. The guide is available on-line
at http://sybooks.sybase.com/dynaweb.


=head1 BUGS

Setting $dbh->{LongReadLen} has no effect. Use $dbh->do("set textsize xxxx")
instead.

You can't set a particular database via the connect() call. Use
$dbh->do("use $database") instead.

You can run out of space in the tempdb database if you use a lot of
calls with bind variables (ie ? style placeholders) without closing the
connection. On my system, with an 8 MB tempdb database I run out of space
after 760 prepare() statements with ? parameters. This is because
Sybase creates stored procedures for each prepare() call. So my
suggestion is to only use ? style placeholders if you really need them
(i.e. if you are going to execute the same prepared statement multiple
times).


=head1 SEE ALSO

L<DBI>
Sybase OpenClient C manuals.
Sybase Transact SQL manuals.

=head1 AUTHOR

DBD::Sybase by Michael Peppler

=head1 COPYRIGHT

The DBD::Sybase module is Copyright (c) 1997, 1998 Michael Peppler.
The DBD::Sybase module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself with the exception that it
cannot be placed on a CD-ROM or similar media for commercial distribution
without the prior approval of the author.

=head1 ACKNOWLEDGEMENTS

Tim Bunce for DBI, obviously!

See also L<DBI/ACKNOWLEDGEMENTS>.

=cut
