#!/usr/local/bin/perl
#
# $Id: login.t,v 1.2 2003/09/08 21:30:22 mpeppler Exp $

use lib 'blib/lib';
use lib 'blib/arch';

use lib 't';
use _test;

use vars qw($Pwd $Uid);

BEGIN {print "1..3\n";}
END {print "not ok 1\n" unless $loaded;}
use DBI;
$loaded = 1;
print "ok 1\n";

($Uid, $Pwd, $Srv, $Db) = _test::get_info();

my $dbh = DBI->connect("dbi:Sybase:server=$Srv;database=$Db", $Uid, $Pwd, {PrintError => 0});

$dbh and print "ok 2\n"
    or print "not ok 2\n";

$dbh->disconnect if $dbh;

$dbh = DBI->connect("dbi:Sybase:server=$Srv;database=$Db", 'ohmygod', 'xzyzzy', {PrintError => 0});

$dbh and print "not ok 3\n"
    or print "ok 3\n";

$dbh->disconnect if $dbh;

exit(0);
