#!perl
#
# $Id: login.t,v 1.3 2004/12/16 12:06:01 mpeppler Exp $

use lib 'blib/lib';
use lib 'blib/arch';

use lib 't';
use _test;

use strict;

use Test::More tests => 4;

use vars qw($Pwd $Uid $Srv $Db);

BEGIN { use_ok('DBI');
        use_ok('DBD::Sybase');}

($Uid, $Pwd, $Srv, $Db) = _test::get_info();

#DBI->trace(3);
my $dbh = DBI->connect("dbi:Sybase:server=$Srv;database=$Db", $Uid, $Pwd, {PrintError => 1});
#DBI->trace(0);
ok(defined($dbh), 'Connect');

$dbh->disconnect if $dbh;

$dbh = DBI->connect("dbi:Sybase:server=$Srv;database=$Db", 'ohmygod', 'xzyzzy', {PrintError => 0});

ok(!defined($dbh), 'Connect fail');

$dbh->disconnect if $dbh;

exit(0);
