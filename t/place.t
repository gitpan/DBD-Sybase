#!perl
#
# $Id: place.t,v 1.7 2004/12/16 12:06:01 mpeppler Exp $

use lib 't';
use _test;

use strict;

use Test::More tests => 13; #qw(no_plan);

BEGIN { use_ok('DBI');
        use_ok('DBD::Sybase');}


my ($Uid, $Pwd, $Srv, $Db) = _test::get_info();

my $dbh = DBI->connect("dbi:Sybase:server=$Srv;database=$Db", $Uid, $Pwd, {PrintError => 0});

ok($dbh, 'Connect');

SKIP: {
    skip "?-style placeholders aren't supported with this SQL Server", 10 unless $dbh->{syb_dynamic_supported};

    my $rc;

    $rc = $dbh->do("create table #t(string varchar(20), date datetime, val float, other_val numeric(9,3))");
    ok($rc, 'Create table');

    my $sth = $dbh->prepare("insert #t values(?, ?, ?, ?)");
    ok($sth, 'prepare');
    
    $rc = $sth->execute("test", "Jan 3 1998", 123.4, 222.3334);
    ok($rc, 'insert 1');

    $rc = $sth->execute("other test", "Jan 25 1998", 4445123.4, 2);
    ok($rc, 'insert 2');

    $rc = $sth->execute("test", "Feb 30 1998", 123.4, 222.3334);
    ok(!$rc, 'insert 3 (fail)');

    $sth = $dbh->prepare("select * from #t where date > ? and val > ?");
    ok($sth, 'prepare 2');

    $rc = $sth->execute('Jan 1 1998', 120);
    ok($rc, 'select');
    my $row;
    my $count = 0;

    while($row = $sth->fetch) {
	print "@$row\n";
	++$count;
    }

    ok($count == 2, 'fetch count');

    $rc = $sth->execute('Jan 1 1998', 140);
    ok($rc, 'select 2');

    $count = 0;

    while($row = $sth->fetch) {
	print "@$row\n";
	++$count;
    }

    ok($count == 1, 'fetch 2');
}
$dbh->disconnect;

exit(0);
