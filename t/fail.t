#!/usr/local/bin/perl
#
# $Id: fail.t,v 1.7 2004/10/02 08:38:37 mpeppler Exp $

use lib 'blib/lib';
use lib 'blib/arch';

use lib 't';
use _test;

BEGIN {print "1..13\n";}
END {print "not ok 1\n" unless $loaded;}
use DBI;
$loaded = 1;
print "ok 1\n";

($Uid, $Pwd, $Srv, $Db) = _test::get_info();

my $dbh = DBI->connect("dbi:Sybase:server=$Srv;database=$Db", $Uid, $Pwd, {PrintError => 0, syb_flush_finish => 1});

die "Unable for connect to $Srv: $DBI::errstr"
    unless $dbh;

my $rc;
#DBI->trace(4);
my $sth = $dbh->prepare("
select * from sysusers
select * from no_such_table
select * from master..sysdatabases
");
$rc = $sth->execute;

defined($rc) and print "not ok 2\n"
    or print "ok 2\n";
$sth = $dbh->prepare("select * from sysusers\n");
$rc = $sth->execute;
defined($rc) and print "ok 3\n"
    or print "not ok 3\n";
while(my $d = $sth->fetch) {
    ;
}
print "ok 4\n";
$rc = $dbh->do("create table #test(one int not null primary key, two int not null, three int not null check(two != three))");
defined($rc) and print "ok 5\n"
    or print "not ok 5\n";

if($dbh->{syb_dynamic_supported}) {

    $sth = $dbh->prepare("insert #test (one, two, three) values(?,?,?)");
    $rc = $sth->execute(3, 4, 5);
    defined($rc) and print "ok 6\n"
	or print "not ok 6\n";
    $rc = $sth->execute(3, 4, 5);
    defined($rc) and print "not ok 7\n"
	or print "ok 7\n";
    $rc = $sth->execute(5, 3, 3);
    defined($rc) and print "not ok 8\n"
	or print "ok 8\n";
} else {
    for (6 .. 8) {
	print "ok $_ # skip\n";
    }
}

$sth = $dbh->prepare("
insert #test(one, two, three) values (1, 2, 3)
insert #test(one, two, three) values (4, 5, 6)
insert #test(one, two, three) values (1, 2, 3)
insert #test(one, two, three) values (8, 9, 10)
");
$rc = $sth->execute;
defined($rc) and print "not ok 9\n"
    or print "ok 9\n";

$sth = $dbh->prepare("select * from #test");
$rc = $sth->execute;
defined($rc) and print "ok 10\n"
    or print "not ok 10\n";

while(my $d = $sth->fetch) {
    print "@$d\n";
}
print "ok 11\n";


$sth = $dbh->prepare("
insert #test(one, two, three) values (11, 12, 13)
select * from #test
insert #test(one, two, three) values (11, 12, 13)
");
$rc = $sth->execute;
defined($rc) and print "ok 12\n"
    or print "not ok 12\n";
do {
    while(my $d = $sth->fetch) {
	print "@$d\n";
    }
} while($sth->{syb_more_results});
print "ok 13\n";

$dbh->do("drop table #test");



