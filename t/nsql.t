#!/usr/local/bin/perl
#
# $Id: nsql.t,v 1.2 2003/09/08 21:30:22 mpeppler Exp $

use lib 'blib/lib';
use lib 'blib/arch';

use lib 't';
use _test;

use vars qw($Pwd $Uid $Srv);

BEGIN {print "1..4\n";}
END {print "not ok 1\n" unless $loaded;}
use DBI;
$loaded = 1;
print "ok 1\n";

#DBI->trace(2);

($Uid, $Pwd, $Srv, $Db) = _test::get_info();

#DBI->trace(3);
my $dbh = DBI->connect("dbi:Sybase:server=$Srv;database=$Db", $Uid, $Pwd, {syb_deadlock_retry=>10, syb_deadlock_verbose=>1});
#exit;
$dbh and print "ok 2\n"
    or print "not ok 2\n";

my @d = $dbh->func("select * from sysusers", 'ARRAY', 'nsql');
foreach (@d) {
    local $^W = 0;
    print "@$_\n";
}
print "ok 3\n";


sub cb {
    my @data = @_;
    local $^W = 0;
    print "@data\n";

    1;
}
@d = $dbh->func("select * from sysusers", 'ARRAY', \&cb, 'nsql');
foreach (@d) {
    print "@$_\n";
}
print "ok 4\n";
