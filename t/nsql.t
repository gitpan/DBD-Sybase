#!/usr/local/bin/perl
#
# $Id: nsql.t,v 1.3 2004/03/16 23:35:31 mpeppler Exp $

use lib 'blib/lib';
use lib 'blib/arch';

use lib 't';
use _test;

use vars qw($Pwd $Uid $Srv);

BEGIN {print "1..6\n";}
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

@d = $dbh->func("select * from sysusers", 'ARRAY', \&cb, 'nsql');
foreach (@d) {
    print "@$_\n";
}
print "ok 4\n";

if($DBI::VERSION >= 1.34) {
    @d = $dbh->syb_nsql("select * from sysusers", 'ARRAY');
    foreach (@d) {
	local $^W = 0;
	print "@$_\n";
    }
    print "ok 5\n";

    @d = $dbh->syb_nsql("select * from sysusers", 'ARRAY', \&cb);
    foreach (@d) {
	print "@$_\n";
    }
    print "ok 6\n";
} else {
    print "ok 5 # skip\n";
    print "ok 6 # skip\n";
}

sub cb {
    my @data = @_;
    local $^W = 0;
    print "@data\n";

    1;
}
