#!/usr/local/bin/perl
#	@(#)main.t	1.1	2/2/96

# Base DBD Driver Test

BEGIN {print "1..8\n";}
END {print "not ok 1\n" unless $loaded;}
use DBI;
$loaded = 1;
print "ok 1\n";

# Find the passwd file:
@dirs = ('./.', './..', './../..', './../../..');
foreach (@dirs)
{
    if(-f "$_/PWD")
    {
	open(PWD, "$_/PWD") || die "$_/PWD is not readable: $!\n";
	while(<PWD>)
	{
	    chop;
	    s/^\s*//;
	    next if(/^\#/ || /^\s*$/);
	    ($l, $r) = split(/=/);
	    $Uid = $r if($l eq UID);
	    $Pwd = $r if($l eq PWD);
	    $Srv = $r if($l eq SRV);
	}
	close(PWD);
	last;
    }
}

my($switch) = DBI->internal;
$switch->debug(0); # 2=detailed handle trace

print "Switch: $switch->{'Attribution'}, $switch->{'Version'}\n";

$switch->{'DebugDispatch'} = 0; # 2=detailed trace of all dispatching
print "DebugDispatch: $switch->{'DebugDispatch'}\n";

print "Available Drivers: ",join(", ",DBI->available_drivers()),"\n";

my($dbh);
my($drh) = DBI->install_driver('Sybase');
print "Driver installed as $drh\n";
$dbh = $drh->connect($Srv, $Uid, $Pwd);

die "Unable for connect to $Srv: $DBI::errstr"
    unless $dbh;

($dbh->do("use master"))
    and print "ok 2\n"
    or print "not ok 2\n";

($sth = $dbh->prepare("select * from sysusers"))
    and print "ok 3\n"
    or print "not ok 3\n";
if($sth->execute) {
    print "ok 4\n";
    my $rows = 0;
    while(@dat = $sth->fetchrow) {
	++$rows;
	foreach (@dat) {
	    $_ = '' unless $_;
	}
	print "@dat\n";
    }
    ($rows == $sth->{CTlib}->{ROW_COUNT})
	and print "ok 5\n"
	    or print "not ok 5\n";
    $sth->finish;
}
else {
    print STDERR ($DBI::err, ":\n", $sth->errstr);
    print "not ok 4\nnot ok 5\n";
}
($sth = $dbh->prepare("select * from sys_users"))
    and print "ok 6\n"
    or print "not ok 6\n";
if($sth->execute) {
    print "not ok 7\n";		# SHOULD FAIL!!!

    while(@dat = $sth->fetchrow) {
	print "@dat\n";
    }
    $sth->finish;
}
else {
    print "ok 7\n";
    ($DBI::err == 208)
	and print "ok 8\n"
	    or print "not ok 8\n";
#    print STDERR ($DBI::err, ":\n", $sth->errstr);
}


