# $Id: Makefile.PL,v 1.3 1997/10/07 00:49:35 mpeppler Exp $

use ExtUtils::MakeMaker;

require 5.004;

use strict;

# If either of these aren't available on your system then you need to
# get them!
use DBI;
use DBI::DBD;
use Config;

use vars qw($SYBASE $inc_string $lib_string $LINKTYPE);

$LINKTYPE = 'dynamic';

configure();

#$lib_string = "-L$SYBASE/lib -lct -lcs -ltcl -lcomn -lintl $$sattr{EXTRA_LIBS} -lm";
#$inc_string = "-I$SYBASE/include -I$Config{sitearchexp}/DBI";


WriteMakefile('NAME' => 'DBD::Sybase',
	      LIBS => [$lib_string],
	      INC => $inc_string,
	      clean	=> { FILES=> 'Sybase.xsi' },
	      OBJECT => '$(O_FILES)',
	      'VERSION_FROM' => 'Sybase.pm',
		LINKTYPE => $LINKTYPE,
	      'dist' => {'TARFLAGS' => 'cvfF', 'COMPRESS' => 'gzip'},
	      );

sub MY::postamble {
    return dbd_postamble();
}
	      

sub configure {
    my %attr;
    my ($key, $val);
    open(IN, "CONFIG") || die "Can't open CONFIG: $!";
    while(<IN>) {
	chomp;
	next if /^\s*\#/;
	next if /^\s*$/;
	
	($key, $val) = split(/\s*=\s*/, $_);
	$key =~ s/\s//g;
	$val =~ s/\s*$//;

	$attr{$key} = $val;
    }
    $SYBASE = $attr{SYBASE} || $ENV{SYBASE};
    if(!$SYBASE || $SYBASE =~ /^\s*$/) {
	die "Please set SYBASE in CONFIG, or set the \$SYBASE environment variable";
    }
    
    $lib_string = "-L$SYBASE/lib -lct -lcs -ltcl -lcomn -lintl $attr{EXTRA_LIBS} -lm";
    
    my $dbidir = $attr{DBI_INCLUDE} || "$Config{sitearchexp}/auto/DBI";
    if(!-e "$dbidir/DBIXS.h") {
	$dbidir = "$Config{sitearchexp}/DBI";
	if(!-e "$dbidir/DBIXS.h") {
	    die "Can't find the DBI include files. Please set DBI_INCLUDE in CONFIG";
	}
    }
    $inc_string = "-I$SYBASE/include -I$dbidir";
    if($attr{LINKTYPE}) {
	$LINKTYPE = $attr{LINKTYPE}
    }
}

    
	
