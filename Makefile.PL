# $Id: Makefile.PL,v 1.8 1999/05/16 18:26:56 mpeppler Exp $

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

my $lddlflags = $Config{lddlflags};
$lddlflags = "-L$SYBASE/lib $lddlflags" unless $^O eq 'VMS';
my $ldflags = $Config{ldflags};
$ldflags = "-L$SYBASE/lib $ldflags" unless $^O eq 'VMS';


WriteMakefile('NAME'         => 'DBD::Sybase',
	      LIBS           => [$lib_string],
	      INC            => $inc_string,
	      clean	     => { FILES=> 'Sybase.xsi' },
	      OBJECT         => '$(O_FILES)',
	      'VERSION_FROM' => 'Sybase.pm',
	      'LDDLFLAGS'    => $lddlflags,
	      'LDFLAGS'      => $ldflags,
	      LINKTYPE       => $LINKTYPE,
	      MAN3PODS       => { 'Sybase.pm' => 'blib/man3/DBD::Sybase.3' },
	      ($] >= 5.005 ?
	       (ABSTRACT   => 'DBI driver for Sybase datasources',
		AUTHOR     => 'Michael Peppler (mpeppler@peppler.org)') : ()),
	      ($] >= 5.005 && $^O eq 'MSWin32' && 
	       $Config{archname} =~ /-object\b/i ? (CAPI => 'TRUE') :()),
	      'dist' => {'TARFLAGS' => 'cvf', 'COMPRESS' => 'gzip'},
	      ($^O eq 'VMS' ? (PM     => 'Sybase.pm') :())
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
    if($attr{SYBASE} && -d $attr{SYBASE}) {
	$SYBASE = $attr{SYBASE}; 
    } else {
	$SYBASE = $ENV{SYBASE};
    }
    if(!$SYBASE || $SYBASE =~ /^\s*$/) {
	die "Please set SYBASE in CONFIG, or set the \$SYBASE environment variable";
    }


    $SYBASE = VMS::Filespec::unixify($SYBASE) if $^O eq 'VMS';

    if($^O eq 'MSWin32') {
	$lib_string = "-L$SYBASE/lib -llibct.lib -llibcs.lib -llibtcl.lib -llibcomn.lib -llibintl.lib $attr{EXTRA_LIBS} -lm";
    } elsif($^O eq 'VMS') {
	$lib_string = "-L$SYBASE/lib -llibct.olb -llibcs.olb -llibtcl.olb -llibcomn.olb -llibintl.olb $attr{EXTRA_LIBS}";
    } else {
	$lib_string = "-L$SYBASE/lib -lct -lcs -ltcl -lcomn -lintl $attr{EXTRA_LIBS} -lm";
    }
    
    my $config_sitearch = $Config{sitearchexp};
    my $attr_dbi_include = $attr{DBI_INCLUDE};
    if ($^O eq 'VMS') {
    	$config_sitearch = VMS::Filespec::unixify($config_sitearch);
    	$attr_dbi_include = VMS::Filespec::unixify($attr_dbi_include);
    }
    my @try = ($Config{sitearchexp}, @INC);
    unshift @try, $attr{DBI_INCLUDE} if $attr{DBI_INCLUDE};
    my $dbidir;
    for my $trydir (@try) {
	if (-e "$trydir/auto/DBI/DBIXS.h") {
	    $dbidir = "$trydir/auto/DBI";
	    last;
 	}
     }
    die "Can't find the DBI include files. Please set DBI_INCLUDE in CONFIG"
	if !$dbidir;
    $inc_string = "-I$SYBASE/include -I$dbidir";
    if($attr{LINKTYPE}) {
	$LINKTYPE = $attr{LINKTYPE}
    }
}

    
	
