# $Id: Makefile.PL,v 1.9 1999/05/17 05:46:11 mpeppler Exp $

use ExtUtils::MakeMaker;

require 5.004;

use strict;

# If either of these aren't available on your system then you need to
# get them!
use DBI;
use DBI::DBD;
use Config;
use Getopt::Long;

use vars qw($SYBASE $inc_string $lib_string $LINKTYPE);

$LINKTYPE = 'dynamic';

my $file;
GetOptions('--file' => \$file);

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

    my $sybase_dir = $ENV{SYBASE};

    if(!$sybase_dir) {
	$sybase_dir = (getpwname('sybase'))[7];
    }

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
	$SYBASE = $sybase_dir;
    }
    if(!$SYBASE || $SYBASE =~ /^\s*$/) {
	die "Please set SYBASE in CONFIG, or set the \$SYBASE environment variable";
    }

    if(! -d "$SYBASE/lib" || !-d "$SYBASE/include") {
	die "Can't find the lib or include directories under $SYBASE!";
    }

    my $test_file = "$SYBASE/lib/libct.$Config{so}";
    if($^O eq 'MSWin32') {
	$test_file = "$SYBASE/lib/libct.lib"; 
    } elsif($^O eq 'VMS') {
	$test_file = "$SYBASE/lib/libct.olb"; 
    }
	
    die "Can't find any Sybase libraries in $SYBASE/lib" if(!-e $test_file);


    $SYBASE = VMS::Filespec::unixify($SYBASE) if $^O eq 'VMS';

    if($^O eq 'MSWin32') {
	$lib_string = "-L$SYBASE/lib -llibct.lib -llibcs.lib -llibtcl.lib -llibcomn.lib -llibintl.lib $attr{EXTRA_LIBS} -lm";
    } elsif($^O eq 'VMS') {
	$lib_string = "-L$SYBASE/lib -llibct.olb -llibcs.olb -llibtcl.olb -llibcomn.olb -llibintl.olb $attr{EXTRA_LIBS}";
    } else {
	my $extra = getExtraLibs($SYBASE, $attr{EXTRA_LIBS});
	if($file) {
	    $lib_string = "-L$SYBASE/lib -lct -lcs -ltcl -lcomn -lintl $attr{EXTRA_LIBS} -lm";
	} else {
	    $lib_string = "-L$SYBASE/lib -lct -lcs -ltcl -lcomn -lintl $extra -lm";
	}
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

sub getExtraLibs {
    my $dir = shift;
    my $cfg = shift;

    my $lib = "$dir/lib";

    my %x = map {$_ => 1} split(' ', $cfg);
    foreach my $f (keys(%x)) {
	my $file = $f;
	$file =~ s/-l//;
	next if($file =~ /^-/);
	if(!-e "$lib/lib$file.$Config{so}") {
	    delete($x{$f});
	}
    }
    

    foreach my $f (qw(insck tli sdna dnet_stub)) {
	$x{"-l$f"} = 1 if(-e "$lib/lib$f.$Config{so}");
    }

    join(' ', keys(%x));
}
    
	
