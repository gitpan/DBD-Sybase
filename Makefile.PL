# $Id: Makefile.PL,v 1.29 2004/06/11 11:52:00 mpeppler Exp $

use ExtUtils::MakeMaker;

require 5.004;

use strict;

# If either of these aren't available on your system then you need to
# get them!
use DBI;
use DBI::DBD;
use Config;
use Getopt::Long;

use vars qw($SYBASE $inc_string $lib_string $LINKTYPE $written_pwd_file
	    $newlibnames);

$LINKTYPE = 'dynamic';
$written_pwd_file = 'PWD';

my $file;
GetOptions('--file' => \$file);

configure();

configPwd();

my $lddlflags = $Config{lddlflags};
$lddlflags = "-L$SYBASE/lib $lddlflags" unless $^O eq 'VMS';
my $ldflags = $Config{ldflags};
$ldflags = "-L$SYBASE/lib $ldflags" unless $^O eq 'VMS';


WriteMakefile('NAME'         => 'DBD::Sybase',
	      LIBS           => [$lib_string],
	      INC            => $inc_string,
	      clean	     => { FILES=> "Sybase.xsi $written_pwd_file" },
	      OBJECT         => '$(O_FILES)',
	      'VERSION_FROM' => 'Sybase.pm',
	      'LDDLFLAGS'    => $lddlflags,
#	      'LDFLAGS'      => $ldflags,
	      LINKTYPE       => $LINKTYPE,
	      ($^O eq 'VMS' ? 
               (MAN3PODS    => { 'Sybase.pm' => 'blib/man3/DBD_Sybase.3' }) :
               (MAN3PODS    => { 'Sybase.pm' => 'blib/man3/DBD::Sybase.3' })),
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
	# PR 517 - getpwnam() isn't portable.
	eval q{
	    $sybase_dir = (getpwnam('sybase'))[7];
	};
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

    $SYBASE = VMS::Filespec::unixify($SYBASE) if $^O eq 'VMS';

    if(!$SYBASE || $SYBASE =~ /^\s*$/) {
	die "Please set SYBASE in CONFIG, or set the \$SYBASE environment variable";
    }

    # System 12.0 has a different directory structure...
    if(defined($ENV{SYBASE_OCS})) {
	$SYBASE .= "/$ENV{SYBASE_OCS}";
    }

    if(! -d "$SYBASE/lib") {
	die "Can't find the lib directory under $SYBASE!";
    }
	
    die "Can't find any Sybase libraries in $SYBASE/lib" unless checkLib($SYBASE);

    my $inc_found = 0;
    if(-d "$SYBASE/include" && -f "$SYBASE/include/cspublic.h") {
      ++$inc_found;
      $inc_string = "-I$SYBASE/include";
    }
    # In some freetds installations the include files have been moved
    # into /usr/include/freetds.
    if(-d "$SYBASE/include/freetds" && "$SYBASE/include/freetds/cspublic.h") {
      ++$inc_found;
      $inc_string = "-I$SYBASE/include/freetds";
    }
    die "Can't find the Client Library include files under $SYBASE" unless($inc_found);

    if($^O eq 'MSWin32') {
	$lib_string = "-L$SYBASE/lib -llibct.lib -llibcs.lib -llibtcl.lib -llibcomn.lib -llibintl.lib $attr{EXTRA_LIBS} -lm";
    } elsif($^O eq 'VMS') {
	$lib_string = "-L$SYBASE/lib -llibct.olb -llibcs.olb -llibtcl.olb -llibcomn.olb -llibintl.olb $attr{EXTRA_LIBS}";
    } else {
	my $extra = getExtraLibs($SYBASE, $attr{EXTRA_LIBS});
	if($file) {
	    $lib_string = "-L$SYBASE/lib -lct -lcs -ltcl -lcomn -lintl $attr{EXTRA_LIBS} -ldl -lm";
	} else {
	    $lib_string = "-L$SYBASE/lib -lct -lcs -ltcl -lcomn -lintl $extra -ldl -lm";
	}
	if($newlibnames) {
	    foreach (qw(ct cs tcl comn intl)) {
		$lib_string =~ s/-l$_/-lsyb$_/;
	    }
	} elsif($^O =~ /linux|freebsd/i) {
	    $lib_string =~ s/ltcl/lsybtcl/;
	}

	# Logic to replace normal libs with _r (re-entrant) libs if
	# usethreads is defined provided by W. Phillip Moore (wpm@ms.com)
	# I have no idea if this works on Win32 systems (probably not!)
	if ( $Config{usethreads} ) {
	    print "Running in threaded mode - looking for _r libraries...\n";
	    opendir(SYBLIB,"$SYBASE/lib")
		or die "Unable to opendir $SYBASE/lib: $!\n";
	    
	    my %libname = ();
	    
	    foreach ( readdir(SYBLIB) ) {
		next unless /^lib(\S+)\.(so|a|sl)/;
		next unless -f "$SYBASE/lib/$_";
		$libname{$1} = 1;
	    }
	    
	    closedir(SYBLIB);
	    my $found = 0;
	    foreach ( split(/\s+/,$lib_string) ) {
		next unless /^-l(\S+)/;
		my $oldname = $1;
		my $newname = $1 . "_r";
		next unless exists $libname{$newname};
		print "Found -l$newname for -l$oldname\n";
		++$found;
		$lib_string =~ s/-l$oldname\b/-l$newname/;
	    }
	    if(!$found) {
		print "No thread-safe Sybase libraries found\n";
		$inc_string .= ' -DNO_THREADS ';
	    }
	}
	
	# If we are building for a 64 bit platform that also supports 32 bit
	# (i.e. Solaris 8, HP-UX11, etc) then we need to make some adjustments
	if( $Config{use64bitall} ) {
	    $inc_string .= ' -DSYB_LP64';
	    
	    print "Running in 64bit mode - looking for '64' libraries...\n";
	    opendir(SYBLIB,"$SYBASE/lib")
		or die "Unable to opendir $SYBASE/lib: $!\n";
	    
	    my %libname = ();
	    
	    foreach ( readdir(SYBLIB) ) {
		next unless -f "$SYBASE/lib/$_";
		next unless /^lib(\S+)\.(so|a|sl)/;
		$libname{$1} = 1;
	    }
	    
	    closedir(SYBLIB);
	    
	    foreach ( split(/\s+/,$lib_string) ) {
		next unless /^-l(\S+)/;
		my $oldname = $1;
		my $newname = $1 . '64';
		next unless exists $libname{$newname};
		print "Found -l$newname for -l$oldname\n";
		$lib_string =~ s/-l$oldname\b/-l$newname/;
	    }	
	}
    }
    
    my $config_sitearch = $Config{sitearchexp};
    my $attr_dbi_include = $attr{DBI_INCLUDE};
    if ($^O eq 'VMS') {
    	$config_sitearch = VMS::Filespec::unixify($config_sitearch);
    	$attr_dbi_include = VMS::Filespec::unixify($attr_dbi_include);
    }
    my @try = (@INC, $Config{sitearchexp});
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
    $inc_string .= " -I$dbidir";
    if($attr{LINKTYPE}) {
	$LINKTYPE = $attr{LINKTYPE}
    }
}

sub getExtraLibs {
    my $dir = shift;
    my $cfg = shift;

    my $lib = "$dir/lib";
    my @files = qw(libct.a libsybct.a libct64.a libsybct64.a);
    my $file;
    foreach (@files) {
	$file = "$lib/$_";
	last if -e $file;
    }

    my $version = `strings $file`;
    $version =~ /Sybase Client-Library\/([^\/]+)\//;
    $version = $1;
    print "Sybase OpenClient $version found.\n";

    opendir(DIR, "$dir/lib") || die "Can't access $dir/lib: $!";
    my %files = map { $_ =~ s/lib([^\.]+)\..*/$1/; $_ => 1 } grep(/lib/ && -f "$dir/lib/$_", readdir(DIR));
    closedir(DIR);

    my %x = map {$_ => 1} split(' ', $cfg);
    my $dlext = $Config{dlext} || 'so';
    foreach my $f (keys(%x)) {
	my $file = $f;
	$file =~ s/-l//;
	next if($file =~ /^-/);
	delete($x{$f}) unless exists($files{$file});
    }

    foreach my $f (qw(insck tli sdna dnet_stub tds skrb gss)) {
	$x{"-l$f"} = 1 if exists $files{$f}  && -f "$dir/lib/lib$f.$dlext";
    }
    if($version gt '11') {
	delete($x{-linsck});
	delete($x{-ltli});
    }
#    if($version ge '12.5.1') {
#	delete($x{-lskrb});
#    }

    join(' ', keys(%x));
}
    
	
sub checkLib {
    my $dir = shift;

    opendir(DIR, "$dir/lib") || die "Can't access $dir/lib: $!";
    my @files = grep(/libct|libsybct/i, readdir(DIR));
    closedir(DIR);
    if(grep(/libsybct/, @files)) {
	$newlibnames = 1;
    } else {
	$newlibnames = 0;
    }

    scalar(@files);
}

sub configPwd {
    open(IN, "PWD.factory") || die "Can't open PWD.factory: $!";
    my %pwd;
    while(<IN>) {
	chomp;
	next if(/^\s*\#/);
	next if(/^\s*$/);
	my ($key, $val) = split(/=/, $_);
	$pwd{$key} = $val || "undef";
    }
    close(IN);
    select(STDOUT); $| = 1;

    print "The DBD::Sybase module need access to a Sybase server to run the tests.\n";
    print "To clear an entry please enter 'undef'\n";
    print "Sybase server to use (default: $pwd{SRV}): ";
    $pwd{SRV} = getAns(0) || $pwd{SRV};
    print "User ID to log in to Sybase (default: $pwd{UID}): ";
    $pwd{UID} = getAns(0) || $pwd{UID};
    print "Password (default: $pwd{PWD}): ";
    $pwd{PWD} = getAns(1) || $pwd{PWD};
    print "Sybase database to use on $pwd{SRV} (default: $pwd{DB}): ";
    $pwd{DB} = getAns(0) || $pwd{DB};

    warn "\n* Writing login information, including password, to file $written_pwd_file.\n\n";
    # Create the file non-readable by anyone else.
    my $old_umask;
    unless($^O =~ /MSWin32/) {
	$old_umask = umask(077);
	die "cannot umask(): $!" unless $old_umask;
    }
    open(OUT, ">$written_pwd_file") || die "Can't open $written_pwd_file: $!";
    unless($^O =~ /MSWin32/) {
	umask($old_umask) != 077 && die "strange return from umask()";
    }
    print OUT <<EOF;
# This file contains optional login id, passwd and server info for the test
# programs:
# You probably don't want to have it lying around after you've made
# sure that everything works OK.

EOF
    foreach (keys %pwd) {
	$pwd{$_} = '' if $pwd{$_} eq 'undef';
	print OUT "$_=$pwd{$_}\n";
    }
    close(OUT);
}

sub getAns {
    my $flag = shift;

    if($flag && -t) {
	eval {
	    require Term::ReadKey;
	    Term::ReadKey::ReadMode('noecho');
	};
    }
    my $ans = <STDIN>;
    if($flag && -t) {
	eval {
	    Term::ReadKey::ReadMode('normal');
	};
    }
    $ans =~ s/^\s+//;
    $ans =~ s/\s+$//;

    return $ans;
}
