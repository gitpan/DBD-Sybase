# $Id: Makefile.PL,v 1.2 1997/09/26 21:25:02 mpeppler Exp $

use ExtUtils::MakeMaker;

require 5.004;

# If either of these aren't available on your system then you need to
# get them!
use DBI;
use DBI::DBD;
use Config;

$SYBASE = '/opt/sybase';

$lib_string = "-L$SYBASE/lib -lct -lcs -ltcl -lcomn -lintl $$sattr{EXTRA_LIBS} -lm";
$inc_string = "-I$SYBASE/include -I$Config{sitearchexp}/DBI";


WriteMakefile('NAME' => 'DBD::Sybase',
	      LIBS => [$lib_string],
	      INC => $inc_string,
	      clean	=> { FILES=> 'Sybase.xsi' },
	      OBJECT => '$(O_FILES)',
	      'VERSION_FROM' => 'Sybase.pm',
#		LINKTYPE => 'static',
	      'dist' => {'TARFLAGS' => 'cvfF', 'COMPRESS' => 'gzip'},
	      );

sub MY::postamble {
    return dbd_postamble();
}
	      
