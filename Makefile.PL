#	@(#)Makefile.PL	1.2	2/2/96

use ExtUtils::MakeMaker;

require 5.002;

# If either of these aren't available on your system then you need to
# get them!
use DBI;
use Sybase::CTlib;

WriteMakefile('NAME' => 'DBD::Sybase',
	      'VERSION' => '0.01',
	      'dist' => {'TARFLAGS' => 'cvfF', 'COMPRESS' => 'gzip'},
	      );

	      
