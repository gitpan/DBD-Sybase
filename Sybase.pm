# -*-Perl-*-
# %W%	%G%

# Copyright (c) 1996
#   Michael Peppler
#
#   You may copy this under the terms of the GNU General Public License,
#   or the Artistic License, copies of which should have accompanied
#   your Perl kit.

# This stuff is based on Tim Bunce's ExampleP driver in DBI-0.66

{
    package DBD::Sybase;

    require DBI;
    use Sybase::CTlib;

    @EXPORT = qw(); # Do NOT @EXPORT anything.

    $VERSION = '0.01';

    $drh = undef;	# holds driver handle once initialised
    $err = 0;		# The $DBI::err value
    $errstr = '';

    sub driver{
	return $drh if $drh;
	my($class, $attr) = @_;
	$class .= "::dr";
	($drh) = DBI::_new_drh($class, {
	    'Name' => 'Sybase',
	    'Version' => $VERSION,
	    'Err'     => \$DBD::Sybase::err,
	    'Errstr'  => \$DBD::Sybase::errstr,
	    'Attribution' => 'Sybase DBI driver by Michael Peppler',
	    }, ['unused']);
	$drh;
    }

    sub srv_cb{
	my($cmd, $number, $severity, $state, $line, $server, $proc, $msg)
	    = @_;
	my($string);

#	print "$cmd\n";

    # Don't print informational or status messages
	if($severity > 10)
	{
	    $err = $number;
	    
	    $string = sprintf("Message number: %ld, Severity %ld, ",
			      $number, $severity);
	    $string .= sprintf("State %ld, Line %ld\n",
			       $state, $line);
	    
	    if (defined($server)) {
		$string .= sprintf("Server '%s'\n", $server);
	    }
    
	    if (defined($proc))	{
		$string .= sprintf(" Procedure '%s'\n", $proc);
	    }

	    $string .= "Message String: $msg\n";
	} elsif ($number == 0) {
	    $string = "$msg\n";
	}
#	print($string) if $string;
	
	$errstr = $string if $string;

	&Sybase::CTlib::CS_SUCCEED;
    }
    Sybase::CTlib::ct_callback(&Sybase::CTlib::CS_SERVERMSG_CB,
			       \&DBD::Sybase::srv_cb);

    sub errstr {
	$errstr;
    }

    1;
}


{   package DBD::Sybase::dr; # ====== DRIVER ======

    use Sybase::CTlib;

    $imp_data_size = 0;
    
    sub errstr {
	DBD::Sybase::errstr(@_);
    }
    
    sub connect { # normally overridden, but a handy default
        my($drh, $dbname, $user, $auth)= @_;
	my($cth) = new Sybase::CTlib $user, $auth, $dbname;
        my($this) = DBI::_new_dbh($drh, {
	    'Name' => $dbname,
	    'User' => $user,
	    'CTlib' => $cth,
	    });

	$this;
    }

    sub disconnect_all {
	# we don't need to tidy up anything
    }
    sub DESTROY { undef }
}


{   package DBD::Sybase::db; # ====== DATABASE ======

    use Sybase::CTlib;
    
    $imp_data_size = 0;
#    use strict;

    sub errstr {
	DBD::Sybase::errstr(@_);
    }

    sub do {
	my($dbh, $statement) = @_;
	my($cth) = $dbh->{CTlib};
	my($rc);

	$cth->{MaxRows} = 1;
	$rc = $cth->ct_sql($statement);
	$rc = 1 if $rc;
	$cth->{MaxRows} = 0;

	$rc;
    }
    
    sub prepare {
	my($dbh, $statement)= @_;
	my($cth) = $dbh->{CTlib};

	if($cth->ct_command(&Sybase::CTlib::CS_LANG_CMD,
			    $statement,
			    &Sybase::CTlib::CS_NULLTERM,
			    &Sybase::CTlib::CS_UNUSED) !=
	   &Sybase::CTlib::CS_SUCCEED) {
	    return undef;
	}
	    
	
	my($outer, $sth) = DBI::_new_sth($dbh, {
	    'Statement'     => $statement,
	    CTlib => $cth,
#	    'fields'        => \@fields,
	    }, ['example implementors private data']);

#	$outer->{NUM_OF_FIELDS} = @fields;
#	$outer->{NUM_OF_PARAMS} = 1;

	$outer;
    }

    sub disconnect {
	my($dbh) = @_;
	my($cth) = $dbh->{CTlib};

	undef($cth);
	$dbh->{CTlib} = undef;
	1;
    }

    sub tables {
    }

    sub commit {
    }
    sub rollback {
    }
    

    sub DESTROY { undef }
}


{   package DBD::Sybase::st; # ====== STATEMENT ======
    use Sybase::CTlib;
    $imp_data_size = 0;
#    use strict; no strict 'refs'; # cause problems with filehandles

    sub errstr {
	DBD::Sybase::errstr(@_);
    }

####
# This doesn't do anything!!!
####
    sub bind_param {
	my($sth, $param, $value, $attribs) = @_;
	$sth->{'param'}->[$param] = $value;
    }
	
    sub execute {
	my($sth, @data) = @_;
	my($cth) = $sth->{CTlib};
	my($restype, $rc);

	if($cth->ct_send != &Sybase::CTlib::CS_SUCCEED) {
	    return undef;
	}
	$restype = 0;
	while(($rc = $cth->ct_results($restype))==&Sybase::CTlib::CS_SUCCEED) {
	    $cth->{'ROW_COUNT'} = $cth->ct_res_info(&Sybase::CTlib::CS_ROW_COUNT)
		if $restype == &Sybase::CTlib::CS_CMD_DONE;
	    last if $cth->ct_fetchable($restype);
	}
	$rc == &Sybase::CTlib::CS_SUCCEED;
    }

    sub fetchrow {
	my($sth) = @_;
	my($cth) = $sth->{CTlib};
	my(@arr, $restype);

	if(!(@arr = $cth->ct_fetch)) {
	    while($cth->ct_results($restype) == &Sybase::CTlib::CS_SUCCEED) {
		$cth->{'ROW_COUNT'} = $cth->ct_res_info(&Sybase::CTlib::CS_ROW_COUNT)
		    if $restype == &Sybase::CTlib::CS_CMD_DONE;
		if($cth->ct_fetchable($restype)) {
		    @arr = $cth->ct_fetch;
		    last;
		}
	    }
	}
	@arr;
    }

    sub finish {
	if(0){
	my($sth) = @_;
	my($cth) = $sth->{CTlib};
	my($restype);

	while($cth->ct_results($restype) == &Sybase::CTlib::CS_SUCCEED) {
	    if($cth->ct_fetchable($restype)) {
		$cth->ct_cancel(&Sybase::CTlib::CS_CANCEL_ALL);
	    }
	}}
	1;
    }

####
# All of this needs to be adapted...
####
    sub FETCH {
	my ($sth, $attrib) = @_;
	# In reality this would interrogate the database engine to
	# either return dynamic values that cannot be precomputed
	# or fetch and cache attribute values too expensive to prefetch.
	if ($attrib eq 'TYPE'){
	    my(@t) = $sth->{CTlib}->ct_col_types;
	    return \@t;
	}
	elsif($attrib eq 'NAME'){
	    my(@t) = $sth->{CTlib}->ct_col_names;
	    return \@t;
	}
	# else pass up to DBI to handle
	return $sth->DBD::_::dr::FETCH($attrib);
    }

    sub STORE {
	my ($sth, $attrib, $value) = @_;
	# would normally validate and only store known attributes
	# else pass up to DBI to handle
	return $sth->DBD::_::dr::STORE($attrib, $value);
    }

    sub DESTROY { undef }
}

1;
