#!/usr/bin/env perl
use Paradox;
use Cwd;
use DBI;
use DBD::Pg;
use Encode qw(encode decode);
use Getopt::Std;
use warnings;
use strict;

my %opts;
&getoptions;
my @files    = glob("*.[Dd][Bb]");    # array of all database files in folder
my $basename = $opts{'n'};            # name of database
my $login    = $opts{'l'};            # login to PGSQL server
my $password = $opts{'p'};            # password to PGSQL server
my ( $dbh, $sth );
my ( $num_f, $code_page, $num )
  ; #number of fields in Paradox file, code page for data in Paradox file, number of records in Paradox
my ( @type, @len, @name )
  ; # array data types of fields in Paradox file, array data length of fields in Paradox file,  array data length of fields in Paradox file

unless ( $opts{'f'} ) {    # convert data to PGSQL

    $dbh = DBI->connect( "DBI:Pg:dbname=postgres", "$login", "$password" )
      or die("Could't connect to database: $DBI:: errstr");
    $dbh->do("drop database $basename");
    $dbh->do("create database $basename;");
    $dbh->do("ALTER DATABASE $basename SET datestyle TO 'DMY';");
    $dbh->disconnect();
    $dbh = DBI->connect( "DBI:Pg:dbname=$basename", "$login", "$password" )
      or die("Could't connect to database: $DBI:: errstr");
}
else { open FILEOUT, "> $opts{'N'}" . '.sql' }    #convert data to file

for my $f_table (@files) {                        # for every file in folder

    my $db = new Paradox "$f_table";
    $code_page = $db->{code_page}
      if $db->{code_page};    # get codepage for using in decode
    @type = @{ $db->{field_type} };
    @len  = @{ $db->{field_length} };
    @name = @{ $db->{field_name} };
    $num  = $db->{all_records};

    if ($code_page) {
        if ( $opts{'d'} ) {
            map { encode( "$opts{'d'}", decode( $code_page, $_ ) ) } @name;
        }
        else {
            map { decode( $code_page, $_ ) } @name;
        }

    }

    $num_f = scalar(@type);
    $f_table = substr( $f_table, 0, -3 );
    my $sqlcommand = &create_table($f_table);

    unless ( $opts{'f'} ) {
        $sth = $dbh->prepare($sqlcommand);
        $sth->execute;
    }

    else { print( FILEOUT "$sqlcommand\n" ); }
    print "Table $f_table created\n";

    if ($num) {    # if table not empty
        unless ( $opts{'f'} ) {    # copy in base
            $sqlcommand = "copy $f_table from stdin";
            $dbh->do($sqlcommand) or die $DBI::errstr;

            for ( my $j = 1 ; $j <= $num ; $j++ ) {
                my @record_data = $db->fetch();
                $sqlcommand = &convert_data( \@record_data );
                $dbh->pg_putcopydata($sqlcommand);
                if ( !( $j % $opts{'c'} ) and $j < $num )
                {                  # copy $opts{'c'} records
                    $dbh->pg_putcopyend();
                    $sqlcommand = "copy $f_table from stdin";
                    $dbh->do($sqlcommand) or die $DBI::errstr;
                    print "$j records of $num from $f_table copied\n";

                }
            }
            $dbh->pg_putcopyend();
        }
        else {                     #copy in file
            my $buffer = '';
            for ( my $j = 1 ; $j <= $num ; $j++ ) {
                my @record_data = $db->fetch();
                $sqlcommand = &convert_data( \@record_data );
                $buffer .= $sqlcommand;
                if ( !( $j % $opts{'c'} ) and $j < $num )
                {                  # copy $opts{'c'} records
                    print( FILEOUT "$buffer" );
                    print "$j records of $num from $f_table copied\n";
                    $buffer = '';
                }
            }
            print( FILEOUT "$buffer" );
        }

    }

    print "Table $f_table copied\n";
    $db->close();
}

unless ( $opts{'f'} ) {
    $dbh->disconnect();
}
else { close(FILEOUT) }

sub basename {    # get name of base
    my $full_path = cwd;
    my @dirs      = split( /\//, $full_path );
    my $basename  = lc( $dirs[ scalar(@dirs) - 1 ] );
    return $basename;
}

sub getoptions {    # get options from command line
    getopts( 'd:m:n:l:p:c:N:f', \%opts );
    unless (%opts) {
        die "
    no parametres!!!\n
    -l login\n
    -p password\n
    -n basename (if empty, basename =  name of current directory)\n
    -d destination codepage (default cp1251)\n
    -f print sql commands in file (by default converting in base directly) \n
    -N name of output file (by default using basename)\n
    -c count of records for one time recording to base (default 10000)\n";
    }
    $opts{'n'} //= &basename;
    $opts{'c'} //= 10000;
    $opts{'d'} //= 'cp1251';
    $opts{'N'} //= $opts{'n'};

}

sub create_table {    # make command 'CREATE TABLE'
    my $f_table    = shift;
    my $sqlcommand = "CREATE TABLE $f_table (";

    for ( my $i = 0 ; $i < $num_f ; $i++ ) {
        $sqlcommand .= '"' . $name[$i] . '"' . ' ';
        if ( $type[$i] eq 0x01 ) {
            $_ = 'char(' . $len[$i] . ')';
        }
        elsif ( $type[$i] eq 0x02 ) {
            $_ = 'date';
        }
        elsif ( $type[$i] eq 0x0C ) {
            $_ = 'text';
        }
        elsif ( $type[$i] eq 0x09 ) {
            $_ = 'boolean';
        }
        elsif ( $type[$i] eq 0x03 ) {
            $_ = 'smallint';
        }
        elsif ( $type[$i] eq 0x04 ) {
            $_ = 'integer';
        }
        elsif ( $type[$i] eq 0x06 ) {
            $_ = 'float';
        }
        elsif ( $type[$i] eq 0x14 ) {
            $_ = 'time';
        }
        elsif ( $type[$i] eq 0x16 ) {
            $_ = 'integer';
        }
        elsif ( $type[$i] eq 0x10 ) {
            $_ = 'bytea';
        }
        $sqlcommand .= $_ . ', ';
    }
    $sqlcommand = substr( $sqlcommand, 0, length($sqlcommand) - 2 );
    $sqlcommand .= ');';
    return $sqlcommand;
}

sub convert_data {    # convert data to copy
    my $record_data = shift;
    my @record_data = @$record_data;
    my $sqlcommand  = '';
    for ( my $i = 0 ; $i < $num_f ; $i++ ) {
        if ( $type[$i] eq 0x01 || $type[$i] eq 0x0C ) {
            if ( $record_data[$i] ne '' ) {
                $record_data[$i] =~
                  s/(\x09|\x0D|\x0A)/'\\x'.sprintf ("%02X", unpack("C", $1))/ge;
                $record_data[$i] =~ s/\\/\\\\/g;

                unless ($code_page) {
                    $record_data[$i] = encode( "$opts{'d'}", $record_data[$i] )
                      if ( $opts{'d'} );
                }
                else {
                    if ( $opts{'d'} ) {
                        $record_data[$i] =
                          encode( "$opts{'d'}",
                            decode( $code_page, $record_data[$i] ) );
                    }
                    else {
                        $record_data[$i] =
                          decode( $code_page, $record_data[$i] );
                    }
                }
            }
            else { $record_data[$i] = '\N' }
        }
        elsif ( ( $type[$i] eq 0x02 ) or ( $type[$i] eq 0x14 ) ) {
            if ( $record_data[$i] ne '' ) {
                $record_data[$i] = "'" . $record_data[$i] . "'";
            }
            else { $record_data[$i] = '\N'; }
        }

        elsif (( $type[$i] eq 0x04 )
            or ( $type[$i] eq 0x06 )
            or ( $type[$i] eq 0x03 ) )
        {
            if ( $record_data[$i] eq '' ) { $record_data[$i] = 0; }
        }

        elsif ( ( $type[$i] eq 0x10 ) ) {
            $record_data[$i] =~
s/([\x00-\x19\x27\x5C\x7F-\xFF])/'\\\\'.sprintf ("%03o", unpack("C", $1))/ge;
        }

        $sqlcommand .= "$record_data[$i]" . "\t";
    }

    $sqlcommand = substr( $sqlcommand, 0, length($sqlcommand) - 1 );
    $sqlcommand .= "\n";
    return $sqlcommand;

}
