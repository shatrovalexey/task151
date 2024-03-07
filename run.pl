#!/usr/bin/perl -I.

use strict ;
use warnings ;
use IO::Handle ;
use Task151::Data ;

die q{первым аргументом укажите путь к папке с БД} unless @ARGV ;

my ( $path ) = @ARGV ; # как вариант - db

die qq{не удаётся получить доступ или создать папку "$path"} unless -d( $path ) && -w( _ ) || mkdir $path ;

my $datah = Task151::Data->new( 'path' => $path ) ;
my $inph = \*STDIN ;
my $outh = \*STDOUT ;
my $rx_cmd = qr{
	^\s*(BEGIN|COMMIT|ROLLBACK|END)\s*$ |
	^\s*(GET|UNSET|FIND|COUNTS)\s*(.+?)\s*$ |
	^\s*(SET)\s*(\S+)\s*(.+?)\s*$
}usxi;

my %cmds = (
	'SET' => sub ( $$ ) { $datah->set( @_ ) } ,
	'UNSET' => sub ( $ ) { $datah->unset( @_ ) } ,
	'COUNTS' => sub ( $ ) { $datah->unset( @_ ) } ,
	'FIND' => sub ( $ ) { $outh->print( join "\n" , $datah->get_keys_by_value( @_ ) , q{} ) } ,
	'GET' => sub ( $ ) { $outh->print( $datah->get_value_by_key( @_ ) , "\n" ) } ,
	'BEGIN' => sub ( ;@ ) { $datah->transaction_begin( ) } ,
	'COMMIT' => sub ( ;@ ) { $datah->transaction_commit( ) } ,
	'ROLLBACK' => sub ( ;@ ) { $datah->transaction_rollback( ) } ,
) ;

@SIG{ +qw{INT TERM} } = $cmds{ 'END' } = sub ( ;@ ) { $cmds{ 'ROLLBACK' }->( ) ; exit } ;

while ( <$inph> ) {
	warn( 'Invalid syntax' ) and next + ( ) unless m{$rx_cmd} ;

	my ( $cmd ) = map uc , grep length , $1 , $2 , $4 ;
	my ( $key ) = grep length , $3 , $5 ;
	my ( $value ) = grep length , $6 ;

	chomp $value if defined $value ;

	$cmds{ $cmd }->( $key , $value )
}