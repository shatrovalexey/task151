package Task151::Data ;
use strict ;
use warnings ;
use IO::File ;
use Digest::SHA1 ;
+ 1 ;

sub new( $;% ) { + bless { + splice( @_ , 1 ) , 'level' => 0 , } , shift }

sub _hash( $$ ) { &Digest::SHA1::sha1_hex( $_[ 1 ] )  }
sub hash( $$;@ ) {
	my $self = shift ;

	$self->_hash( join q{} , map $self->_hash( $_ ) , @_ )
}
sub node_name_joined( $$$$ ) { + join '-' , splice @_ , 1 , 3 }

sub node_name( $$;@ ) {
	my $self = shift ;

	$self->node_name_joined( shift , map $self->hash( $_ ) , shift , shift )
}

sub get_path( $$;@ ) { + join '/' , shift( @_ )->{ 'path' } , @_ }

sub node_path( $$$$ ) {
	my $self = shift ;

	$self->get_path( $self->node_name( @_ ) )
}

sub get_io( $$$ ) { + IO::File->new( @_[ 1 , 2 , ] )  }
sub get_read( $$ ) { shift( @_ )->get_io( shift , &O_RDONLY( ) ) }
sub get_write( $$ ) { shift( @_ )->get_io( shift , &O_WRONLY( ) | &O_CREAT( ) ) }

sub get_value( $$ ) {
	my $fh = shift( @_ )->get_read( shift ) ;

	scalar $fh->getline( ) ;

	$fh->getline( )
}

sub get_key( $$ ) { + chomp and return foreach shift( @_ )->get_read( shift )->getline( ) }

sub get_value_by_key( $$ ) {
	my $self = shift ;

	return $self->get_value( $_ ) foreach $self->find_node_by_key( shift )
}

sub get_keys_by_value( $$ ) {
	my $self = shift ;

	map $self->get_key( $_ ) , $self->find_nodes_by_value( shift )
}

sub find_node( $$$$ ) {
	my $self = shift ;

	glob $self->get_path( $self->node_name_joined( @_ ) )
}

sub find_node_by_key( $$ ) {
	my $self = shift ;

	$self->order_nodes_by_current_level( $self->find_node( '*' , $self->hash( shift ) , '*' ) )
}
sub find_nodes_by_value( $$ ) {
	my $self = shift ;

	$self->order_nodes_by_current_level( $self->find_node( '*' , '*' , $self->hash( shift ) ) )
}
sub unset( $$ ) { + unlink shift( @_ )->find_node_by_key( shift ) }
sub set( $$$ ) {
	my ( $self , $key , $value ) = @_ ;
	my ( $path ) = $self->find_node_by_key( $key ) ;

	unlink $path if $path && $path =~ m{/\Q$self->{ 'level' }\E\-\w+\-\w+$}usx ;

	$self->get_write( $self->node_path( $self->{ 'level' } , $key , $value ) )->print( join "\n" , $key , $value )
}
sub count( $$ ) { scalar @{ [ shift( @_ )->find_nodes_by_value( shift ) ] } }

sub explode_path( $$ ) { $_[ 1 ] =~ m{/(\d+)\-(\w+)\-(\w+)$}usx }

sub order_nodes_by_current_level( $;@ ) {
	my ( $self , @nodes , %result ) = @_ ;

	foreach ( sort { $b cmp $a } @nodes ) {
		my ( undef , $key ) = $self->explode_path( $_ ) or next + ( ) ;

		$result{ $key } = $_ unless exists $result{ $key }
	}

	values %result
}
sub transaction_begin( $ ) { ++ shift( @_ )->{ 'level' } }
sub transaction_commit( $ ) {
	my ( $self ) = @_ ;

	return $self->{ 'level' } unless $self->{ 'level' } > 0 ;

	foreach my $old_filename ( glob $self->get_path( $self->node_name_joined( $self->{ 'level' } , '*' ) ) ) {
		my ( undef , $key , $value ) = $self->explode_path( $old_filename ) ;
		my $level_new = $self->{ 'level' } - 1 ;
		my $new_filename = $self->get_path( $self->node_name_joined( $level_new , $key , $value ) ) ;

		unlink glob $self->get_path( $self->node_name_joined( $level_new , $key , '*' ) ) ;

		rename $old_filename , $new_filename
	}

	-- $self->{ 'level' }
}
sub transaction_rollback( $ ) {
	my ( $self ) = @_ ;

	return $self->{ 'level' } unless $self->{ 'level' } > 0 ;

	unlink glob $self->get_path( $self->node_name_joined( $self->{ 'level' } , '*' ) ) ;

	-- $self->{ 'level' }
}