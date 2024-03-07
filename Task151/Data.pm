package Task151::Data ;
use strict ;
use warnings ;
use IO::File ;
use Digest::SHA1 ;
+ 1 ;

sub new( $;% ) { + bless { + splice( @_ , 1 ) , 'level' => 0 , } , shift }

sub hash( $$@ ) { &Digest::SHA1::sha1_hex( join q{} , map &Digest::SHA1::sha1_hex( $_ ) , splice @_ , 1 ) }

sub node_name_joined( $$$ ) { + join '-' , splice @_ , 1 , 3 }

sub node_name( $$;@ ) {
	my $self = shift ;

	$self->node_name_joined( map( $self->hash( $_ ) , splice( @_ , 0 , 2 ) ) , @_ )
}

sub get_path( $$;@ ) {
	my $self = shift ;

	join '/' , $self->{ 'path' } , @_
}

sub node_path( $$$$ ) {
	my $self = shift ;

	$self->get_path( $self->node_name( @_ ) )
}

sub rewrite( $$$ ) {
	my $self = shift ;

	IO::File->new( $self->node_path( @_ ) , &O_WRONLY( ) )->print( join "\n" , @_ )
}

sub get_read( $$ ) { + IO::File->new( splice( @_ , 1 , 1 ) , &O_RDONLY( ) ) }
sub get_write( $$ ) { + IO::File->new( splice( @_ , 1 , 1 ) , &O_WRONLY( ) | &O_CREAT( ) ) }

sub get_value( $$ ) {
	my $fh = shift( @_ )->get_read( shift ) ;

	scalar $fh->getline( ) ;

	$fh->getline( )
}

sub get_key( $$ ) {
	my $key = shift( @_ )->get_read( shift )->getline( ) ;

	chomp $key ;

	$key
}

sub get_value_by_key( $$ ) {
	my $self = shift ;

	return $self->get_value( $_ ) foreach $self->find_node_by_key( shift )
}

sub get_keys_by_value( $$ ) {
	my $self = shift ;

	map $self->get_key( $_ ) , $self->find_nodes_by_value( shift )
}

sub find_node( $$$ ) {
	my $self = shift ;

	glob $self->get_path( $self->node_name_joined( splice( @_ , 0 , 2 ) ) )
}

sub find_node_by_key( $$ ) {
	my $self = shift ;

	$self->order_nodes_by_current_level( $self->find_node( $self->hash( shift ) , '*' ) )
}
sub find_nodes_by_value( $$ ) {
	my $self = shift ;

	$self->order_nodes_by_current_level( $self->find_node( '*' , $self->hash( shift ) , '*' ) )
}
sub unset( $$ ) { + unlink shift( @_ )->find_node_by_key( shift ) }
sub set( $$$ ) {
	my ( $self , $key , $value ) = @_ ;
	my @path = $self->find_node_by_key( $key ) ;
	my ( $path ) = @path ;

	unlink $path if $path && $path =~ m{^.+?/\w+\-\w+\-\Q$self->{ 'level' }\E$}usx ;

	$self->get_write( $self->node_path( $key , $value , $self->{ 'level' } ) )->print( join "\n" , $key , $value )
}
sub count( $$ ) { scalar @{ [ shift( @_ )->find_nodes_by_value( shift ) ] } }

sub order_nodes_by_current_level( $;@ ) {
	my ( $self , @nodes , %result ) = @_ ;

	foreach ( sort { [ $b =~ m{(\d+)$}usx ]->[ 0 ] <=> [ $a =~ m{(\d+)$}usx ]->[ 0 ] } @nodes ) {
		$result{ $& } = $_ if m{/\w+}usx && ! exists( $result{ $& } )
	}

	values %result
}
sub transaction_begin( $ ) { ++ shift( @_ )->{ 'level' } }
sub transaction_commit( $ ) {
	my ( $self ) = @_ ;

	return $self->{ 'level' } unless $self->{ 'level' } > 0 ;

	foreach my $old_filename ( glob $self->get_path( $self->node_name_joined( '*' , $self->{ 'level' } ) ) ) {
		my ( $key , $value ) = $old_filename =~ m{^.+?/(\w+)(\-\w+\-)\d+$}usx ;
		my $level_new = $self->{ 'level' } - 1 ;
		my $new_filename = $self->get_path( $key . $value . $level_new ) ;

		unlink glob $self->get_path( $self->node_name_joined( $key , '*' , $level_new ) ) ;

		rename $old_filename , $new_filename
	}

	-- $self->{ 'level' }
}
sub transaction_rollback( $ ) {
	my ( $self ) = @_ ;

	return $self->{ 'level' } unless $self->{ 'level' } > 0 ;

	unlink glob $self->get_path( $self->node_name_joined( '*' , $self->{ 'level' } ) ) ;

	-- $self->{ 'level' }
}