package Net::Async::AMQP::Server;

use strict;
use warnings;

use parent qw(IO::Async::Listener);

=head1 NAME

Net::Async::AMQP::Server

=head1 SYNOPSIS

=head1 DESCRIPTION

=cut

use curry;
use IO::Socket::IP;

use Net::Async::AMQP::Server::Connection;

=pod

=cut

=head2 configure

Set up the instance.

Takes the following named parameters:

=over 4

=item * local_host

=item * port

=back

=cut

sub configure {
	my ($self, %args) = @_;
	$self->{$_} = delete $args{$_} for qw/local_host port/;
	return $self->SUPER::configure(%args);
}

=head2 local_host

Accessor for the current local_host setting.

=cut

sub local_host { shift->{local_host} }

=head2 port

Accessor for the current port setting

=cut

sub port { shift->{port} }

=head2 listening

Resolves with the listener.

=cut

sub listening {
	my $self = shift;
	$self->{listening} ||= $self->loop->new_future
}

sub notifier_name {
	my $self = shift;
	'NaAMQPServer=' . join ':', $self->local_host, $self->port
}

=head2 on_listen

Called when we have a listening socket.

=cut

sub on_listen {
	my $self = shift;
	my ($host, $port) = $self->read_handle->sockhost_service($self->sockname);
	$self->{port} = $port;
	$self->{local_host} = $host;
	$self->listening->done(
		$self->{local_host},
		$self->{port}
	)
}

sub _add_to_loop {
	my ($self, $loop) = @_;
	$self->SUPER::_add_to_loop($loop);
	$self->adopt_future(
		$self->listen(
			addr => {
				family => 'inet',
				socktype => 'stream',
				port => $self->port,
				ip => ($self->local_host // '0.0.0.0'),
			},
		)->then(sub {
			$self->on_listen;
		})
	)
}

sub on_accept {
	my ($self, $sock) = @_;
	$self->debug_printf("Incoming: $sock");
	my $stream = Net::Async::AMQP::Server::Connection->new(
		handle => $sock,
	);
	$self->add_child($stream);
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@perlsite.co.uk>

=head1 LICENSE

Licensed under the same terms as Perl itself, with additional licensing
terms for the MQ spec to be found in C<share/amqp0-9-1.extended.xml>
('a worldwide, perpetual, royalty-free, nontransferable, nonexclusive
license to (i) copy, display, distribute and implement the Advanced
Messaging Queue Protocol ("AMQP") Specification').

