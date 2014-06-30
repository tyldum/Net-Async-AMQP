package Net::Async::AMQP::Server;

use strict;
use warnings;

use parent qw(IO::Async::Listener);

use curry;

use Net::Async::AMQP::Server::Connection;

=pod

=cut

sub configure {
	my ($self, %args) = @_;
	$self->{$_} = delete $args{$_} for qw/local_host port/;
	return $self->SUPER::configure(%args);
}

sub local_host { shift->{local_host} }
sub port { shift->{port} }
sub listening {
	my $self = shift;
	$self->{listening} ||= $self->loop->new_future
}

sub notifier_name {
	my $self = shift;
	'NaAMQPServer=' . join ':', $self->local_host, $self->port
}

use IO::Socket::IP;

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
	$self->listen(
		addr => {
			family => 'inet',
			socktype => 'stream',
			port => $self->port,
			ip => ($self->local_host // '0.0.0.0'),
		},
		on_listen => $self->curry::weak::on_listen,

		on_resolve_error => sub { print STDERR "Cannot resolve - $_[0]\n"; },
		on_listen_error  => sub { print STDERR "Cannot listen\n"; },
	);
}

sub on_accept {
	my ($self, $sock) = @_;
	warn "Incoming: $sock\n";
	my $stream = Net::Async::AMQP::Server::Connection->new(
		handle => $sock,
	);
	$self->add_child($stream);
}

1;

