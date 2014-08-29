package Net::Async::AMQP::Server::Connection;

use strict;
use warnings;

use parent qw(IO::Async::Stream);

use constant DEBUG => 1;

use curry;
use Net::Async::AMQP;
use Net::Async::AMQP::Server::Protocol;

sub protocol {
	my $self = shift;
	$self->{protocol} ||= Net::Async::AMQP::Server::Protocol->new(
		write => $self->curry::weak::write,
		future_factory => $self->loop->curry::weak::new_future,
	)
}

sub on_read {
	my ($self, $buffer, $eof) = @_;
	$self->debug_printf("MQ connection - read %s", $$buffer);
	return $self->protocol->on_read($buffer, $eof);
}

1;
