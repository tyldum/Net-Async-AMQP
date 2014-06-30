package Net::Async::AMQP::ConnectionManager::Channel;

use strict;
use warnings;

=head1 NAME

Net::Async::AMQP::ConnectionManager::Channel - channel proxy object

=head1 METHODS

=head2 new

Instantiate.

=cut

sub new {
	my $class = shift;
	my $self = bless { @_ }, $class;
	Scalar::Util::weaken($_) for @{$self}{qw(manager channel)};
	$self
}

=head2 channel

Returns the underlying AMQP channel.

=cut

sub channel { shift->{channel} }

=head2 manager

Returns our ConnectionManager instance.

=cut

sub manager { shift->{manager} }

=head2 DESTROY

On destruction we release the channel by informing the connection manager
that we no longer require the data.

=cut

sub DESTROY {
	my $self = shift;
	warn "Releasing channel " . $self->channel->id;
	(delete $self->{manager})->release_channel(delete $self->{channel});
}

{
our $AUTOLOAD;
sub AUTOLOAD {
	my ($self, @args) = @_;
	(my $method = $AUTOLOAD) =~ s/.*:://;
	die "attempt to proxy unknown method $method for $self" unless $self->channel->can($method);
	my $code = sub {
		my $self = shift;
		$self->channel->$method(@_);
	};
	{ no strict 'refs'; *$method = $code }
	$code->(@_)
}
}

1;
