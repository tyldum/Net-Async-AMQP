package Net::Async::AMQP::ConnectionManager::Channel;

use strict;
use warnings;

=head1 NAME

Net::Async::AMQP::ConnectionManager::Channel - channel proxy object

=cut

use Future::Utils qw(fmap_void);

use overload
    '""' => sub { shift->as_string },
    '0+' => sub { 0 + shift->id },
    bool => sub { 1 },
    fallback => 1;

=head1 METHODS

=head2 new

Instantiate. Expects at least the manager and channel named parameters.

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

=head2 as_string

String representation of the channel object.

Takes the form "Channel[N]", where N is the ID.

=cut

sub as_string {
	my $self = shift;
	sprintf "Channel[%d]", $self->id;
}

=head2 DESTROY

On destruction we release the channel by informing the connection manager
that we no longer require the data.

There may be some cleanup tasks required before we can release - cancelling
any trailing consumers, for example. These are held in the cleanup hash.

=cut

sub DESTROY {
	my $self = shift;
	my $conman = delete $self->{manager};
	my $ch = delete $self->{channel};
	$conman->debug_printf("Releasing channel %d", $ch->id);
	return $conman->release_channel($ch) unless $self->{cleanup};
	my $f;
	$f = (
		fmap_void {
			$_->()
		} foreach => [
			sort values %{$self->{cleanup}}
		]
	)->on_ready(sub {
		$conman->release_channel($ch);
		undef $f;
	});
}

=head2 AUTOLOAD

All other methods are proxied to the underlying L<Net::Async::AMQP::Channel>.

=cut

sub AUTOLOAD {
	my ($self, @args) = @_;
	(my $method = our $AUTOLOAD) =~ s/.*:://;
	# We could check for existence first, but we wouldn't store the resulting coderef anyway,
	# so might as well just allow the method call to fail normally (we have no idea what
	# subclasses are used for $self->channel, using the first one we find would not be very nice)
	# die "attempt to proxy unknown method $method for $self" unless $self->channel->can($method);
	my $code = sub {
		my $self = shift;
		$self->channel->$method(@_);
	};
	{ no strict 'refs'; *$method = $code }
	$code->(@_)
}

1;
