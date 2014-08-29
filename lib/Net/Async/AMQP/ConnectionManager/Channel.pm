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
#	warn "Acquiring $self\n";
	$self->bus->subscribe_to_event(
		my @ev = (
			listener_start => $self->curry::weak::_listener_start,
			listener_stop  => $self->curry::weak::_listener_stop,
		)
	);
	$self->{cleanup}{events} = sub {
		shift->bus->unsubscribe_from_event(@ev);
		Future->wrap;
	};
	$self
}

sub _listener_start {
	my ($self, $ev, $ctag) = @_;
	$self->{cleanup}{"listener-$ctag"} = sub {
		my $self = shift;
#		warn "::: CLEANUP TASK - kill $ctag listener on $self\n";
		Net::Async::AMQP::Queue->new(
			amqp => $self->amqp,
			channel => $self,
			future => Future->wrap
		)->cancel(
			consumer_tag => $ctag
		)
	};
}

sub _listener_stop {
	my ($self, $ev, $ctag) = @_;
	# warn "No longer need to clean up $ctag listener\n" if DEBUG;
	delete $self->{cleanup}{"listener-$ctag"};
}

=head2 queue_declare

=cut

sub queue_declare {
	my ($self, %args) = @_;
	$self->channel->queue_declare(%args)->transform(
		done => sub {
			my ($q) = @_;
			# Ensure that this wrapped channel is used
			# as the stored channel value. This means
			# the channel holds the queue, we hold a weakref
			# to the channel, and the queue holds a strong
			# ref to our channel wrapper.
			$q->configure(channel => $self);
			$q
		}
	)
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
	sprintf "ManagedChannel[%d]", $self->id;
}

=head2 DESTROY

On destruction we release the channel by informing the connection manager
that we no longer require the data.

There may be some cleanup tasks required before we can release - cancelling
any trailing consumers, for example. These are held in the cleanup hash.

=cut

sub DESTROY {
	my $self = shift;
	unless($self->{cleanup}) {
#		warn "Releasing $self without cleanup\n";
		my $conman = delete $self->{manager};
		return $conman->release_channel(delete $self->{channel});
	}

#	warn "Releasing $self\n";
	my $f;
	$f = (
		fmap_void {
			$_->($self)
		} foreach => [
			sort values %{$self->{cleanup}}
		]
	)->on_ready(sub {
		my $conman = delete $self->{manager};
		$conman->release_channel(delete $self->{channel}) if $conman;
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
