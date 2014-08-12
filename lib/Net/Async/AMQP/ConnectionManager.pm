package Net::Async::AMQP::ConnectionManager;

use strict;
use warnings;

use parent qw(IO::Async::Notifier);

=head1 NAME

Net::Async::AMQP::ConnectionManager - handle MQ connections

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::AMQP;
 my $loop = IO::Async::Loop->new;
 $loop->add(
  my $cm = Net::Async::AMQP::ConnectionManager->new
 );
 $cm->add(
   host  => 'localhost',
   user  => 'guest',
   pass  => 'guest',
   vhost => 'vhost',
 );
 $cm->request_channel->then(sub {
   my $ch = shift;
   Future->needs_all(
     $ch->declare_exchange(
       'exchange_name'
     ),
     $ch->declare_queue(
       'queue_name'
     ),
   )->transform(done => sub { $ch })
 })->then(sub {
   my $ch = shift;
   $ch->bind_queue(
     'exchange_name',
	 'queue_name',
	 '*'
   )
 })->get;

=cut

use Future;
use Future::Utils qw(call try_repeat fmap_void);

use Time::HiRes ();

use Net::Async::AMQP;
use Net::Async::AMQP::ConnectionManager::Channel;
use Net::Async::AMQP::ConnectionManager::Connection;

=head1 DESCRIPTION

=head2 Channel management

Each connection has N total available channels, recorded in a hash. The total number
of channels per connection is negotiated via the intial AMQP Tune/TuneOk sequence on
connection.

We also maintain lists:

=over 4

=item * Unassigned channel - these are channels which were in use and have now been released.

=item * Closed channel - any time a channel is closed, the ID is pushed onto this list so we can reopen it later without needing to scan the hash, contains arrayrefs of [$mq_conn, $id]

=back

Highest-assigned ID is also recorded per connection.

if(have unassigned) {
	return shift unassigned
} elsif(have closed) {
	my $closed = shift closed;
	return $closed->{mq}->open_channel($closed->{id})
} elsif(my $next_id = $mq->next_id) {
	return $mq->open_channel($next_id)
} else {

}

Calling methods on the channel proxy will establish
a cycle for the duration of the pending request.
This cycle will not be resolved until after all
the callbacks have completed for a given request.

The channel object does not expose any methods that allow
altering QoS or other channel state settings. These must be
requested on channel assignment. This does not necessarily
mean that any QoS change will require allocation of a new
channel.

Bypassing the proxy object to change QoS flags is not recommended.

=head2 Connection pool

Connections are established on demand.

=head1 METHODS

=head2 request_channel

Attempts to assign a channel with the given QoS settings.

Will resolve to the channel object on success.

=cut

sub request_channel {
	my $self = shift;
	my %args = @_;

	die "We are shutting down" if $self->{shutdown_future};

	# Assign channel with matching QoS if available
	my $k = $self->key_for_args(\%args);
	if(exists $self->{channel_by_key}{$k} && @{$self->{channel_by_key}{$k}}) {
		my $ch = shift @{$self->{channel_by_key}{$k}};
		return Future->wrap(
			Net::Async::AMQP::ConnectionManager::Channel->new(
				channel => $ch,
				manager => $self,
			)
		)
	}

	# If we get here, we don't have an appropriate channel already available,
	# so whichever means we use to obtain a channel will need to set QoS afterwards
	my $f;

	if(exists $self->{closed_channel} && @{$self->{closed_channel}}) {
		# If we have an ID for a closed channel then reuse that first.
		my ($mq, $id) = @{shift @{$self->{closed_channel}}};
		$f = $mq->open_channel(
			channel => $id
		);
	} else {
		# Try to get a channel - limit this to 3 attempts
		my $count = 0;
		$f = try_repeat {
			$self->request_connection->then(sub {
				my $mq = shift;
				call {
					# If we have any spare IDs on this connection, attempt to open
					# a channel here
					if(my $id = $mq->next_channel) {
						return $mq->open_channel(
							channel => $id
						)
					}

					# No spare IDs, so record this to avoid hitting this MQ connection
					# on the next request as well
					$self->mark_connection_full($mq);

					# We can safely fail at this point, since we're in a loop and the
					# next iteration should get a new MQ connection to try with
					Future->fail(channel => 'no spare channels on connection');
				}
			});
		} until => sub { shift->is_done || ++$count > 3 };
	}

	# Apply our QoS on the channel if we ever get one
	return $f->then(sub {
		my $ch = shift;
		call {
			$ch->subscribe_to_event(
				close => $self->curry::weak::on_channel_close($ch),
			);
			$self->apply_qos($ch => %args)
		}
	})->set_label(
		'Channel QoS'
	)->transform(
		done => sub {
			my $ch = shift;
			$self->{channel_args}{$ch->id} = \%args;
			Net::Async::AMQP::ConnectionManager::Channel->new(
				channel => $ch,
				manager => $self,
			)
		}
	);
}

=head2 apply_qos

Set QoS on the given channel.

Expects the L<Net::Async::AMQP::Channel> object as the first
parameter, followed by the key/value pairs corresponding to
the desired QoS settings:

=over 4

=item * prefetch_count - number of messages that can be delivered before ACK
is required

=back

Returns a L<Future> which will resolve to the original 
L<Net::Async::AMQP::Channel> instance.

=cut

sub apply_qos {
	my ($self, $ch, %args) = @_;
	(fmap_void {
		my $k = shift;
		my $v = $args{$k};
		my $method = "qos_$k";
		my $code = $self->can($method) or die "Unknown QoS setting $k (value $v)";
		$code->($self, $ch, $k => $v);
	} foreach => [
		sort keys %args
	])->transform(
		done => sub { $ch }
	)->set_label(
		'Apply QoS settings'
	);
}

sub qos_prefetch_size {
	my ($self, $ch, $k, $v) = @_;
	return $ch->qos(
		$k => $args{$k}
	)->set_label("Apply $k QoS");
}

sub qos_prefetch_count {
	my ($self, $ch, $k, $v) = @_;
	return $ch->qos(
		$k => $args{$k}
	)->set_label("Apply $k QoS");
}

sub qos_confirm_mode {
	my ($self, $ch, $k, $v) = @_;
	return $ch->confirm_mode(
		$k => $args{$k}
	)->set_label("Apply $k QoS");
}

=head2 request_connection

Attempts to connect to one of the known AMQP servers.

=cut

sub request_connection {
	my ($self) = @_;
	die "We are shutting down" if $self->{shutdown_future};
	if(my $conn = $self->{pending_connection}) {
		return $conn
	}

	if(exists $self->{available_connections} && @{$self->{available_connections}}) {
		$self->debug_printf("Assigning existing connection");
		return Future->wrap(
			Net::Async::AMQP::ConnectionManager::Connection->new(
				amqp    => shift @{$self->{available_connections}},
				manager => $self,
			)
		)
	}
	die "No connection details available" unless $self->{amqp_host};

	$self->{pending_connection} = $self->connect(
		%{$self->next_host}
	)->on_ready(sub {
		delete $self->{pending_connection};
	})->transform(
		done => sub {
			my $mq = shift;
			Net::Async::AMQP::ConnectionManager::Connection->new(
				amqp    => $mq,
				manager => $self,
			)
		}
	)->set_label(
		'Connect to MQ server'
	)
}

=head2 next_host

Returns the next AMQP host.

=cut

sub next_host {
	my $self = shift;
	$self->{amqp_host}[rand @{$self->{amqp_host}}]
}

=head2 connect

Attempts a connection to an AMQP host.

=cut

sub connect {
	my ($self, %args) = @_;
	die "We are shutting down" if $self->{shutdown_future};
	my $amqp = Net::Async::AMQP->new(
		loop => $self->loop,
	);
	$args{port} ||= 5672;
	$amqp->connect(
		%args
	)
}

=head2 mark_connection_full

Indicate that this connection has already allocated all available
channels.

=cut

sub mark_connection_full {
	my ($self, $mq) = @_;

}

=head2 key_for_args

Returns a key that represents the given arguments.

=cut

sub key_for_args {
	my ($self, $args) = @_;
	join ',', map { "$_=$args->{$_}" } sort keys %$args;
}

=head2 on_channel_close

Called when one of our channels has been closed.

=cut

sub on_channel_close {
	my ($self, $ch, $ev, %args) = @_;
	$self->debug_printf("channel closure: %s", join ' ', @_);
	my $amqp = $ch->amqp or die "This channel (" . $ch->id . ") has no AMQP connection";
	push @{$self->{closed_channel}}, [ $amqp, $ch->id ];
}

=head2 release_channel

Releases the given channel back to our channel pool.

=cut

sub release_channel {
	my ($self, $ch) = @_;
	my $args = $self->{channel_args}{$ch->id};
	my $k = $self->key_for_args($args);
	push @{$self->{channel_by_key}{$k}}, $ch;
	$self
}

=head2 add

Adds connection details for an AMQP server to the pool.

=cut

sub add {
	my ($self, %args) = @_;
	push @{$self->{amqp_host}}, \%args;
}

=head2 exch

=cut

sub exch {
	my ($self, $exch) = @_;
	return $self->{exchange}{$exch} if exists $self->{exchange}{$exch};
	$self->{exchange}{$exch} = $self->request_channel->then(sub {
		my $ch = shift;
		$ch->declare_exchange(
			$exch
		)
	});
}

sub queue {
	my ($self, $q) = @_;
	return $self->{queue}{$q} if exists $self->{queue}{$q};
	$self->{queue}{$q} = $self->request_channel->then(sub {
		my $ch = shift;
		$ch->declare_queue(
			$q
		)
	});
}

sub release_connection {
	my ($self, $mq) = @_;
	$self->debug_printf("Releasing connection %s", $mq);
	push @{$self->{available_connections}}, $mq;
}

sub shutdown {
	my $self = shift;
	$self->debug_printf("Shutdown started");
	die "Shutdown already in progress?" if $self->{shutdown_future};
	my $start = [Time::HiRes::gettimeofday];
	$self->{shutdown_future} = Future->wait_all(
		map $_->close, @{$self->{available_connections}}
	)->on_ready(sub {
		delete $self->{shutdown_future};
	})->on_done(sub {
		$self->debug_printf("All connections closed - elapsed %.3fs", Time::HiRes::tv_interval($start, [Time::HiRes::gettimeofday]));
	});
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>
