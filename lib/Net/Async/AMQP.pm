package Net::Async::AMQP;
# ABSTRACT: IO::Async support for the AMQP protocol
use strict;
use warnings;

use parent qw(IO::Async::Notifier);

our $VERSION = '0.012';

=head1 NAME

Net::Async::AMQP - provides client interface to AMQP using L<IO::Async>

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::AMQP;
 my $loop = IO::Async::Loop->new;
 $loop->add(my $amqp = Net::Async::AMQP->new);
 $amqp->connect(
   host => 'localhost',
   user => 'guest',
   pass => 'guest',
   on_connected => sub { ... }
 );
 $loop->run;

=head1 DESCRIPTION

Does AMQP things. Note that the API may change before the stable 1.000
release - L</SEE ALSO> has some alternative modules if you're looking for
something that has been around for longer.

If you want a higher-level API which manages channels and connections, try
L<Net::Async::AMQP::ConnectionManager>.

=cut

use Net::AMQP;
use Net::AMQP::Common qw(:all);

use Future;
use curry::weak;
use Class::ISA ();
use List::Util qw(min);
use List::UtilsBy qw(extract_by);
use File::ShareDir ();
use Scalar::Util qw(weaken);
use Mixin::Event::Dispatch::Bus;

=head1 CONSTANTS

=head2 AUTH_MECH

Defines the mechanism used for authentication. Currently only AMQPLAIN
is supported.

=cut

use constant AUTH_MECH             => 'AMQPLAIN';

=head2 PAYLOAD_HEADER_LENGTH

Length of header used in payload messages. Defined by the AMQP standard
as 8 bytes.

=cut

use constant PAYLOAD_HEADER_LENGTH => 8;

=head2 MAX_FRAME_SIZE

Largest amount of data we'll attempt to send in a single frame. Actual
frame limit will be negotiated with the remote server. Defaults to 262144.

=cut

use constant MAX_FRAME_SIZE        => 262144;

=head2 MAX_CHANNELS

Maximum number of channels to request. Defaults to the AMQP limit (65535).
Attempting to set this any higher will not end well, it's an unsigned 16-bit
value.

=cut

use constant MAX_CHANNELS          => 65535;

=head2 DEBUG

Debugging flag - set C<PERL_AMQP_DEBUG> to 1 in the environment to enable
informational messages to STDERR.

=cut

use constant DEBUG                 => $ENV{PERL_AMQP_DEBUG} // 0;

=head2 HEARTBEAT_INTERVAL

Interval in seconds between heartbeat frames, zero to disable. Can be
overridden by C<PERL_AMQP_HEARTBEAT_INTERVAL> in the environment, default
is 0 (disabled).

=cut

use constant HEARTBEAT_INTERVAL    => $ENV{PERL_AMQP_HEARTBEAT_INTERVAL} // 0;

use Net::Async::AMQP::Channel;
use Net::Async::AMQP::Queue;

=head1 PACKAGE VARIABLES

=head2 $XML_SPEC

This defines the path to the AMQP XML spec, which L<Net::AMQP> uses
to create methods and handlers for the appropriate version of the MQ
protocol.

Defaults to an extended version of the 0.9.1 protocol as used by RabbitMQ,
this is found in the C<amqp0-9-1.extended.xml> distribution sharedir (see
L<File::ShareDir>).

Normally, you should be able to ignore this. If you want to load an alternative
spec, note that (a) this is global, rather than per-instance, (b) it needs to
be set before you C<use> this module.

 BEGIN { $Net::Async::AMQP::XML_SPEC = '/tmp/amqp.xml' }
 use Net::Async::AMQP;

Once loaded, this module will not attempt to apply the spec again.

=cut

our $XML_SPEC;
our $SPEC_LOADED;
BEGIN {
	$XML_SPEC //= File::ShareDir::dist_file(
		'Net-Async-AMQP',
		'amqp0-9-1.extended.xml'
	);

	# Load the appropriate protocol definitions. RabbitMQ uses a
	# modified version of AMQP 0.9.1
	Net::AMQP::Protocol->load_xml_spec($XML_SPEC) unless $SPEC_LOADED++;
}

=head1 %CONNECTION_DEFAULTS

The default parameters to use for L</connect>. Changing these values is permitted,
but do not attempt to delete or add any entries from the hash.

Passing parameters directly to L</connect> is much safer, please do that instead.

=cut

our %CONNECTION_DEFAULTS = (
    port => 5672,
    host => 'localhost',
    user => 'guest',
    pass => 'guest',
);

=head1 METHODS

=cut

=head2 configure

Set up variables. Takes the following optional named parameters:

=over 4

=item * heartbeat_interval - (optional) interval between heartbeat messages,
default is set by the L</HEARTBEAT_INTERVAL> constant

=back

Returns the new instance.

=cut

sub configure {
    my ($self, %args) = @_;
	for (qw(heartbeat_interval)) {
		$self->{$_} = delete $args{$_} if exists $args{$_}
	}
    $self->SUPER::configure(%args)
}

=head2 bus

Event bus. Used for sharing global events such as connection closure.

=cut

sub bus { $_[0]->{bus} ||= Mixin::Event::Dispatch::Bus->new }

=head2 connect

Takes the following parameters:

=over 4

=item * port - the AMQP port, defaults to 5672, can be a service name if preferred

=item * host - host to connect to, defaults to localhost

=item * local_host - our local IP to connect from

=item * user - which user to connect as, defaults to guest

=item * pass - the password for this user, defaults to guest

=item * on_connected - callback for when we establish a connection

=item * on_error - callback for any errors encountered during connection

=back

Returns $self.

=cut

sub connect {
    my $self = shift;
    my %args = @_;

    die 'no loop' unless my $loop = $self->loop;

    my $f = $self->loop->new_future;

    # Apply defaults
    $self->{$_} = $args{$_} // $CONNECTION_DEFAULTS{$_} for keys %CONNECTION_DEFAULTS;

	# Remember our event callbacks so we can unsubscribe
	my $connected;
	my $close;

	# Clean up once we succeed/fail
	$f->on_ready(sub {
		$self->bus->unsubscribe_from_event(close => $close) if $close;
		$self->bus->unsubscribe_from_event(connected => $connected) if $connected;
		undef $close;
		undef $connected;
		undef $self;
		undef $f;
	});

    # One-shot event on connection
    $self->bus->subscribe_to_event(connected => $connected = sub {
		$f->done($self) unless $f->is_ready;
    });
	# Also pick up connection termination
    $self->bus->subscribe_to_event(close => $close = sub {
		$f->fail('Remote closed connection') unless $f->is_ready;
    });

    $loop->connect(
        host     => $self->{host},
        # local_host can be used to send from a different source address,
        # sometimes useful for routing purposes
        (exists $args{local_host} ? (local_host => $args{local_host}) : ()),
        service  => $self->{port},
        socktype => 'stream',

        on_stream => $self->curry::on_stream(\%args),

        on_resolve_error => $f->curry::fail('resolve'),
        on_connect_error => $f->curry::fail('connect'),
    );
    $f;
}

sub on_stream {
    my ($self, $args, $stream) = @_;
	$self->debug_printf("Stream received");
    $self->{stream} = $stream;
    $stream->configure(
        on_read => $self->curry::on_read,
    );
    $self->add_child($stream);
    $self->apply_heartbeat_timer if $self->heartbeat_interval;
    $self->post_connect(%$args);
    return;
}

sub dump_frame {
	my ($self, $pkt) = @_;
	my ($type) = unpack 'C1', substr $pkt, 0, 1, '';
	printf "Type: %02x (%s)\n", $type, {
		1 => 'Method',
	}->{$type};

	my ($chan) = unpack 'n1', substr $pkt, 0, 2, '';
	printf "Channel: %d\n", $chan;

	my ($len) = unpack 'N1', substr $pkt, 0, 4, '';
	printf "Length: %d bytes\n", $len;

	if($type == 1) {
		my ($class, $method) = unpack 'n1n1', substr $pkt, 0, 4, '';
		printf "Class: %s\n", $class;
		printf "Method: %s\n", $method;
	}
}

sub on_read {
    my ($self, $stream, $buffref, $eof) = @_;
	# Frame dumping support - not that useful yet, so it's disabled
	if(0) {
		my $mem = $$buffref;
		$self->dump_frame($mem);
		my $idx = 0;
		while(length $mem) {
			my $hex = join ' ', unpack 'H2'x16, my $bytes = substr $mem, 0, 16, '';
			substr $hex, 8 * 3, 0, '  ';
			my $asc = join '', map /([[:print:]])/ ? $1 : '.', split //, $bytes;
			substr $asc, 8, 0, ' ';
			printf "%8d:  %-52.52s %s\n", $idx, $hex, $asc;
			$idx += length($asc);
		}
		print "\n";
		$self->debug_printf("At EOF") if $eof;
	}

	$self->last_frame_time($self->loop->time);

	# As each frame is parsed it will be removed from the buffer
    $self->process_frame($_) for Net::AMQP->parse_raw_frames($buffref);
    $self->on_closed if $eof;
    return 0;
}

sub on_closed {
	my $self = shift;
	my $reason = shift // 'unknown';
	$self->debug_printf("Connection closed [%s]", $reason);
	$self->stream->close if $self->stream;
	$self->bus->invoke_event(close => $reason)
}

=head2 post_connect

Sends initial startup header and applies listener for the Connection::Start message.

Returns $self.

=cut

sub post_connect {
    my $self = shift;
    my %args = @_;

    my %client_prop = (
        platform    => 'Perl/NetAsyncAMQP',
        product     => __PACKAGE__,
        information => $args{information} // 'http://search.cpan.org/perldoc?Net::Async::AMQP',
        version     => $VERSION,
		($args{client_properties} ? %{$args{client_properties}} : ()),
    );

    $self->push_pending(
        'Connection::Start' => sub {
            my ($self, $frame) = @_;
            my $method_frame = $frame->method_frame;
            my @mech = split ' ', $method_frame->mechanisms;
            die "Auth mechanism " . AUTH_MECH . " not supported, unable to continue - options were: @mech" unless grep $_ eq AUTH_MECH, @mech;
            my $output = Net::AMQP::Frame::Method->new(
                channel => 0,
                method_frame => Net::AMQP::Protocol::Connection::StartOk->new(
                    client_properties => \%client_prop,
                    mechanism         => AUTH_MECH,
                    locale            => $args{locale} // 'en_GB',
                    response          => {
                        LOGIN    => $args{user},
                        PASSWORD => $args{pass},
                    },
                ),
            );
            $self->setup_tuning(%args);
            $self->send_frame($output);
        }
    );

    # Send the initial header bytes. It'd be nice
	# if we could use L<Net::AMQP::Protocol/header>
	# for this, but it seems to be sending 1 for
	# the protocol ID, and the revision number is
	# before the major/minor version.
    # $self->write(Net::AMQP::Protocol->header);
    $self->write($self->header_bytes);
    $self
}

=head2 setup_tuning

Applies listener for the Connection::Tune message, used for determining max frame size and heartbeat settings.

Returns $self.

=cut

sub setup_tuning {
    my $self = shift;
    my %args = @_;
    $self->push_pending(
        'Connection::Tune' => sub {
            my ($self, $frame) = @_;
            my $method_frame = $frame->method_frame;
            # Lowest value for frame max wins - our predef constant, or whatever the server suggests
            $self->frame_max(my $frame_max = min $method_frame->frame_max, MAX_FRAME_SIZE);
			$self->channel_max(my $channel_max = $method_frame->channel_max || $self->channel_max || MAX_CHANNELS);
			$self->debug_printf("Remote says %d channels, will use %d", $method_frame->channel_max, $channel_max);
			$self->{channel} = 0;
            $self->send_frame(
                Net::AMQP::Protocol::Connection::TuneOk->new(
                    channel_max => $channel_max,
                    frame_max   => $frame_max,
                    heartbeat   => $self->heartbeat_interval,
                )
            );
            $self->open_connection(%args);
        }
    );
}

=head2 open_connection

Establish a new connection to a vhost - this is called after tuning is complete,
and must happen before any channel connections are attempted.

Returns $self.

=cut

sub open_connection {
    my $self = shift;
    my %args = @_;
    $self->setup_connection(%args);
    $self->send_frame(
        Net::AMQP::Frame::Method->new(
            method_frame => Net::AMQP::Protocol::Connection::Open->new(
                virtual_host => $args{vhost} // '/',
                capabilities => '',
                insist       => 1,
            ),
        )
    );
    $self
}

=head2 setup_connection

Applies listener for the Connection::OpenOk message, which triggers the
C<connected> event.

Returns $self.

=cut

sub setup_connection {
    my $self = shift;
    my %args = @_;
    $self->push_pending(
        'Connection::OpenOk' => sub {
            my ($self, $frame) = @_;
            my $method_frame = $frame->method_frame;
			$self->debug_printf("OpenOk received");
            $self->bus->invoke_event(connected =>);
        }
    );
    $self
}

=head2 next_channel

Returns the next available channel ready for L</open_channel>.
Note that whatever it reports will be completely wrong if you've
manually specified a channel anywhere, so don't do that.

=cut

sub next_channel {
    my $self = shift;
	$self->{channel} //= 0;
	return undef if $self->{channel} >= $self->channel_max;
    ++$self->{channel}
}

=head2 open_channel

Opens a new channel.

Returns the new L<Net::Async::AMQP::Channel> instance.

=cut

sub open_channel {
    my $self = shift;
    my %args = @_;
    my $f = $self->loop->new_future;
    my $channel = $args{channel} // $self->next_channel;
	die "Channel " . $channel . " exists already" if exists $self->{channel_map}{$channel};
	$self->{channel_map}{$channel} = $f;

    my $frame = Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Channel::Open->new,
    );
    $frame->channel($channel);
    $self->add_child(my $c = Net::Async::AMQP::Channel->new(
        amqp   => $self,
        future => $f,
        id     => $channel,
    ));
    $self->{channel_by_id}{$channel} = $c;
	$self->debug_printf("Record channel %d as %s", $channel, $c);
    $c->push_pending(
        'Channel::OpenOk' => sub {
            my ($c, $frame) = @_;
            {
                my $method_frame = $frame->method_frame;
                $self->{channel_map}{$frame->channel} = $c;
                $f->done($c) unless $f->is_ready;
            }
            weaken $f;
        }
    );
    $self->send_frame($frame);
    return $f;
}

=head2 close

Close the connection.

Returns a L<Future> which will resolve with C<$self> when the connection is closed.

=cut

sub close {
    my $self = shift;
    my %args = @_;

	$self->heartbeat_send_timer->stop if $self->heartbeat_send_timer;

    my $f = $self->loop->new_future;

	# We might end up with a connection shutdown rather
	# than a clean Connection::Close response, so
	# we need to handle both possibilities
	$self->bus->subscribe_to_event(
		my @handler = (
			close => sub {
				my ($ev, $reason) = @_;
				$f->done($reason) unless $f->is_ready;
				$ev->unsubscribe;
				weaken $f;
			}
		)
	);

	# ... and make sure we clean up after ourselves
	$f->on_ready(sub {
		$self->bus->unsubscribe_from_event(
			@handler
		);
		weaken $f;
	});

    my $frame = Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Connection::Close->new(
			reply_code => $args{code} // 320,
			reply_text => $args{text} // 'Request connection close',
		),
    );
    $self->push_pending(
        'Connection::CloseOk' => [ $f, $self ],
    );
    $self->send_frame($frame);
    return $f;
}

sub channel_closed {
    my ($self, $id) = @_;
	my $ch = delete $self->{channel_map}{$id}
		or die "Had a close indication for channel $id but this channel is unknown";
    delete $self->{channel_by_id}{$ch};
    $self
}

sub channel_by_id { my $self = shift; $self->{channel_by_id}{+shift} }

=head2 next_pending

Retrieves the next pending handler for the given incoming frame type (see L</get_frame_type>),
and calls it.

Takes the following parameters:

=over 4

=item * $type - the frame type, such as 'Basic::ConnectOk'

=item * $frame - the frame itself

=back

Returns $self.

=cut

sub next_pending {
    my ($self, $type, $frame) = @_;
    $self->debug_printf("Check next pending for %s", $type);

    if(my $next = shift @{$self->{pending}{$type} || []}) {
		# We have a registered handler for this frame type. This usually
		# means that we've sent a frame and are awaiting a response.
		if(ref($next) eq 'ARRAY') {
			my ($f, @args) = @$next;
			$f->done(@args) unless $f->is_ready;
		} else {
			$next->($self, $frame, @_);
		}
	} else {
		# It's quite possible we'll see unsolicited frames back from
		# the server: these will typically be errors, connection close,
		# or consumer cancellation if the consumer_cancel_notify
		# option is set (RabbitMQ). We don't expect many so report
		# them when in debug mode.
		$self->debug_printf("We had no pending handlers for %s, raising as event", $type);
		$self->bus->invoke_event(
			unexpected_frame => $type, $frame
		);
	}
    $self
}

=head1 METHODS - Accessors

=head2 host

The current host.

=cut

sub host { shift->{host} }

=head2 vhost

Virtual host.

=cut

sub vhost { shift->{vhost} }

=head2 port

Port number. Usually 5672.

=cut

sub port { shift->{port} }

=head2 user

MQ user.

=cut

sub user { shift->{user} }

=head2 frame_max

Maximum number of bytes allowed in any given frame. This is the
value negotiated with the remote server.

=cut

sub frame_max {
    my $self = shift;
    return $self->{frame_max} unless @_;

    $self->{frame_max} = shift;
    $self
}

=head2 channel_max

Maximum number of channels. This is whatever we ended up with after initial negotiation.

=cut

sub channel_max {
    my $self = shift;
    return $self->{channel_max} unless @_;

    $self->{channel_max} = shift;
    $self
}

=head2 last_frame_time

Timestamp of the last frame we received from the remote. Used for handling heartbeats.

=cut

sub last_frame_time {
    my $self = shift;
    return $self->{last_frame_time} unless @_;

    $self->{last_frame_time} = shift;
	$self->heartbeat_receive_timer->reset if $self->heartbeat_receive_timer;
    $self
}

=head2 stream

Returns the current L<IO::Async::Stream> for the AMQP connection.

=cut

sub stream { shift->{stream} }

=head2 incoming_message

L<Future> for the current incoming message (received in two or more parts:
the header then all body chunks).

=cut

sub incoming_message { shift->{incoming_message} }

=head1 METHODS - Internal

The following methods are intended for internal use. They are documented
for completeness but should not normally be needed outside this library.

=cut

=head2 heartbeat_interval

Current maximum interval between frames.

=cut

sub heartbeat_interval { shift->{heartbeat_interval} //= HEARTBEAT_INTERVAL }

=head2 missed_heartbeats_allowed

How many times we allow the remote to miss the frame-sending deadline in a row
before we give up and close the connection. Defined by the protocol, should be
3x heartbeats.

=cut

sub missed_heartbeats_allowed { 3 }

=head2 apply_heartbeat_timer

Enable both heartbeat timers.

=cut

sub apply_heartbeat_timer {
    my $self = shift;
	{ # On expiry, will trigger a heartbeat send from us to the server
		my $timer = IO::Async::Timer::Countdown->new(
			delay     => $self->heartbeat_interval,
			on_expire => $self->curry::weak::send_heartbeat,
		);
		$self->add_child($timer);
		$timer->start;
		Scalar::Util::weaken($self->{heartbeat_send_timer} = $timer);
	}
	{ # This timer indicates no traffic from the remote for 3*heartbeat
		my $timer = IO::Async::Timer::Countdown->new(
			delay     => $self->missed_heartbeats_allowed * $self->heartbeat_interval,
			on_expire => $self->curry::weak::handle_heartbeat_failure,
		);
		$self->add_child($timer);
		$timer->start;
		Scalar::Util::weaken($self->{heartbeat_receive_timer} = $timer);
	}
    $self
}

=head2 reset_heartbeat

Resets our side of the heartbeat timer.

This is used to ensure we send data at least once every L</heartbeat_interval>
seconds.

=cut

sub reset_heartbeat {
    my $self = shift;
    return unless my $timer = $self->heartbeat_send_timer;

    $timer->reset;
}


=head2 heartbeat_receive_timer

Timer for tracking frames we've received.

=cut

sub heartbeat_receive_timer { shift->{heartbeat_receive_timer} }

=head2 heartbeat_send_timer

Timer for tracking when we're due to send out something.

=cut

sub heartbeat_send_timer { shift->{heartbeat_send_timer} }

=head2 handle_heartbeat_failure

Called when heartbeats are enabled and we've had no response from the server for 3 heartbeat
intervals (see L</missed_heartbeats_allowed>). We'd expect some frame from the remote - even
if just a heartbeat frame - at least once every heartbeat interval so if this triggers then
we're likely dealing with a dead or heavily loaded server.

This will invoke the L</heartbeat_failure event> then close the connection.

=cut

sub handle_heartbeat_failure {
	my $self = shift;
	$self->debug_printf("Heartbeat timeout: no data received from server since %s, closing connection", $self->last_frame_time);

	$self->bus->invoke_event(
		heartbeat_failure => $self->last_frame_time
	);
	$self->close;
}

=head2 send_heartbeat

Sends the heartbeat frame.

=cut

sub send_heartbeat {
    my $self = shift;
	$self->debug_printf("Sending heartbeat frame");

    # Heartbeat messages apply to the connection rather than
    # individual channels, so we use channel 0 to represent this
    $self->send_frame(
        Net::AMQP::Frame::Heartbeat->new,
        channel => 0,
    );

	# Ensure heartbeat timer is active for next time
	if(my $timer = $self->heartbeat_send_timer) {
		$timer->reset;
		$timer->start;
	}
}

=head2 push_pending

Adds the given handler(s) to the pending handler list for the given type(s).

Takes one or more of the following parameter pairs:

=over 4

=item * $type - the frame type, see L</get_frame_type>

=item * $code - the coderef to call, will be invoked once as follows when a matching frame is received:

 $code->($self, $frame, @_)

=back

Returns C< $self >.

=cut

sub push_pending {
    my $self = shift;
    while(@_) {
        my ($type, $code) = splice @_, 0, 2;
        push @{$self->{pending}{$type}}, $code;
    }
    return $self;
}

=head2 remove_pending

Removes a coderef from the pending event handler.

Returns C< $self >.

=cut

sub remove_pending {
	my $self = shift;
    while(@_) {
        my ($type, $code) = splice @_, 0, 2;
		# This is the same as extract_by { $_ eq $code } @{$self->{pending}{$type}};,
		# but since we'll be calling it a lot might as well do it inline:
		splice
			@{$self->{pending}{$type}},
			$_,
			1 for grep {
				$self->{pending}{$type}[$_] eq $code
			} reverse 0..$#{$self->{pending}{$type}};
    }
    return $self;
}

=head2 write

Writes data to the server.

=cut

sub write {
    my $self = shift;
    $self->stream->write(@_);
    $self
}

=head2 get_frame_type

Takes the following parameters:

=over 4

=item * $frame - the L<Net::AMQP::Frame> instance

=back

Returns string representing type, typically the base class with Net::AMQP::Protocol prefix removed.

=cut

{ # We cache the lookups since they're unlikely to change during the application lifecycle
my %types;
sub get_frame_type {
    my ($self, $raw_frame) = @_;
	return 'Heartbeat' if $raw_frame->isa('Net::AMQP::Frame::Heartbeat');
	return 'Header' if $raw_frame->isa('Net::AMQP::Frame::Header');
	return 'Body' if $raw_frame->isa('Net::AMQP::Frame::Body');

    my $frame = $raw_frame->method_frame;
    my $ref = ref $frame;
    return $types{$ref} if exists $types{$ref};
    my $re = qr/^Net::AMQP::Protocol::([^:]+::[^:]+)$/;
    my ($frame_type) = grep /$re/, Class::ISA::self_and_super_path($ref);
    ($frame_type) = $frame_type =~ $re;
    $types{$ref} = $frame_type;
    return $frame_type;
}
}

=head2 process_frame

Process a single incoming frame.

Takes the following parameters:

=over 4

=item * $frame - the L<Net::AMQP::Frame> instance

=back

Returns $self.

=cut

sub process_frame {
    my ($self, $frame) = @_;

    my $frame_type = $self->get_frame_type($frame);

	if($frame_type eq 'Heartbeat') {
		# Ignore these completely. Since we have the last frame update at the data-read
		# level, there's nothing for us to do here.
		$self->debug_printf("Heartbeat received");

		# A peer that receives an invalid heartbeat frame MUST raise a connection
		# exception with reply code 501 (frame error)
		$self->send_frame(
			Net::AMQP::Frame::Method->new(
				method_frame => Net::AMQP::Protocol::Connection::Close->new(
					reply_code => 501,
					reply_text => 'Frame error - heartbeat should have channel 0'
				)
			)
		) if $frame->channel;
		return $self;
	} elsif(my $ch = $self->channel_by_id($frame->channel)) {
		$self->debug_printf("Processing frame %s on channel %d", $frame_type, $ch);
		return $self if $ch->next_pending($frame);
	}

	$self->debug_printf("Processing connection frame %s", $frame_type);

	$self->next_pending($frame_type, $frame);

    return $self;
}

=head2 split_payload

Splits a message into separate frames.

Takes the $payload as a scalar containing byte data, and the following parameters:

=over 4

=item * exchange - where we're sending the message

=item * routing_key - other part of message destination

=back

Returns list of frames suitable for passing to L</send_frame>.

=cut

sub split_payload {
    my $self = shift;
    my $payload = shift;
    my %opts = @_;

    # Get the original content length first
    my $payload_size = length $payload;

    my @body_frames;
    while (length $payload) {
        my $chunk = substr $payload, 0, $self->frame_max - PAYLOAD_HEADER_LENGTH, '';
        push @body_frames, Net::AMQP::Frame::Body->new(
            payload => $chunk
        );
    }

    return
        Net::AMQP::Protocol::Basic::Publish->new(
            map {; $_ => $opts{$_} } grep defined($opts{$_}), qw(ticket exchange routing_key mandatory immediate)
        ),
        Net::AMQP::Frame::Header->new(
            weight       => $opts{weight} || 0,
            body_size    => $payload_size,
            header_frame => Net::AMQP::Protocol::Basic::ContentHeader->new(
                map {; $_ => $opts{$_} } grep defined($opts{$_}), qw(
                    content_type
                    content_encoding
                    headers
                    delivery_mode
                    priority
                    correlation_id
                    reply_to
                    expiration
                    message_id
                    timestamp
                    type
                    user_id
                    app_id
                    cluster_id
                )
            ),
        ),
        @body_frames;
}

=head2 send_frame

Send a single frame.

Takes the $frame instance followed by these optional named parameters:

=over 4

=item * channel - which channel we should send on

=back

Returns $self.

=cut

sub send_frame {
    my $self = shift;
    my $frame = shift;
    my %args = @_;

    # Apply defaults and wrap as required
    $frame = $frame->frame_wrap if $frame->isa("Net::AMQP::Protocol::Base");
    $frame->channel($args{channel} // 0) unless defined $frame->channel;
#    warn "Sending frame " . Dumper($frame) if DEBUG;

    # Get bytes to send across our transport
    my $data = $frame->to_raw_frame;

#    warn "Sending data: " . Dumper($frame) . "\n";
    $self->write($data);
	$self->reset_heartbeat;
    $self;
}

sub header_bytes { "AMQP\x00\x00\x09\x01" }

sub _add_to_loop {
	my ($self, $loop) = @_;
	$self->debug_printf("Added %s to loop", $self);
}

=head1 future

Returns a new L<IO::Async::Future> instance.

Supports optional named parameters for setting label etc.

=cut

sub future {
	my $self = shift;
	my $f = $self->loop->new_future;
	while(my ($k, $v) = splice @_, 0, 2) {
		$f->can($k) ? $f->$k($v) : $self->debug_printf("Unable to call method $k on $f");
	}
	$f
}

1;

__END__

=head1 EVENTS

The following events may be raised by this class - use
L<Mixin::Event::Dispatch/subscribe_to_event> to watch for them:

 $mq->bus->subscribe_to_event(
   heartbeat_failure => sub {
     my ($ev, $last) = @_;
	 print "Heartbeat failure detected\n";
   }
 );

=head2 connected event

Called after the connection has been opened.

=head2 close event

Called after the remote has closed the connection.

=head2 heartbeat_failure event

Raised if we receive no data from the remote for more than 3 heartbeat intervals and heartbeats are enabled,

=head2 unexpected_frame event

If we receive an unsolicited frame from the server this event will be raised:

 $mq->bus->subscribe_to_event(
  unexpected_frame => sub {
   my ($ev, $type, $frame) = @_;
   warn "Frame type $type received: $frame\n";
  }
 )

=head1 SEE ALSO

=over 4

=item * L<Net::AMQP> - this does all the hard work of converting the XML protocol
specification into appropriate Perl methods and classes.

=item * L<Net::AMQP::RabbitMQ> - librabbitmq support

=item * L<POE::Component::Client::AMQP> - POE equivalent of this module

=item * L<AnyEvent::RabbitMQ>

=back

=head1 AUTHOR

Tom Molesworth <cpan@perlsite.co.uk>

=head1 LICENSE

Licensed under the same terms as Perl itself, with additional licensing
terms for the MQ spec to be found in C<share/amqp0-9-1.extended.xml>
('a worldwide, perpetual, royalty-free, nontransferable, nonexclusive
license to (i) copy, display, distribute and implement the Advanced
Messaging Queue Protocol ("AMQP") Specification').

