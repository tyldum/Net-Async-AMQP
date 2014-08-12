package Net::Async::AMQP;
# ABSTRACT: IO::Async support for the AMQP protocol
use strict;
use warnings;

use parent qw(Mixin::Event::Dispatch);
use constant EVENT_DISPATCH_ON_FALLBACK => 0;

our $VERSION = '0.004';

=head1 NAME

Net::Async::AMQP - provides client interface to AMQP using L<IO::Async>

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::AMQP;
 my $amqp = Net::Async::AMQP->new(loop => my $loop = IO::Async::Loop->new);
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

=head1 CONSTANTS

=head2 AUTH_MECH

Defines the mechanism used for authentication. Currently only AMQPLAIN
is supported.

=cut

use constant AUTH_MECH             => 'AMQPLAIN';

=head2 PAYLOAD_HEADER_LENGTH

Length of header used in payload messages. Defined by the AMQP standard.

=cut

use constant PAYLOAD_HEADER_LENGTH => 8;

=head2 MAX_FRAME_SIZE

Largest amount of data we'll attempt to send in a single frame. Actual
frame limit will be negotiated with the remote server.

=cut

use constant MAX_FRAME_SIZE        => 262144;

=head2 MAX_CHANNELS

Maximum number of channels to request. Defaults to the AMQP limit (65535).
Attempting to set this any higher will not end well.

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

# Heartbeat support - the frame type isn't handled by the factory, but
# there's a basic subclass in place so we just need to map the type_id here.
# This is already supported in version 0.06 onwards.
BEGIN {
	if(Net::AMQP->VERSION < 0.06) {
		my $factory = sub {
			my ($class, %args) = @_;

			unless (exists $args{type_id}) { die "Mandatory parameter 'type_id' missing in call to Net::AMQP::Frame::factory"; }
			unless (exists $args{channel}) { die "Mandatory parameter 'channel' missing in call to Net::AMQP::Frame::factory"; }
			unless (exists $args{payload}) { die "Mandatory parameter 'payload' missing in call to Net::AMQP::Frame::factory"; }
			unless (keys %args == 3)       { die "Invalid parameter passed in call to Net::AMQP::Frame::factory"; }

			my $subclass;
			if ($args{type_id} == 1) {
				$subclass = 'Method';
			}
			elsif ($args{type_id} == 2) {
				$subclass = 'Header';
			}
			elsif ($args{type_id} == 3) {
				$subclass = 'Body';
			} elsif ($args{type_id} == 8) {
				$subclass = 'Heartbeat';
			}
			else {
				die "Unknown type_id $args{type_id}";
			}

			$subclass = 'Net::AMQP::Frame::' . $subclass;
			my $object = bless \%args, $subclass;
			$object->parse_payload();
			return $object;
		};
		{ no strict 'refs'; no warnings 'redefine'; *{'Net::AMQP::Frame::factory'} = $factory; }
	}

}

=head1 PACKAGE VARIABLES

=head2 $XML_SPEC

This defines the path to the AMQP XML spec, which L<Net::AMQP> uses
to create methods and handlers for the appropriate version of the MQ
protocol.

Defaults to an extended version of the 0.9.1 protocol as used by RabbitMQ,
this is found in the C<amqp0-9-1.extended.xml> distribution sharedir (see
L<File::ShareDir>).

=cut

our $XML_SPEC;
BEGIN {
	$XML_SPEC //= File::ShareDir::dist_file(
		'Net-Async-AMQP',
		'amqp0-9-1.extended.xml'
	);

	# Load the appropriate protocol definitions. RabbitMQ uses a
	# modified version of AMQP 0.9.1
	Net::AMQP::Protocol->load_xml_spec($XML_SPEC);
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

=head2 new

Constructor. Takes the following parameters:

=over 4

=item * loop - the L<IO::Async::Loop> which we should add ourselves to

=item * heartbeat_interval - (optional) interval between heartbeat messages,
default is set by the L</HEARTBEAT_INTERVAL> constant

=back

Returns the new instance.

=cut

sub new {
    my $class = shift;
    my $self = bless {
		# Apply default heartbeat value, can be overriden
        heartbeat_interval => HEARTBEAT_INTERVAL,
        @_
    }, $class;
	die "no loop provided" unless $self->{loop};
	weaken($self->{loop});
    $self
}

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
		$self->unsubscribe_from_event(close => $close) if $close;
		$self->unsubscribe_from_event(connected => $connected) if $connected;
		undef $close;
		undef $connected;
		undef $self;
		undef $f;
	});

    # One-shot event on connection
    $self->subscribe_to_event(connected => $connected = sub {
        my $ev = shift;
		$f->done($ev->instance) unless $f->is_ready;
    });
	# Also pick up connection termination
    $self->subscribe_to_event(close => $close = sub {
        my $ev = shift;
		$f->fail('Remote closed connection') unless $f->is_ready;
    });

    $loop->connect(
        host     => $args{host},
        # local_host can be used to send from a different source address,
        # sometimes useful for routing purposes
        (exists $args{local_host} ? (local_host => $args{local_host}) : ()),
        service  => $args{port},
        socktype => 'stream',

        on_stream => $self->curry::on_stream(\%args),

        on_resolve_error => $f->curry::fail('resolve'),
        on_connect_error => $f->curry::fail('connect'),
    );
    $f;
}

sub on_stream {
    my ($self, $args, $stream) = @_;
    warn "We're in: $stream\n" if DEBUG;
    $self->{stream} = $stream;
    $stream->configure(
        on_read => $self->curry::on_read,
    );
    $self->loop->add($stream);
    $self->apply_heartbeat_timer if $self->heartbeat_interval;
    $self->post_connect(%$args);
    return;
}

sub on_read {
    my ($self, $stream, $buffref, $eof) = @_;
    warn "EOF" if DEBUG && $eof;

	$self->last_frame_time($self->loop->time);

	# As each frame is parsed it will be removed from the buffer
    $self->process_frame($_) for Net::AMQP->parse_raw_frames($buffref);
    $self->on_closed if $eof;
    return 0;
}

sub on_closed {
	my $self = shift;
	my $reason = shift // 'unknown';
    warn "Connection closed (reason: $reason)" if DEBUG;
	$self->stream->close if $self->stream;
	$self->invoke_event(close => $reason)
}

sub heartbeat_interval { shift->{heartbeat_interval} }

sub apply_heartbeat_timer {
    my $self = shift;
	# IO::Async::Timer::Countdown
    my $timer = IO::Async::Timer::Countdown->new(
        delay     => $self->heartbeat_interval,
        on_expire => $self->curry::weak::send_heartbeat,
    );
    $self->loop->add($timer);
    $timer->start;
    Scalar::Util::weaken($self->{heartbeat_timer} = $timer);
    $self
}

sub heartbeat_timer { shift->{heartbeat_timer} }

=head2 handle_heartbeat_failure

Called when heartbeats are enabled and we've had no response from the server for 3 heartbeat
intervals. We'd expect some frame from the remote - even if just a heartbeat frame - at least
once every heartbeat interval so if this triggers then we're likely dealing with a dead or
heavily loaded server.

This will invoke the L</heartbeat_failure event> then close the connection.

=cut

sub handle_heartbeat_failure {
	my $self = shift;
    warn "Heartbeat timeout: no data received from server since " . $self->last_frame_time . ", closing connection\n";
	$self->heartbeat_timer->stop if $self->heartbeat_timer;
	$self->invoke_event(heartbeat_failure => $self->last_frame_time);
	$self->close;
}

=head2 send_heartbeat

Sends the heartbeat frame.

=cut

sub send_heartbeat {
    my $self = shift;

    # Heartbeat messages apply to the connection rather than
    # individual channels, so we use channel 0 to represent this
    $self->send_frame(
        Net::AMQP::Frame::Heartbeat->new,
        channel => 0,
    );
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

    # Send the initial header bytes
    $self->write(Net::AMQP::Protocol->header);
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
            warn "we are open for business" if DEBUG;
            $self->invoke_event(connected =>);
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
	die "This channel exists already" if exists $self->{channel_map}{$channel};
	$self->{channel_map}{$channel} = $f;

    my $frame = Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Channel::Open->new,
    );
    $frame->channel($channel);
    my $c = Net::Async::AMQP::Channel->new(
        amqp   => $self,
        future => $f,
        id     => $channel,
    );
    $self->{channel_by_id}{$channel} = $c;
    warn "Record channel $channel as $c\n" if DEBUG;
    $self->push_pending(
        'Channel::OpenOk' => sub {
            my ($self, $frame) = @_;
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
    my $f = $self->loop->new_future;
    my $frame = Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Connection::Close->new(
			reply_code => $args{code} // 320,
			reply_text => $args{text} // 'Request connection close',
		),
    );
    $self->push_pending(
        'Connection::CloseOk' => sub {
            my ($self, $frame) = @_;
            {
                my $method_frame = $frame->method_frame;
                $f->done($self);
            }
            weaken $f;
        }
    );
    $self->send_frame($frame);
    return $f;
}

sub channel_closed {
    my $self = shift;
    delete $self->{channel_by_id}{+shift};
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
    my $self = shift;
    my $type = shift;
    my $frame = shift;
    warn "Check next pending for $type\n" if DEBUG;
    if(my $next = shift @{$self->{pending}{$type} || []}) {
		# We have a registered handler for this frame type. This usually
		# means that we've sent a message and are awaiting a response.
		$next->($self, $frame, @_);
	} else {
		# It's quite possible we'll see unsolicited frames back from
		# the server: these will typically be errors, connection close,
		# or consumer cancellation if the consumer_cancel_notify
		# option is set (RabbitMQ). We don't expect many so report
		# them when in debug mode.
		warn "We had no pending handlers for $type, raising as event" if DEBUG;
		$self->invoke_event(
			unexpected_frame => $type, $frame
		);
	}
    $self
}

=head1 METHODS - Accessors

=head2 loop

L<IO::Async::Loop> container.

=cut

sub loop { shift->{loop} }

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

Maximum number of bytes allowed in any given frame.

=cut

sub frame_max {
    my $self = shift;
    return $self->{frame_max} unless @_;

    $self->{frame_max} = shift;
    $self
}

=head2 channel_max

Maximum number of channels.

=cut

sub channel_max {
    my $self = shift;
    return $self->{channel_max} unless @_;

    $self->{channel_max} = shift;
    $self
}

=head2 last_frame_time

Timestamp of the last frame we received from
the remote. Used for handling heartbeats.

=cut

sub last_frame_time {
    my $self = shift;
    return $self->{last_frame_time} unless @_;

    $self->{last_frame_time} = shift;
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
    my $self = shift;
    my $frame = shift->method_frame;
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
    my $self = shift;
    my $frame = shift;

	# Basic::Deliver - we're delivering a message to a ctag
	# Frame::Header - header part of message
	# Frame::Body* - body content
    warn "Processing frame $self => $frame\n" if DEBUG;
    # First part of a frame. There's more to come, so stash a new future
    # and return.
    if($frame->isa('Net::AMQP::Frame::Header')) {
        if($frame->header_frame->headers) {
            eval {
				$self->{incoming_message}{$frame->channel}{type} = $frame->header_frame->headers->{type};
				1
			} or warn $@;
        }
        unless($frame->body_size) {
            $self->{incoming_message}{$frame->channel}{payload} = '';
            $self->{channel_map}{$frame->channel}->invoke_event(
                message => @{$self->{incoming_message}{$frame->channel}}{qw(type payload ctag dtag rkey)},
            );
            delete $self->{incoming_message}{$frame->channel};
        }
        return $self;
    }

    # Body part of an incoming message.
    # TODO should handle multiple chunks?
    if($frame->isa('Net::AMQP::Frame::Body')) {
        $self->{incoming_message}{$frame->channel}{payload} = $frame->payload;
        $self->{channel_map}{$frame->channel}->invoke_event(
            message => @{$self->{incoming_message}{$frame->channel}}{qw(type payload ctag dtag rkey)},
        );
        delete $self->{incoming_message}{$frame->channel};
        return $self;
    }
    return $self unless $frame->can('method_frame');

    my $frame_type = $self->get_frame_type($frame);
    if($frame_type eq 'Basic::Deliver') {
        warn "Already have incoming_message?" if DEBUG && exists $self->{incoming_message}{$frame->channel};
        $self->{incoming_message}{$frame->channel} = {
            ctag => $frame->method_frame->consumer_tag,
            dtag => $frame->method_frame->delivery_tag,
            rkey => $frame->method_frame->routing_key,
        };
        return $self;
    }

    # Any channel errors will be represented as a channel close event
    if($frame_type eq 'Channel::Close') {
        warn "Channel was " . $frame->channel . ", calling close\n" if DEBUG;
        $self->channel_by_id($frame->channel)->on_close(
            $frame->method_frame
        );
        return $self;
    }

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
    warn "Sending frame " . Dumper $frame if DEBUG;

    # Get bytes to send across our transport
    my $data = $frame->to_raw_frame;

#    warn "Sending data: " . Dumper($frame) . "\n";
    $self->write($data);
	$self->reset_heartbeat if $self->heartbeat_timer;
    $self;
}

=head2 reset_heartbeat

Resets our side of the heartbeat timer.

This is used to ensure we send data at least once every L</heartbeat_interval>
seconds.

=cut

sub reset_heartbeat {
    my $self = shift;
    return unless my $timer = $self->heartbeat_timer;

	if(($self->loop->time - $self->last_frame_time) > 3 * $self->heartbeat_interval) {
		return $self->handle_heartbeat_failure;
	}

    $timer->reset;
}

=head1 future

Returns a new L<IO::Async::Future> instance.

Supports optional 

=cut

sub future {
	my $self = shift;
	my $f = $self->loop->new_future;
	while(my ($k, $v) = splice @_, 0, 2) {
		$f->can($k) ? $f->$k($v) : $self->debug_printf("Unable to call method $k on $f");
	}
	$f
}

sub notifier_name { undef }
sub parent { undef }

# IO::Async::Notifier
sub debug_printf {
	my $self = shift;
	return unless DEBUG;
	IO::Async::Notifier::debug_printf($self, @_);
}

1;

__END__

=head1 EVENTS

The following events may be raised by this class - use
L<Mixin::Event::Dispatch/subscribe_to_event> to watch for them:

 $mq->subscribe_to_event(
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

 $mq->subscribe_to_event(
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

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Licensed under the same terms as Perl itself, with additional licensing
terms for the MQ spec to be found in C<share/amqp0-9-1.extended.xml>
('a worldwide, perpetual, royalty-free, nontransferable, nonexclusive
license to (i) copy, display, distribute and implement the Advanced
Messaging Queue Protocol ("AMQP") Specification').

