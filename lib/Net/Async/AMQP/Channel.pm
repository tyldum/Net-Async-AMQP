package Net::Async::AMQP::Channel;

use strict;
use warnings;

use parent qw(IO::Async::Notifier);

=head1 NAME

Net::Async::AMQP::Channel - represents a single channel in an MQ connection

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::AMQP;
 my $loop = IO::Async::Loop->new;
 $loop->add(my $amqp = Net::Async::AMQP->new);
 $amqp->connect(
   host => 'localhost',
   username => 'guest',
   password => 'guest',
 )->then(sub {
  shift->open_channel->publish(
   type => 'application/json'
  )
 });

=head1 DESCRIPTION

Each Net::Async::AMQP::Channel instance represents a virtual channel for
communicating with the MQ server.

Channels are layered over the TCP protocol and most of the common AMQP frames
operate at channel level - typically you'd connect to the server, open one
channel for one-shot requests such as binding/declaring/publishing, and a further
channel for every consumer.

Since any error typically results in a closed channel, it's not recommended to
have multiple consumers on the same channel if there's any chance the Basic.Consume
request will fail.

=cut

use Future;
use curry::weak;
use Class::ISA ();
use Data::Dumper;
use Scalar::Util qw(weaken);

use Net::Async::AMQP;
use constant DEBUG => Net::Async::AMQP->DEBUG;

use overload
    '""' => sub { shift->as_string },
    '0+' => sub { 0 + shift->id },
    bool => sub { 1 },
    fallback => 1;

=head1 METHODS

=cut

sub configure {
	my ($self, %args) = @_;
	for(grep exists $args{$_}, qw(amqp)) {
		Scalar::Util::weaken($self->{$_} = delete $args{$_})
	}
	for(grep exists $args{$_}, qw(future id)) {
		$self->{$_} = delete $args{$_};
	}
    $self->SUPER::configure(%args);
}

=head2 confirm_mode

Switches confirmation mode on for this channel.
In confirm mode, all messages must be ACKed
explicitly after delivery.

Note that this is an irreversible operation - once
confirm mode has been enabled on a channel, closing that
channel and reopening is the only way to turn off confirm
mode again.

Returns a L<Future> which will resolve with this
channel once complete.

 $ch->confirm_mode ==> $ch

=cut

sub confirm_mode {
    my $self = shift;
    my %args = @_;
    $self->debug_printf("Enabling confirm mode");

    my $f = $self->loop->new_future;
    my $frame = Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Confirm::Select->new(
            nowait      => 0,
        )
    );
    $self->push_pending(
        'Confirm::SelectOk' => [ $f, $self ]
    );
    $self->send_frame($frame);
    return $f;
}

=head2 exchange_declare

Declares a new exchange.

Returns a L<Future> which will resolve with this
channel once complete.

 $ch->exchange_declare(
  exchange   => 'some_exchange',
  type       => 'fanout',
  autodelete => 1,
 ) ==> $ch

=cut

sub exchange_declare {
    my $self = shift;
    my %args = @_;
    die "No exchange specified" unless exists $args{exchange};
    die "No exchange type specified" unless exists $args{type};

    $self->debug_printf("Declaring exchange [%s]", $args{exchange});

    my $f = $self->loop->new_future;
    my $frame = Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Exchange::Declare->new(
            exchange    => Net::AMQP::Value::String->new($args{exchange}),
            type        => Net::AMQP::Value::String->new($args{type}),
            passive     => $args{passive} || 0,
            durable     => $args{durable} || 0,
            auto_delete => $args{auto_delete} || 0,
            internal    => $args{internal} || 0,
            ticket      => 0,
            nowait      => 0,
        )
    );
    $self->push_pending(
        'Exchange::DeclareOk' => [ $f, $self ]
    );
    $self->send_frame($frame);
    return $f;
}

=head2 queue_declare

Returns a L<Future> which will resolve with the
new L<Net::Async::AMQP::Queue> instance once complete.

 $ch->queue_declare(
  queue      => 'some_queue',
 ) ==> $q

=cut

sub queue_declare {
    my $self = shift;
    my %args = @_;
    die "No queue specified" unless defined $args{queue};

    $self->future->then(sub {
        my $f = $self->loop->new_future;
        $self->add_child(
			my $q = Net::Async::AMQP::Queue->new(
				amqp    => $self->amqp,
				future  => $f,
				channel => $self,
			)
		);
		# Avoid the cycle caused by self -> children [ queue ],
		# but do it outside the queue object so that we
		# can assign a different channel elsewhere without
		# triggering cleanup logic as soon as that channel
		# goes out of scope.
		Scalar::Util::weaken($q->{channel});
        $self->debug_printf("Declaring queue [%s]", $args{queue});
        my $frame = Net::AMQP::Frame::Method->new(
            method_frame => Net::AMQP::Protocol::Queue::Declare->new(
                queue       => Net::AMQP::Value::String->new($args{queue}),
                passive     => $args{passive} || 0,
                durable     => $args{durable} || 0,
                exclusive   => $args{exclusive} || 0,
                auto_delete => $args{auto_delete} || 0,
                no_ack      => $args{no_ack} || 0,
				($args{arguments}
				? (arguments   => $args{arguments})
				: ()
				),
                ticket      => 0,
                nowait      => 0,
            )
        );
        $self->push_pending(
            'Queue::DeclareOk' => sub {
                my ($amqp, $frame) = @_;
                my $method_frame = $frame->method_frame;
                $q->queue_name($method_frame->queue);
                $f->done($q) unless $f->is_ready;
            }
        );
        $self->send_frame($frame);
        $f;
    })
}

=head2 publish

Publishes a message on this channel.

Returns a L<Future> which will resolve with the
channel instance once the server has confirmed publishing is complete.

 $ch->publish(
  exchange => 'some_exchange',
  routing_key => 'some.rkey.here',
  type => 'some_type',
 ) ==> $ch

=cut

sub publish {
    my $self = shift;
    my %args = @_;
    die "no exchange" unless exists $args{exchange};

    $self->future->then(sub {
        my $f = $self->loop->new_future;
		{ # When publishing a message, we should expect either an ACK, or a return.
		  # Since these are mutually exclusive, we also need to remove the pending
		  # handler for the opposing event once one event has been received. Note
		  # that this crosslinking gives us an unfortunate cycle which we resolve
		  # by weakening the opposite handler once we've removed it.
			my $return;
			my $ack = sub {
				my ($amqp, $frame) = @_;
				my $method_frame = $frame->method_frame;
				$self->remove_pending('Basic::Return' => $return);
				$f->done unless $f->is_ready;
				weaken $return;
			};
			$return = sub {
				my ($amqp, $frame) = @_;
				my $method_frame = $frame->method_frame;
				# $self->remove_pending('Basic::Ack' => $ack);
				$f->fail(
                    $method_frame->reply_text,
                    code     => $method_frame->reply_code,
                    exchange => $method_frame->exchange,
                    rkey     => $method_frame->routing_key
                ) unless $f->is_ready;
				weaken $ack;
			};
			$self->push_pending(
				'Basic::Return' => $return,
			);
			$self->push_pending(
				'Basic::Ack' => $ack,
			);
		}

        my @frames = $self->amqp->split_payload(
            $args{payload},
            exchange         => Net::AMQP::Value::String->new($args{exchange}),
            mandatory		 => $args{mandatory} // 0,
            immediate        => 0,
            (exists $args{routing_key} ? (routing_key => Net::AMQP::Value::String->new($args{routing_key})) : ()),
            ticket           => 0,
            content_type     => 'application/binary',
            content_encoding => undef,
            timestamp        => time,
            type             => Net::AMQP::Value::String->new($args{type}),
            user_id          => $self->amqp->user,
            no_ack           => 0,
#            headers          => {
#                type => $args{type},
#            },
            delivery_mode    => 1,
            priority         => 1,
            correlation_id   => undef,
            expiration       => undef,
            message_id       => undef,
            app_id           => undef,
            cluster_id       => undef,
            weight           => 0,
        );
        $self->send_frame(
            $_,
        ) for @frames;
        $f
    })
}

=head2 qos

Changes QOS settings on the channel. Probably most
useful for limiting the number of messages that can
be delivered to us before we have to ACK/NAK to
proceed.

Returns a L<Future> which will resolve with the
channel instance once the operation is complete.

 $ch->qos(
  prefetch_count => 5,
  prefetch_size  => 1048576,
 ) ==> $ch

=cut

sub qos {
    my $self = shift;
    my %args = @_;

    $self->future->then(sub {
        my $f = $self->loop->new_future;
        my $channel = $self->id;
        $self->push_pending(
            'Basic::QosOk' => [ $f, $self ],
        );

        my $frame = Net::AMQP::Frame::Method->new(
            method_frame => Net::AMQP::Protocol::Basic::Qos->new(
                nowait         => 0,
                prefetch_count => $args{prefetch_count},
                prefetch_size  => $args{prefetch_size} || 0,
            )
        );
        $self->send_frame($frame);
        $f
    });
}

=head2 ack

Acknowledge a specific delivery.

Returns a L<Future> which will resolve with the
channel instance once the operation is complete.

 $ch->ack(
  delivery_tag => 123,
 ) ==> $ch

=cut

sub ack {
    my $self = shift;
    my %args = @_;

    my $id = $self->id;
    $self->future->on_done(sub {
        my $channel = $id;
        my $frame = Net::AMQP::Frame::Method->new(
            method_frame => Net::AMQP::Protocol::Basic::Ack->new(
               # nowait      => 0,
				delivery_tag => $args{delivery_tag},
				multiple     => $args{multiple} // 0,
            )
        );
        $self->send_frame($frame);
    });
}

=pod

Example output:

        'method_id' => 40,
        'reply_code' => 404,
        'class_id' => 60,
        'reply_text' => 'NOT_FOUND - no exchange \'invalidchan\' in vhost \'vhost\''

=cut

=head2 on_close

Called when the channel has been closed.

=cut

sub on_close {
    my $self = shift;
    my $frame = shift;
    $self->bus->invoke_event(
        'close',
        code => $frame->reply_code,
        message => $frame->reply_text,
    );
    $self->amqp->channel_closed($self->id);
}

=head2 send_frame

Proxy frame sending requests to the parent
L<Net::Async::AMQP> instance.

=cut

sub send_frame {
	my $self = shift;
	$self->amqp->send_frame(
		@_,
		channel => $self->id,
	)
}

=head2 close

Ask the server to close this channel.

Returns a L<Future> which will resolve with the
channel instance once the operation is complete.

 $ch->close(
  code => 404,
  text => 'something went wrong',
 ) ==> $ch

=cut

sub close {
    my $self = shift;
    my %args = @_;
    $self->debug_printf("Close channel %d", $self->id);

    my $f = $self->loop->new_future;
    my $frame = Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Channel::Close->new(
			reply_code  => $args{code} // 404,
			reply_text  => $args{text} // 'closing',
        )
    );
    $self->push_pending(
        'Channel::CloseOk' => [ $f, $self ],
    );
    $self->send_frame($frame);
    return $f;
}

=head2 push_pending

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

=head2 next_pending

Retrieves the next pending handler for the given incoming frame type (see L</get_frame_type>),
and calls it.

Takes the following parameters:

=over 4

=item * $frame - the frame itself

=back

Returns $self.

=cut

sub next_pending {
    my ($self, $frame) = @_;

    # First part of a frame. There's more to come, so stash a new future
    # and return.
    if($frame->isa('Net::AMQP::Frame::Header')) {
		$self->{incoming_message}{type} = $frame->header_frame->type;
        if($frame->header_frame->headers) {
            eval {
				$self->{incoming_message}{type} = $frame->header_frame->headers->{type}
					if exists $frame->header_frame->headers->{type};
				1
			} or $self->debug_printf("Unexpected exception while doing something: %s", $@);
        }

		# Messages may be empty - in this case we'd have no body frames at all, we're done already:
        unless($frame->body_size) {
            $self->{incoming_message}{payload} = '';
            $self->bus->invoke_event(
                message => @{$self->{incoming_message}}{qw(type payload ctag dtag rkey)},
            );
            delete $self->{incoming_message};
        }

        return $self;
    }

    # Body part of an incoming message.
    # TODO should handle multiple chunks?
    if($frame->isa('Net::AMQP::Frame::Body')) {
        $self->{incoming_message}{payload} = $frame->payload;
        $self->bus->invoke_event(
            message => @{$self->{incoming_message}}{qw(type payload ctag dtag rkey)},
        );
        delete $self->{incoming_message};
        return $self;
    }

    return $self unless $frame->can('method_frame') && (my $method_frame = $frame->method_frame);
    my $type = $self->amqp->get_frame_type($frame);
	if($type eq 'Basic::CancelOk') {
		my ($ctag) = ($method_frame->consumer_tag);
        $self->debug_printf("Cancel $ctag");
		$self->bus->invoke_event(
			'cancel',
			ctag => $ctag,
		);
	}

    if(my $next = shift @{$self->{pending}{$type} || []}) {
		# We have a registered handler for this frame type. This usually
		# means that we've sent a message and are awaiting a response.
		if(ref($next) eq 'ARRAY') {
			my ($f, @args) = @$next;
			$f->done(@args) unless $f->is_ready;
		} else {
			$next->($self, $frame, @_);
		}
		return $self;
	}

	# Message delivery, part 3: The "Deliver" message.
	# This is actually where we start.
    if($type eq 'Basic::Deliver') {
        $self->debug_printf("Already have incoming_message?") if exists $self->{incoming_message};
        $self->{incoming_message} = {
            ctag => $method_frame->consumer_tag,
            dtag => $method_frame->delivery_tag,
            rkey => $method_frame->routing_key,
        };
        return $self;
    }

    if($type eq 'Channel::Close') {
        $self->debug_printf("Channel was %d, calling close", $frame->channel);
        $self->on_close(
            $method_frame
        );
        return $self;
    }

	# It's quite possible we'll see unsolicited frames back from
	# the server: these will typically be errors, connection close,
	# or consumer cancellation if the consumer_cancel_notify
	# option is set (RabbitMQ). We don't expect many so report
	# them when in debug mode.
	$self->debug_printf("We had no pending handlers for [%s]", $type);
	return undef;
}

=head1 METHODS - Accessors

=cut

=head2 amqp

The parent L<Net::Async::AMQP> instance.

=cut

sub amqp { shift->{amqp} }

=head2 bus

Event bus. Used for sharing channel-specific events.

=cut

sub bus { $_[0]->{bus} ||= Mixin::Event::Dispatch::Bus->new }

=head2 write

Proxy a write operation through the parent L<Net::Async::AMQP> instance.

=cut

sub write { shift->amqp->write(@_) }

=head2 future

The underlying L<Future> for this channel which
will resolve to the instance once the channel
is open.

=cut

sub future { shift->{future} }

=head2 id

This channel ID.

=cut

sub id {
    my $self = shift;
    return $self->{id} unless @_;
    $self->{id} = shift;
    $self
}

sub as_string {
	my $self = shift;
	sprintf "Channel[%d]", $self->id;
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

