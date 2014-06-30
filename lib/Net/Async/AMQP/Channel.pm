package Net::Async::AMQP::Channel;

use strict;
use warnings;
use parent qw(Mixin::Event::Dispatch);

=head1 NAME

Net::Async::AMQP::Channel - represents a single channel in an MQ connection

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::AMQP;
 my $amqp = Net::Async::AMQP->new(loop => my $loop = IO::Async::Loop->new);
 $amqp->connect(
   host => 'localhost',
   username => 'guest',
   password => 'guest',
   on_connected => sub {
   }
 );

=head1 DESCRIPTION

=cut

use Future;
use curry::weak;
use Closure::Explicit qw(callback);
use Class::ISA ();
use Data::Dumper;
use Scalar::Util qw(weaken);

use Net::Async::AMQP;
use constant DEBUG => Net::Async::AMQP->DEBUG;

=head1 METHODS

=cut

sub new {
    my $class = shift;
    my $self = bless { @_ }, $class;
    Scalar::Util::weaken($_) for @{$self}{qw/amqp/};
    $self
}

=head2 confirm_mode

Switches confirmation mode on for this channel.
In confirm mode, all messages must be ACKed
explicitly after delivery.

Returns a L<Future> which will resolve with this
channel once complete.

 $ch->confirm_mode ==> $ch

=cut

sub confirm_mode {
    my $self = shift;
    my %args = @_;
    warn "Attempting to switch to confirm mode\n" if DEBUG;

    my $f = $self->loop->new_future;
    my $frame = Net::AMQP::Frame::Method->new(
        channel => $self->id,
        method_frame => Net::AMQP::Protocol::Confirm::Select->new(
            nowait      => 0,
        )
    );
    $self->amqp->push_pending(
        'Confirm::SelectOk' => callback {
            my $self = shift;
            $f->done($self) unless $f->is_ready;
            undef $f;
        } weaken => [qw($self)],
          allowed => [qw($f)]
    );
    $self->amqp->send_frame($frame);
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

    warn "Attempting to declare our exchange: " . $args{exchange} if DEBUG;

    my $f = $self->loop->new_future;
    my $frame = Net::AMQP::Frame::Method->new(
        channel => $self->id,
        method_frame => Net::AMQP::Protocol::Exchange::Declare->new(
            exchange    => $args{exchange},
            type        => $args{type},
            passive     => $args{passive} || 0,
            durable     => $args{durable} || 0,
            auto_delete => $args{auto_delete} || 0,
            internal    => $args{internal} || 0,
            ticket      => 0,
            nowait      => 0,
        )
    );
    $self->amqp->push_pending(
        'Exchange::DeclareOk' => callback {
            my ($amqp, $frame) = @_;
            my $method_frame = $frame->method_frame;
            $f->done($self) unless $f->is_ready;
            undef $f;
        } allowed => [qw($f $self)]
    );
    $self->amqp->send_frame($frame);
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

    warn "queue dec start\n" if DEBUG;
    $self->future->then(callback {
        my $self = shift;
        warn "queue decl\n" if DEBUG;
        my $f = $self->loop->new_future;
        my $q = Net::Async::AMQP::Queue->new(
            amqp => $self->amqp,
            future => $f,
        );
        warn "Attempting to declare our queue" if DEBUG;
        $q->channel($self);
        my $frame = Net::AMQP::Frame::Method->new(
            channel => $self->id,
            method_frame => Net::AMQP::Protocol::Queue::Declare->new(
                queue       => $args{queue},
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
        $self->amqp->push_pending(
            'Queue::DeclareOk' => callback {
                my ($self, $frame) = @_;
                my $method_frame = $frame->method_frame;
                $q->queue_name($method_frame->queue);
                $f->done($q) unless $f->is_ready;
				undef $q;
				undef $f;
            } allowed => [qw($f $q)]
        );
        $self->send_frame($frame);
        $f;
    } weaken => [qw($self)],
      allowed => [qw(%args)]);
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

    $self->future->then(callback {
        my $self = shift;
        my $f = $self->loop->new_future;
        my $channel = $self->id;
		{ # When publishing a message, we should expect either an ACK, or a return.
		  # Since these are mutually exclusive, we also need to remove the pending
		  # handler for the opposing event once one event has been received. Note
		  # that this crosslinking gives us an unfortunate cycle which we resolve
		  # by weakening the opposite handler once we've removed it.
			my $return;
			my $ack = callback {
				my ($amqp, $frame) = @_;
				my $method_frame = $frame->method_frame;
				$amqp->remove_pending('Basic::Return' => $return);
				$f->done unless $f->is_ready;
				weaken $f;
				weaken $return;
			} allowed => [qw($f $return)];
			$return = callback {
				my ($amqp, $frame) = @_;
				my $method_frame = $frame->method_frame;
				$amqp->remove_pending('Basic::Ack' => $ack);
				$f->fail(
                    $method_frame->reply_text,
                    code     => $method_frame->reply_code,
                    exchange => $method_frame->exchange,
                    rkey     => $method_frame->routing_key
                ) unless $f->is_cancelled;
				weaken $f;
				weaken $ack;
			} allowed => [qw($f $ack)];
			$self->amqp->push_pending(
				'Basic::Return' => $return,
			);
			$self->amqp->push_pending(
				'Basic::Ack' => $ack,
			);
		}

        my @frames = $self->amqp->split_payload(
            $args{payload},
            exchange         => $args{exchange},
            mandatory		 => $args{mandatory} // 0,
            immediate        => 0,
            (exists $args{routing_key} ? (routing_key => $args{routing_key}) : ()),
            ticket           => 0,
            content_type     => 'application/binary',
            content_encoding => undef,
            timestamp        => time,
            type             => $args{type},
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
            channel => $channel,
        ) for @frames;
        $f
    } weaken => [qw($self)],
      allowed => [qw(%args)]);
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

    $self->future->then(callback {
        my $self = shift;
        my $f = $self->loop->new_future;
        my $channel = $self->id;
        $self->amqp->push_pending(
            'Basic::QosOk' => callback {
                my ($self, $frame) = @_;
                my $method_frame = $frame->method_frame;
				warn "QOS Ack" if DEBUG;
                $f->done unless $f->is_ready;
                undef $f;
            } allowed => [qw($f)]
        );

        my $frame = Net::AMQP::Frame::Method->new(
            channel => $self->id,
            method_frame => Net::AMQP::Protocol::Basic::Qos->new(
                nowait         => 0,
                prefetch_count => $args{prefetch_count},
                prefetch_size  => $args{prefetch_size} || 0,
            )
        );
        $self->send_frame($frame);
        $f
    } weaken => [qw($self)],
      allowed => [qw(%args)]);
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
    $self->future->on_done(callback {
        my $self = shift;
        my $channel = $id;
        my $frame = Net::AMQP::Frame::Method->new(
            channel => $id,
            method_frame => Net::AMQP::Protocol::Basic::Ack->new(
               # nowait      => 0,
				delivery_tag => $args{delivery_tag},
				multiple	=> $args{multiple} // 0,
            )
        );
        $self->send_frame($frame);
    } weaken => [qw($self)],
      allowed => [qw(%args $id)]);
}

=pod

Example output:

        'method_id' => 40,
        'reply_code' => 404,
        'class_id' => 60,
        'reply_text' => 'NOT_FOUND - no exchange \'invalidchan\' in vhost \'mv\''

=cut

=head2 on_close

Called when the channel has been closed.

=cut

sub on_close {
    my $self = shift;
    my $frame = shift;
    $self->invoke_event(
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

sub send_frame { shift->amqp->send_frame(@_) }

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
    warn "Closing channel " . $self->id . "\n" if DEBUG;

    my $f = $self->loop->new_future;
    my $frame = Net::AMQP::Frame::Method->new(
        channel => $self->id,
        method_frame => Net::AMQP::Protocol::Channel::Close->new(
			reply_code  => $args{code} // 404,
			reply_text  => $args{text} // 'closing',
        )
    );
    $self->amqp->push_pending(
        'Channel::CloseOk' => callback {
            my $self = shift;
            $f->done($self) unless $f->is_ready;
            undef $f;
        } weaken => [qw($self)],
          allowed => [qw($f)]
    );
    $self->amqp->send_frame($frame);
    return $f;
}

=head1 METHODS - Accessors

=cut

=head2 amqp

The parent L<Net::Async::AMQP> instance.

=cut

sub amqp { shift->{amqp} }

=head2 loop

Our containing L<IO::Async::Loop> instance.

=cut

sub loop { shift->amqp->loop }

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

{
package Net::Async::AMQP::Channel::Helper;

sub new { bless { channel => $_[1] }, $_[0] }
sub channel { shift->{channel} }
sub DESTROY {
	my $self = shift;
	$self->channel->release if $self->channel;
}

our $AUTOLOAD;
sub AUTOLOAD {
	my $self = shift;
	(my $method = $AUTOLOAD) =~ s/^.*:://;
	warn "Adding new autoload method $method\n";
	my $code = $self->channel->${\"curry::weak::$method"}->();
	*{"__PACKAGE__::$method"} = $code;
	$code->(@_);
}

sub diagnostics {
	my ($d, $level) = @_;
}
}
1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Licensed under the same terms as Perl itself.

