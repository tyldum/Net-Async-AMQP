package Net::Async::AMQP::Queue;

use strict;
use warnings;

use parent qw(IO::Async::Notifier);

=head1 NAME

Net::Async::AMQP::Queue - deal with queue-specific functionality

=head1 SYNOPSIS


=head1 DESCRIPTION


=cut

use Future;
use curry::weak;
use Class::ISA ();
use Scalar::Util qw(weaken);

use Net::Async::AMQP;

=head1 METHODS

=cut

=head2 listen

Starts a consumer on this queue.

 $q->listen(
  channel => $ch,
  ack => 1
 )->then(sub {
  my ($q, $ctag) = @_;
  print "Queue $q has ctag $ctag\n";
  ...
 })

Expects the following named parameters:

=over 4

=item * channel - which channel to listen on

=item * ack (optional) - true to enable ACKs

=item * consumer_tag (optional) - specific consumer tag

=back

Returns a L<Future> which resolves with ($queue, $consumer_tag) on
completion.

=cut

sub listen {
    my $self = shift;
    my %args = @_;

	my $ch = delete $args{channel} or die "No channel provided";
	$self->{channel} = $ch;

    # Attempt to bind after we've successfully declared the exchange.
    my $f = $self->future->then(sub {
        my $f = $self->loop->new_future;
        $self->debug_printf("Attempting to listen for events on queue [%s]", $self->queue_name);

        my $frame = Net::AMQP::Protocol::Basic::Consume->new(
            queue        => Net::AMQP::Value::String->new($self->queue_name),
            consumer_tag => (exists $args{consumer_tag} ? Net::AMQP::Value::String->new($args{consumer_tag}) : ''),
            no_local     => 0,
            no_ack       => ($args{ack} ? 0 : 1),
            exclusive    => 0,
            ticket       => 0,
            nowait       => 0,
        );
        $self->push_pending(
            'Basic::ConsumeOk' => (sub {
                my ($amqp, $frame) = @_;
				my $ctag = $frame->method_frame->consumer_tag;
				$ch->bus->invoke_event(
					listener_start => $ctag
				);
                $f->done($self => $ctag) unless $f->is_ready;

				# If we were cancelled before we received the OK response,
				# that's mildly awkward - we need to cancel the consumer,
				# note that messages may be delivered in the interim.
				if($f->is_cancelled) {
					$self->adopt_future(
						$self->cancel(
							consumer_tag => $ctag
						)->on_fail(sub {
							# We should report this, but where to?
							$self->debug_printf("Failed to cancel listener %s", $ctag);
						})->else_done->set_label(
							"Cancel $ctag"
						)
					)
				}
            })
        );
		$ch->closure_protection($f);
        $ch->send_frame($frame);
        $f;
    });
	$self->adopt_future($f->else_done);
	$f
}

=head2 cancel

Cancels the given consumer.

 $q->cancel(
  consumer_tag => '...',
 )->then(sub {
  my ($q, $ctag) = @_;
  print "Queue $q ctag $ctag cancelled\n";
  ...
 })

Expects the following named parameters:

=over 4

=item * consumer_tag (optional) - specific consumer tag

=back

Returns a L<Future> which resolves with ($queue, $consumer_tag) on
completion.

=cut

sub cancel {
    my $self = shift;
    my %args = @_;
	my $ch = delete $self->{channel} or die "No channel";
	my $ctag = delete $args{consumer_tag} or die "No ctag";

    # Attempt to bind after we've successfully declared the exchange.
	my $f = $self->future->then(sub {
		my $f = $self->loop->new_future;
		$self->debug_printf("Attempting to cancel consumer [%s]", $args{consumer_tag});

		my $frame = Net::AMQP::Protocol::Basic::Cancel->new(
			consumer_tag => Net::AMQP::Value::String->new($args{consumer_tag}),
			nowait       => 0,
		);
		$self->push_pending(
			'Basic::CancelOk' => (sub {
				my ($amqp, $frame) = @_;
				my $ctag = $frame->method_frame->consumer_tag;
				$ch->bus->invoke_event(
					listener_stop => $ctag
				);
				$f->done($self => $ctag) unless $f->is_cancelled;
			})
		);
		$ch->closure_protection($f);
		$ch->send_frame($frame);
		$f;
	});
    $self->adopt_future($f->else_done);
	$f
}

=head2 bind_exchange

Binds this queue to an exchange.

 $q->bind_exchange(
  channel => $ch,
  exchange => '',
 )->then(sub {
  my ($q) = @_;
  print "Queue $q bound to default exchange\n";
  ...
 })

Expects the following named parameters:

=over 4

=item * channel - which channel to perform the bind on

=item * exchange - the exchange to bind, can be '' for default

=item * routing_key (optional) - a routing key for the binding

=back

Returns a L<Future> which resolves with ($queue) on
completion.

=cut

sub bind_exchange {
    my $self = shift;
    my %args = @_;
    die "No exchange specified" unless exists $args{exchange};
	my $ch = delete $args{channel} or die "No channel provided";

    # Attempt to bind after we've successfully declared the exchange.
	my $f = $self->future->then(sub {
		my $f = $self->loop->new_future;
		$self->debug_printf("Binding queue [%s] to exchange [%s] with rkey [%s]", $self->queue_name, $args{exchange}, $args{routing_key} // '(none)');

		my $frame = Net::AMQP::Frame::Method->new(
			method_frame => Net::AMQP::Protocol::Queue::Bind->new(
				queue       => Net::AMQP::Value::String->new($self->queue_name),
				exchange    => Net::AMQP::Value::String->new($args{exchange}),
				(exists($args{routing_key}) ? ('routing_key' => Net::AMQP::Value::String->new($args{routing_key})) : ()),
				ticket      => 0,
				nowait      => 0,
			)
		);
		$self->push_pending(
			'Queue::BindOk' => [ $f, $self ],
		);
		$ch->closure_protection($f);
		$ch->send_frame($frame);
		$f
	});
	$self->adopt_future($f->else_done);
	$f
}

=head2 delete

Deletes this queue.

 $q->delete(
  channel => $ch,
 )->then(sub {
  my ($q) = @_;
  print "Queue $q deleted\n";
  ...
 })

Expects the following named parameters:

=over 4

=item * channel - which channel to perform the bind on

=back

Returns a L<Future> which resolves with ($queue) on
completion.

=cut

sub delete : method {
    my $self = shift;
    my %args = @_;
	my $ch = delete $args{channel} or die "No channel provided";

	my $f = $self->future->then(sub {
		my $f = $self->loop->new_future;
		$self->debug_printf("Deleting queue [%s]", $self->queue_name);

		my $frame = Net::AMQP::Frame::Method->new(
			method_frame => Net::AMQP::Protocol::Queue::Delete->new(
				queue       => Net::AMQP::Value::String->new($self->queue_name),
				nowait      => 0,
			)
		);
		$self->push_pending(
			'Queue::DeleteOk' => [ $f, $self ],
		);
		$ch->closure_protection($f);
		$ch->send_frame($frame);
		$f
	});
	$self->adopt_future($f->else_done);
	$f
}

=head1 ACCESSORS

These are mostly intended for internal use only.

=cut

=head2 configure

Applies C<amqp> or C<future> value.

=cut

sub configure {
	my ($self, %args) = @_;
	for(grep exists $args{$_}, qw(amqp)) {
		Scalar::Util::weaken($self->{$_} = delete $args{$_})
	}
	for(grep exists $args{$_}, qw(future channel)) {
		$self->{$_} = delete $args{$_};
	}
    $self->SUPER::configure(%args);
}

=head2 amqp

A weakref to the L<Net::Async::AMQP> instance.

=cut

sub amqp { shift->{amqp} }

=head2 future

A weakref to the L<Future> representing the queue readiness.

=cut

sub future { shift->{future} }

=head2 queue_name

Sets or returns the queue name.

=cut

sub queue_name {
    my $self = shift;
    return $self->{queue_name} unless @_;
    $self->{queue_name} = shift;
    $self
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

