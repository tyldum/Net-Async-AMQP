package Net::Async::AMQP::RPC::Server;

use strict;
use warnings;

use parent qw(Net::Async::AMQP::RPC::Base);

=pod

=over 4

=item * Declare a queue

=item * Declare the RPC exchange

=item * Bind our queue to the exchange

=item * Start a consumer on the queue

=item * For each message, process via subclass-defined handlers and send a reply to the default ('') exchange with the reply_to as the routing key

=back

=cut

use Log::Any qw($log);

sub request {
	my ($self) = @_;
	my $id = $self->next_id;
	my $q = $self->{queue};
	$self->{pending}{$id} = my $f = $self->loop->new_future->set_label('RPC response for ' . $id);
	$self->channel->publish(
		reply_to => $q->queue_name,
		delivery_mode => 2, # persistent
		correlation_id => $id,
	)->then(sub { $f });
}

sub whatever {
	my ($self, %args) = @_;
	die "Needs an MQ connection" unless $self->mq;

	my $name = delete $args{queue} // '';
	$self->open_channel->then(sub {
		my ($ch) = shift;
		$ch->queue_declare(
			queue => $name
		)->then(sub {
			my ($q) = @_;
			$log->infof("Queue is %s", $q->queue_name);
			$q->consumer(
				channel => $ch,
				ack => 1,
				on_message => sub {
					my ($ev, %args) = @_;
					my $dtag = $args{delivery_tag};
					eval {
						$self->on_message(%args)
					} or do {
						$log->errorf("Error processing: %s", $@);
					};
					$self->{pending}{$dtag} = $ch->ack(
						delivery_tag => $dtag
					)->on_ready(sub {
						delete $self->{pending}{$dtag}
					});
				}
			)->on_done(sub {
				my ($q, $ctag) = @_;
				$self->{consumer_tag} = $ctag;
				$log->infof("Queue %s has ctag %s", $q->queue_name, $ctag);
			})
		})
	})
}

sub queue { shift->server_queue }

sub process_message {
	my ($self, %args) = @_;
	$log->infof("Have message: %s", join ' ', %args);
	$self->reply(
		reply_to => $args{reply_to},
		correlation_id => $args{id},
		type => $args{type},
		payload => '{ "status": "ok" }',
	);
}

sub configure {
	my ($self, %args) = @_;
	for (qw(json_handler handler)) {
		$self->{$_} = delete $args{$_} if exists $args{$_}
	}
	$self->SUPER::configure(%args)
}

1;

