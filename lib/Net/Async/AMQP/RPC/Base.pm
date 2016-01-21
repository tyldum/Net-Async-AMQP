package Net::Async::AMQP::RPC::Base;

use strict;
use warnings;

use parent qw(IO::Async::Notifier);

use Net::Async::AMQP;

use Variable::Disposition qw(retain_future);
use Log::Any qw($log);
use Scalar::Util ();

sub mq {
	my $self = shift;
	return $self->{mq} if $self->{mq};
	$log->debugf("Establishing connection to MQ server at %s:%s", $self->{host}, $self->{port});
	$self->add_child(
		$self->{mq} = my $mq = Net::Async::AMQP->new
	);
	$mq->connect(
		host  => $self->{host},
		user  => $self->{user},
		pass  => $self->{pass},
		port  => $self->{port},
		vhost => $self->{vhost},
		client_properties => {
			capabilities => {
				'consumer_cancel_notify' => Net::AMQP::Value->true,
			},
		},
	);
	$mq
}

sub queue_name {
	$_[0]->{queue_name} ||= $_[0]->future(set_label => 'queue name')->on_done(sub {
		$log->infof("Queue name is %s", shift);
	})
}

sub routing_key { '' }

sub exchange { shift->{exchange} }

sub future { my $self = shift; $self->mq->future(@_) }

sub _add_to_loop {
	my ($self, $loop) = @_;
	die "Need an MQ connection" unless $self->mq;
	die "Need an exchange name" unless defined $self->{exchange};
	retain_future(
		$self->connected->then(sub {
			$log->debug("Connected to MQ server, activating consumer");
			$self->consumer
		})->then(sub {
			$log->info("Ready for requests");
			$self->active->done
		})->on_fail(sub {
			$log->errorf("Failure: %s", shift);
		})
	)
}

sub connected { shift->mq->connected }

sub client_queue {
	my $self = shift;
	$self->{client_queue} ||= $self->consumer_channel->then(sub {
		my ($ch) = @_;
		$log->debug("Declaring queue");
		$ch->queue_declare(
			queue => $self->{queue} // '',
		)->then(sub {
			my ($q) = @_;
			$self->queue_name->done($q->queue_name);
			Future->done($q)
		})
	})->on_fail(sub {
		$log->errorf("Failed to set up client queue: %s", shift)
	})
}

sub server_queue {
	my $self = shift;
	$self->{server_queue} ||= $self->consumer_channel->then(sub {
		my ($ch) = @_;
		$log->debug("Declaring queue");
		Future->needs_all(
			$ch->queue_declare(
				queue => $self->{queue} // '',
			),
			$ch->exchange_declare(
				exchange    => $self->{exchange},
				type        => 'topic',
			)
		)->then(sub {
			my ($q) = @_;
			$self->queue_name->done($q->queue_name);
			$log->debugf("Binding queue %s to exchange %s", $q->queue_name, $self->{exchange});
			$q->bind_exchange(
				channel     => $ch,
				exchange    => $self->{exchange},
				routing_key => $self->{routing_key} // '',
			)->transform(
				done => sub { $q }
			)
		})
	})->on_fail(sub {
		$log->errorf("Failed to set up server queue: %s", shift)
	})
}

sub reply {
	my ($self, %args) = @_;
	$self->publisher_channel->then(sub {
		my ($ch) = @_;
		$ch->publish(
			exchange       => '',
			routing_key    => $args{reply_to},
			delivery_mode  => 2, # persistent
			correlation_id => $args{correlation_id},
			type           => $args{type},
			payload        => $args{payload},
		)
	});
}

sub consumer {
	my $self = shift;
	$self->{consumer} ||= Future->needs_all(
		$self->queue,
		$self->consumer_channel
	)->then(sub {
		my ($q, $ch) = @_;
		$log->debug("Starting consumer");
		$q->consumer(
			channel => $ch,
			ack => 1,
			on_message => $self->curry::weak::on_message($ch),
		)
	})->on_fail(sub {
		$log->errorf("Failed to set up consumer: %s", shift)
	})
}

sub on_message {
	my ($self, $ch, %args) = @_;
	$log->debugf("Received message of type %s, correlation ID %s, reply_to %s", $args{type}, $args{properties}{correlation_id}, $args{properties}{reply_to});
	my $dtag = $args{delivery_tag};
	(eval {
		my $f = $self->process_message(
			type => $args{type},
			id => $args{properties}{correlation_id},
			reply_to => $args{properties}{reply_to},
			payload => $args{payload},
		);
		$f = Future->done($f) unless Scalar::Util::blessed($f) && $f->isa('Future');
		$f
	} or do {
		my $err = $@;
		$log->errorf("Error processing: %s", $err);
		Future->fail($err);
	})->on_ready(sub {
		$self->{pending}{$dtag} = $ch->ack(
			delivery_tag => $dtag
		)->on_ready(sub {
			delete $self->{pending}{$dtag}
		});
	})
}

sub process_message { die 'abstract method ->process_message called - please subclass and override' }

sub consumer_channel {
	my $self = shift;
	$self->{consumer_channel} ||= $self->connected->then(sub {
		$self->mq->open_channel
	})->on_done(sub {
		$log->debugf("Receiver channel ID %d", shift->id);
	})
}

sub publisher_channel {
	my $self = shift;
	$self->{consumer_channel} ||= $self->connected->then(sub {
		$self->mq->open_channel
	})->on_done(sub {
		$log->debugf("Sender channel ID %d", shift->id);
	})
}

sub active {
	my $self = shift;
	$self->{active} ||= $self->mq->future
}

sub configure {
	my ($self, %args) = @_;
	for (qw(mq user pass host port vhost mq queue exchange)) {
		$self->{$_} = delete $args{$_} if exists $args{$_}
	}
	$self->SUPER::configure(%args)
}

1;

