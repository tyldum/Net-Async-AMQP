use strict;
use warnings;
use Test::More;
use Test::Refcount;
use Test::Fatal;
use Test::Future;

use Future::Utils qw(fmap0);
use IO::Async::Loop::Epoll;
use Net::Async::AMQP::ConnectionManager;

plan skip_all => 'set NET_ASYNC_AMQP_HOST/USER/PASS/VHOST env vars to test' unless exists $ENV{NET_ASYNC_AMQP_HOST};

my $loop = IO::Async::Loop::Epoll->new;

# Set up a connection manager with our MQ server details
$loop->add(
	my $cm = Net::Async::AMQP::ConnectionManager->new
);
$cm->add(
	host  => $ENV{NET_ASYNC_AMQP_HOST},
	user  => $ENV{NET_ASYNC_AMQP_USER},
	pass  => $ENV{NET_ASYNC_AMQP_PASS},
	vhost => $ENV{NET_ASYNC_AMQP_VHOST},
	max_channels => 32,
);

{ # Exchange-to-exchange binding
	my $thing = 'aa00001';
	my $countdown = 1000;
	(fmap0 {
		$cm->request_channel(
			confirm_mode => 1,
		)->then(sub {
			my ($ch) = @_;
			eval {
				$ch->bus->subscribe_to_event(
					close => sub {
						note "Channel closed - @_";
						shift->unsubscribe;
					}
				);
				my $delivery = $loop->new_future;
				my @ev;
				my $rkey = $thing++;
				Future->needs_all(
					$ch->queue_declare(
						queue => '',
					),
					$ch->exchange_declare(
						exchange => 'test_channel_spam',
						type     => 'topic',
						auto_delete => 1,
					),
				)->then(sub {
					my ($q) = @_;
					if(rand > 0.9) {
						$q->delete;
					}
					Future->needs_all(
						$q->bind_exchange(
							exchange => 'test_channel_spam',
							routing_key => $rkey,
						),
					)
				})->then(sub {
					my ($q) = @_;
					$q->listen
				})->then(sub {
					my ($q, $ctag) = @_;
					note 'ctag is ' . $ctag;
					my $id = $ch->id;
					$ch->bus->subscribe_to_event(
						message => sub {
							my ($ev, $type, $payload, $ctag, $dtag, $rkey) = @_;
							note "Had message: $type, $payload on $id rkey $rkey with delivery $delivery";
							$delivery->done($type => $payload);
							$ev->unsubscribe;
						}
					);
					$ch->publish(
						exchange    => 'test_channel_spam',
						routing_key => $rkey,
						type        => 'some_type',
						payload     => 'test message',
					)->transform(done => sub { $q })
				})->then(sub {
					my ($q) = @_;
					Future->wait_any(
						$loop->delay_future(after => 10),
						$delivery
					)
				})->then(sub {
					ok($delivery->is_ready, 'delivery ready');
					ok(!$delivery->failure, 'did not fail');
					is_deeply([ $delivery->get ], [ 'some_type' => 'test message' ], 'had expected type and content');
					Future->wrap;
				})
			} or do {
				note "Exception - $@";
				Future->fail($@);
			}
		})->else_done
	} concurrent => 256, generate => sub { $countdown-- || () })->get;
}

$cm->shutdown->get;

done_testing;

