#!/usr/bin/env perl 
use strict;
use warnings;
use Future::Utils;
use IO::Async::Loop;
use Net::Async::AMQP::ConnectionManager;

my $loop = IO::Async::Loop->new;

# Set up a connection manager with our MQ server details
my $cm = Net::Async::AMQP::ConnectionManager->new;
$loop->add($cm);
$cm->add(
  host  => '10.2.0.44',
  user  => 'guest',
  pass  => 'guest',
  vhost => '/',
);

my @seen;
(Future::Utils::fmap_void {
	$cm->request_channel->then(sub {
		my $ch = shift;
		warn "Have channel " . $ch->id . " on " . $ch->amqp . "\n";
		$ch->exchange_declare(
			exchange => 'test_exchange',
			type     => 'fanout',
		)
		# push @seen, $ch;
#		Future->wrap;
	})->on_done(sub { warn "Done here\n" });
} foreach => [1..8], concurrent => 4)->then(sub {
	$cm->shutdown;
})->get;

