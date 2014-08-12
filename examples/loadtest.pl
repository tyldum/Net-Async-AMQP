#!/usr/bin/env perl
use strict;
use warnings;

use Net::Async::AMQP;
use IO::Async::Loop;
use IO::Async::Loop::Epoll;
use IO::Async::Timer::Periodic;
use Future::Utils qw(fmap0);
use feature qw(say);

use Getopt::Long;

my %args;
GetOptions(
	"host|h=s" => \$args{host},
	"user|u=s" => \$args{user},
	"pass=s" => \$args{pass},
	"port=i" => \$args{port},
	"vhost|v=s" => \$args{vhost},
) or die("Error in command line arguments\n");

my $loop = IO::Async::Loop->new;

say "start";
my %stats;
$loop->add(IO::Async::Timer::Periodic->new(
	interval => 2,
	reschedule => 'skip',
	on_tick => sub {
		say join ', ', map { sprintf "%d %s", $stats{$_}, $_ } sort keys %stats;
	}
)->start);
my $true = (Net::AMQP->VERSION >= 0.06) ? Net::AMQP::Value->true : 1;
my %mq;
(fmap0 {
	++$stats{active};
    my $mq = Net::Async::AMQP->new(
        loop               => $loop,
        heartbeat_interval => 0,
    );
	my $k = "$mq";
	$mq{$k} = Future->wait_any(
		$mq->connect(
			%args,
			client_properties => {
				capabilities => {
					'consumer_cancel_notify' => $true,
					'connection.blocked'     => $true,
				},
			},
		)->on_fail(sub { ++$stats{failed} }),
		$loop->timeout_future(after => 15)
		 ->on_fail(sub { ++$stats{timeout} })
	)->on_ready(sub { --$stats{active}; ++$stats{total} })
	 ->on_fail(sub { delete $mq{$k} })
} concurrent => 128, generate => sub { 1 })->get;

