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
my $total = 0; my $active = 0;
$loop->add(IO::Async::Timer::Periodic->new(
	interval => 2,
	reschedule => 'skip',
	on_tick => sub {
		printf "%d total, %d active\n", $total, $active;
	}
)->start);
my $true = (Net::AMQP->VERSION >= 0.06) ? Net::AMQP::Value->true : 1;
my %mq;
(fmap0 {
	++$active;
    my $mq = Net::Async::AMQP->new(
        loop               => $loop,
        heartbeat_interval => 0,
    );
    $mq{$mq} = $mq->connect(
		%args,
        client_properties => {
            capabilities => {
                'consumer_cancel_notify' => $true,
                'connection.blocked'     => $true,
            },
        },
	)->on_ready(sub { --$active; ++$total })
} concurrent => 64, generate => sub { 1 })->get;

