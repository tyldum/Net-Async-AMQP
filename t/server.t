use strict;
use warnings;

use Test::More;
use Future::Utils;
use IO::Async::Loop;
use Net::Async::AMQP;
use Net::Async::AMQP::Server;

my $loop = IO::Async::Loop->new;
my $srv = Net::Async::AMQP::Server->new;
$loop->add($srv);

my ($host, $port) = $srv->listening->get;

is($host, '0.0.0.0', 'host is 0.0.0.0');
$host = 'localhost';
ok($port, 'non-zero port');

my $cli = Net::Async::AMQP->new(
	loop => $loop
);
$cli->subscribe_to_event(
	close => sub { fail("close - @_") },
	unexpected_frame => sub { fail("unexpected - @_") },
);

my $true = (Net::AMQP->VERSION >= 0.06) ? Net::AMQP::Value->true : 1;
$cli->connect(
	host  => $host,
	user  => 'guest',
	pass  => 'guest',
	port  => $port,
	vhost => '/',
	client_properties => {
		capabilities => {
			'connection.blocked'     => $true,
			'consumer_cancel_notify' => $true,
		},
	},
)->get;
$cli->close->get;
done_testing;

