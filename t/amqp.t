use strict;
use warnings;

use Test::More;
use Test::Fatal;

use Net::Async::AMQP;
use IO::Async::Loop;

my $loop = IO::Async::Loop->new;

subtest 'spec handling' => sub {
	ok(-r $Net::Async::AMQP::XML_SPEC, 'spec is readable');
	ok($Net::Async::AMQP::SPEC_LOADED, 'spec was loaded');
	can_ok('Net::AMQP::Protocol::Connection::Open', 'new');
	can_ok('Net::AMQP::Frame', 'new');
	done_testing;
};

subtest 'channel IDs' => sub {
	# The full number of channels can take a while to process the tests, so first we check
	# that we have the right number in the 'constant':
	is(Net::Async::AMQP->MAX_CHANNELS, 65535, 'max channels matches AMQP 0.9.1 spec');

	# ... then we replace it with something more amenable to testing
	no warnings 'redefine';
	local *Net::Async::AMQP::MAX_CHANNELS = sub { 100 };

	$loop->add(my $mq = Net::Async::AMQP->new);
	my %idmap = map {; $mq->create_channel->id => 1 } 1..$mq->MAX_CHANNELS;
	is(keys(%idmap), $mq->MAX_CHANNELS, 'assign all available channels');
	is($mq->next_channel, undef, 'undef after running out of channels');
	# ok($mq->channel_by_id(1)->bus->invoke_event(close => 123, 'closing'), 'can close a channel');
	ok($mq->channel_closed(3), 'can close a channel');
	is($mq->next_channel, 3, 'have a valid channel ID again after closing');
	is($mq->next_channel, undef, 'and undef after running out again');
	is(exception {
		$mq->channel_closed($_) for 5..8;
	}, undef, 'close more channels');
	is($mq->create_channel->id, 5, 'reopen channel 5');
	is($mq->create_channel->id, 6, 'reopen channel 6');
	is($mq->create_channel->id, 7, 'reopen channel 7');
	is($mq->create_channel->id, 8, 'reopen channel 8');
	like(exception {
		$mq->create_channel->id
	}, qr/No channel available/, 'and ->create_channel gives an exception after running out again');
	done_testing;
};

done_testing;

