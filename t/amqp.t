use strict;
use warnings;

use Test::More;

use Net::Async::AMQP;

subtest 'spec handling' => sub {
	ok(-r $Net::Async::AMQP::XML_SPEC, 'spec is readable');
	ok($Net::Async::AMQP::SPEC_LOADED, 'spec was loaded');
	can_ok('Net::AMQP::Protocol::Connection::Open', 'new');
	can_ok('Net::AMQP::Frame', 'new');
	done_testing;
};

subtest 'channel IDs' => sub {
	my $mq = Net::Async::AMQP->new;
	is($mq->MAX_CHANNELS, 65535, 'max channels matches AMQP 0.9.1 spec');
	my %idmap = map {; $mq->next_channel => 1 } 1..$mq->MAX_CHANNELS;
	is(keys(%idmap), $mq->MAX_CHANNELS, 'assign all available channels');
	is($mq->next_channel, undef, 'undef after running out of channels');
	done_testing;
};

done_testing;

