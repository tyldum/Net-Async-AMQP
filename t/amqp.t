use strict;
use warnings;

use Test::More;

use Net::Async::AMQP;

{ # Spec handling
	ok(-r $Net::Async::AMQP::XML_SPEC, 'spec is readable');
	ok($Net::Async::AMQP::SPEC_LOADED, 'spec was loaded');
	can_ok('Net::AMQP::Protocol::Connection::Open', 'new');
	can_ok('Net::AMQP::Frame', 'new');
}

{ # Channel IDs
	my $mq = Net::Async::AMQP->new;
	my $id = 0;
	my %idmap = map {; $mq->next_channel => 1 } 1..65535;
	is(keys(%idmap), 65535, 'assign 64k unique channels');
	is($mq->next_channel, undef, 'undef after running out of channels');
}

done_testing;

