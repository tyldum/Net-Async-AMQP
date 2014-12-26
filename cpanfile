requires 'parent', 0;
requires 'curry', 0;
requires 'Future', '>= 0.30';
requires 'Mixin::Event::Dispatch', '>= 1.006';
requires 'IO::Async', '>= 0.63';
requires 'Net::AMQP', '>= 0.06';
requires 'Class::ISA', 0;
requires 'List::UtilsBy', 0;
requires 'File::ShareDir', 0;
requires 'IO::Socket::IP', 0;
requires 'Time::HiRes', 0;
requires 'List::UtilsBy', 0;

on 'test' => sub {
	requires 'Test::More', '>= 0.98';
	requires 'Test::Fatal', '>= 0.010';
	requires 'Test::Refcount', '>= 0.07';
	requires 'Test::HexString', 0;
	requires 'Test::MemoryGrowth', 0;
};
