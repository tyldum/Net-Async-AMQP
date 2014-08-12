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
$args{localhost} = [];
GetOptions(
	"host|h=s"    => \$args{host},
	"user|u=s"    => \$args{user},
	"pass=s"      => \$args{pass},
	"port=i"      => \$args{port},
	"vhost|v=s"   => \$args{vhost},
	"parallel=i"  => \$args{parallel},
	"forks=i"     => \$args{forks},
	"localhost=s" => $args{localhost},
) or die("Error in command line arguments\n");

my $loop = IO::Async::Loop->new;
$loop->resolver->configure(
	min_workers => 2,
	max_workers => 2,
);

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
my @hosts = @{ delete($args{localhost}) || [qw(localhost)] };
my $parallel = delete $args{parallel} || 128;
my $forks = delete $args{forks} || 1;

for my $idx (1..$forks) {
	my $loop_class = ref($loop);
	$loop->add(IO::Async::Process->new(
		stdin => { from => '' },
		stdout => {
			on_read => sub {
				my ($stream, $buffref, $eof) = @_;
				while($$buffref =~ s/^(.*)\n//) {
					my ($k, $v) = split /:/, $1;
					$stats{$k} += $v;
				}
				warn "Closed input" if $eof;
				return 0
			}
		},
		on_finish => sub { warn "Finished: @_\n" },
		code => sub {
		eval {
			my $loop = $loop_class->new;
			$loop->resolver->configure(
				min_workers => 1,
				max_workers => 1,
			);
			$loop->add(my $stdout = IO::Async::Stream->new_for_stdout);
			(fmap0 {
				$stdout->write("active:1\n");
				my $mq = Net::Async::AMQP->new(
					loop               => $loop,
					heartbeat_interval => 0,
				);
				my $k = "$mq";
				push @hosts, my $host = shift @hosts;
				$mq{$k} = Future->wait_any(
					$mq->connect(
						%args,
						local_host => $host,
						client_properties => {
							capabilities => {
								'consumer_cancel_notify' => $true,
								'connection.blocked'     => $true,
							},
						},
					)->on_fail(sub {
						$stdout->write("failed:1\n");
						warn "Failure: @_\n"
					})->on_done(sub {
						$stdout->write("success:1\n");
					}),
					$loop->timeout_future(after => 30)
					 ->on_fail(sub {
						$stdout->write("timeout:1\n");
					})
				)->on_ready(sub {
					$stdout->write("active:-1\n");
					$stdout->write("total:1\n");
				})
				 ->on_fail(sub { delete $mq{$k} })
				 ->else(sub { Future->wrap })
			} concurrent => $parallel, generate => sub { 1 })->get;
			1 } or warn "Failed: $@\n";
		}
	));
}
$loop->run;

