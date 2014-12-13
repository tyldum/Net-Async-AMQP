package Net::Async::AMQP::Utils;

use strict;
use warnings;

use parent qw(Exporter);

our @EXPORT_OK = our @EXPORT = qw(
	frame_info
);

sub frame_info($) {
	my ($frame) = @_;
	my $txt = get_frame_type($frame);
	if($frame->can('method_frame') && (my $method_frame = $frame->method_frame)) {
		note($_) for 
	} else {

	}
	return $txt;
}

{ # We cache the lookups since they're unlikely to change during the application lifecycle
my %types;
sub get_frame_type {
    my $frame = shift->method_frame;
    my $ref = ref $frame;
    return $types{$ref} if exists $types{$ref};
    my $re = qr/^Net::AMQP::Protocol::([^:]+::[^:]+)$/;
    my ($frame_type) = grep /$re/, Class::ISA::self_and_super_path($ref);
    ($frame_type) = $frame_type =~ $re;
    $types{$ref} = $frame_type;
    return $frame_type;
}
}
1;

