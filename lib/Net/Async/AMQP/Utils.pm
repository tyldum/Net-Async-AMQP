package Net::Async::AMQP::Utils;

use strict;
use warnings;

use parent qw(Exporter);

=head1 NAME

Net::Async::AMQP::Server::Exchange

=head1 SYNOPSIS

=head1 DESCRIPTION

=cut

our @EXPORT_OK = our @EXPORT = qw(
	amqp_frame_info
	amqp_frame_type
);

=head2 amqp_frame_info

=cut

sub amqp_frame_info($) {
	my ($frame) = @_;
	my $txt = amqp_frame_type($frame);
	if($frame->can('method_frame') && (my $method_frame = $frame->method_frame)) {
		#note($_);
	} else {

	}
	return $txt;
}

{ # We cache the lookups since they're unlikely to change during the application lifecycle

my %types;

=head2 amqp_frame_type

Takes the following parameters:

=over 4

=item * $frame - the L<Net::AMQP::Frame> instance

=back

Returns string representing type, typically the base class with Net::AMQP::Protocol prefix removed.

=cut

sub amqp_frame_type {
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

__END__

=head1 AUTHOR

Tom Molesworth <cpan@perlsite.co.uk>

=head1 LICENSE

Licensed under the same terms as Perl itself, with additional licensing
terms for the MQ spec to be found in C<share/amqp0-9-1.extended.xml>
('a worldwide, perpetual, royalty-free, nontransferable, nonexclusive
license to (i) copy, display, distribute and implement the Advanced
Messaging Queue Protocol ("AMQP") Specification').

