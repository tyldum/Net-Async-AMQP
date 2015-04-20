package Net::Async::AMQP::Utils;

use strict;
use warnings;

use parent qw(Exporter);

=head1 NAME

Net::Async::AMQP::Utils

=head1 SYNOPSIS

=head1 DESCRIPTION

All functions are imported by default.

=cut

our @EXPORT_OK = our @EXPORT = qw(
	amqp_frame_info
	amqp_frame_type
);

=head2 amqp_frame_info

Returns a string with information about the given AMQP frame.

=cut

sub amqp_frame_info($) {
	my ($frame) = @_;
	my $txt = amqp_frame_type($frame);
	$txt .= ', channel ' . $frame->channel if $frame->channel;
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
	my ($frame) = @_;
	return 'Header' if $frame->isa('Net::AMQP::Frame::Header');
	return 'Heartbeat' if $frame->isa('Net::AMQP::Frame::Heartbeat');
	return 'Unknown' unless $frame->can('method_frame');

	my $method_frame = shift->method_frame;
	my $ref = ref $method_frame;
	return $types{$ref} if exists $types{$ref};
	my $re = qr/^Net::AMQP::Protocol::([^:]+::[^:]+)$/;
	my ($frame_type) = grep /$re/, Class::ISA::self_and_super_path($ref);
	($frame_type) = $frame_type =~ $re;
	$types{$ref} = $frame_type;
	return $frame_type;
}
}

{
my %amqp_codes = (
	200 => { message => 'reply­success', description => 'Indicates that the method completed successfully. This reply code is reserved for future use ­ the current protocol design does not use positive confirmation and reply codes are sent only in case of an error.' },
	311 => { message => 'content­too­large', type => 'channel', description => 'The client attempted to transfer content larger than the server could accept at the present time. The client may retry at a later time.' },
	313 => { message => 'no­consumers', type => 'channel', description => 'When the exchange cannot deliver to a consumer when the immediate flag is set. As a result of pending data on the queue or the absence of any consumers of the queue.' },
	320 => { message => 'connection­forced', type => 'connection', description => 'An operator intervened to close the connection for some reason. The client may retry at some later date.' },
	402 => { message => 'invalid­path', type => 'connection', description => 'The client tried to work with an unknown virtual host.' },
	403 => { message => 'access­refused', type => 'channel', description => 'The client attempted to work with a server entity to which it has no access due to security settings.' },
	404 => { message => 'not­found', type => 'channel', description => 'The client attempted to work with a server entity that does not exist.' },
	405 => { message => 'resource­locked', type => 'channel', description => 'The client attempted to work with a server entity to which it has no access because another client is working with it.' },
	406 => { message => 'precondition­failed', type => 'channel', description => 'The client requested a method that was not allowed because some precondition failed.' },
	501 => { message => 'frame­error', type => 'connection', description => 'The sender sent a malformed frame that the recipient could not decode.  This strongly implies a programming error in the sending peer.' },
	502 => { message => 'syntax­error', type => 'connection', description => 'The sender sent a frame that contained illegal values for one or more fields.  This strongly implies a programming error in the sending peer.' },
	503 => { message => 'command­invalid', type => 'connection', description => 'The client sent an invalid sequence of frames, attempting to perform an operation that was considered invalid by the server. This usually implies a programming error in the client.' },
	504 => { message => 'channel­error', type => 'connection', description => 'The client attempted to work with a channel that had not been correctly opened. This most likely indicates a fault in the client layer.' },
	505 => { message => 'unexpected­frame', type => 'connection', description => 'The peer sent a frame that was not expected, usually in the context of a content header and body. This strongly indicates a fault in the peer\'s content processing.' },
	506 => { message => 'resource­error', type => 'connection', description => 'The server could not complete the method because it lacked sufficient resources. This may be due to the client creating too many of some type of entity.' },
	530 => { message => 'not­allowed', type => 'connection', description => 'The client tried to work with some entity in a manner that is prohibited by the server, due to security settings or by some other criteria.' },
	540 => { message => 'not­implemented', type => 'connection', description => 'The client tried to use functionality that is not implemented in the server.' },
	541 => { message => 'internal­error', type => 'connection', description => 'The server could not complete the method because of an internal error. The server may require intervention by an operator in order to resume normal operations.' },
);

=head2 message_for_code

Returns the name (short message) corresponding to the given status code.

Example:

 message_for_code(540) => 'not implemented'

=cut

sub message_for_code {
	my ($code) = @_;
	$amqp_codes{$code}{message}
}

=head2 description_for_code

Returns the description (long message) corresponding to the given status code.

=cut

sub description_for_code {
	my ($code) = @_;
	$amqp_codes{$code}{description}
}

=head2 type_for_code

Returns the type for the given status code - typically "channel" or "connection".

=cut

sub type_for_code {
	my ($code) = @_;
	$amqp_codes{$code}{type}
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

