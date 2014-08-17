package Net::Async::AMQP::Server::Protocol;

use strict;
use warnings;

sub new { my ($class) = shift; bless { @_ }, $class }

# use parent qw(Net::Async::AMQP);

sub write { my $self = shift; $self->{write}->(@_) }

sub on_read {
	my ($self, $buffer, $eof) = @_;
	return 0 unless length $$buffer >= length Net::AMQP::Protocol->header;
	$self->{initial_header} = substr $$buffer, 0, length Net::AMQP::Protocol->header, '';
	my ($proto, $version) = $self->{initial_header} =~ /^(AMQP)(....)/ or die "Invalid header received: " . sprintf "%v02x", $self->{initial_header};
	warn "Protocol $proto, version " . join '.', sprintf '%08x', unpack 'N1', $version;
	$self->curry::weak::startup;
}

sub startup {
	my ($self, $stream, $buffer, $eof) = @_;
	warn "In startup: @_";
	my $frame = Net::AMQP::Frame::Method->new(
		channel => 0,
		method_frame => Net::AMQP::Protocol::Connection::Start->new(
			server_properties => { },
			mechanisms        => 'AMQPLAIN',
			locale            => 'en_GB',
		),
	);
    $frame = $frame->frame_wrap if $frame->isa("Net::AMQP::Protocol::Base");
    $frame->channel(0) unless defined $frame->channel;
	$self->write($frame->to_raw_frame);
    $self->push_pending(
        'Connection::StartOk' => $self->can('start_ok'),
        'Connection::Close'  => $self->can('conn_close'),
	);
	$self->curry::weak::conn_start;
}
use Data::Dumper;

sub conn_start {
	my ($self, $stream, $buffer, $eof) = @_;
	warn "Have " . length($$buffer) . " bytes of post-connect data\n";
	for my $frame (Net::AMQP->parse_raw_frames($buffer)) {
		warn ":: Frame $frame\n" . Dumper($frame);
		$self->process_frame($frame);
	}
	0;
}

sub start_ok {
	my ($self, $frame) = @_;
	warn "Start okay:\n";
	my $method_frame = $frame->method_frame;
	warn "Auth:     " . $method_frame->mechanism;
	warn "Locale:   " . $method_frame->locale;
	warn "Response: " . $method_frame->response;
	$self->send_frame(
		Net::AMQP::Protocol::Connection::Tune->new(
			channel_max => 12 || $self->channel_max,
			frame_max   => $self->frame_max,
			heartbeat   => $self->heartbeat_interval,
		)
	);
    $self->push_pending(
        'Connection::TuneOk' => $self->can('tune_ok'),
	);
}

sub tune_ok {
	my ($self, $frame) = @_;
	warn "Tune okay:\n";
	my $method_frame = $frame->method_frame;
	warn "Channels:  " . $method_frame->channel_max;
	warn "Max size:  " . $method_frame->frame_max;
	warn "Heartbeat: " . $method_frame->heartbeat;
	$self->send_frame(
		Net::AMQP::Protocol::Connection::OpenOk->new(
			reserved_1 => '',
		)
	);
}

sub process_frame {
	my ($self, $frame) = @_;
	$self->SUPER::process_frame($frame);
}

sub conn_close {
	my ($self, $frame) = @_;
	warn "Close request\n";
	my $method_frame = $frame->method_frame;
	warn "Code:   " . $method_frame->reply_code;
	warn "Text:   " . $method_frame->reply_text;
	warn "Class:  " . $method_frame->class_id;
	warn "Method: " . $method_frame->method_id;
	$self->send_frame(
		Net::AMQP::Protocol::Connection::CloseOk->new(
		)
	);
}

1;
