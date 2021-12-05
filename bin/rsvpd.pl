#!/usr/bin/perl

use strict;
use warnings FATAL => qw(all);
use FindBin;

# Use version of modules in current tree, if they exists, and fall
# back to installed copies, then versions built by nightly.  These are
# in the opposite order, since they get "unshift"'d onto the @INC
# list.
use lib "/permabit/build/perl/lastrun/lib";
use lib "/usr/lib/rsvpd/";
use lib "$FindBin::Bin/../../../perl/lib";
use lib "$FindBin::Bin";

use Data::Dumper;
use English qw( -no_match_vars );
use Getopt::Long;
use RSVPD::Host;
use IO::Select;
use IO::Socket;
use JSON qw(from_json to_json);
use Log::Log4perl;
use Net::hostent;
use POSIX;
use Permabit::Assertions qw(assertNumArgs);
use Permabit::Utils qw(getUserName
                       redirectOutput
                       restoreOutput
                       sendMail
                       sendChat);
use Pod::Text;
use RSVPD::RSVPServer;
use Socket;
use Sys::Hostname;
use Time::HiRes qw(sleep);

# Make sure we get called on any signal
use sigtrap qw(die any normal-signals error-signals);

my $log = Log::Log4perl->get_logger(__PACKAGE__);

# The default log4perl configuration file
my $DEFAULT_CONFIG = '/etc/rsvpd/log.conf';

# The default delay between attempts to ping hosts
my $DEFAULT_PING_DELAY = 60;

# Maximum number of times to retry sending response to the client
my $MAX_RESPONSE_TRIES = 50;

# The default hosts state file
my $DEFAULT_STATE_FILE = 'hosts.state';

# The default port the server should listen on
my $DEFAULT_PORT = 1752;

my $Time = time();
my $notifyInterval = 6 * 60 * 60;  # 6 hours
my $notifyExpired = 1;

$Data::Dumper::Purity = 1;
$Data::Dumper::Indent = 0;

use vars qw($Select %Clients $VAR1);

redirectOutput("/tmp/rsvpd.stdout");
main();
restoreOutput();
exit(0);                        # not ever reached

##############################################################################
# main
##
sub main {
  my $config    = $DEFAULT_CONFIG;
  my $pingDelay = $DEFAULT_PING_DELAY;
  my $port      = $DEFAULT_PORT;
  my $stateFile = $DEFAULT_STATE_FILE;

  if (!GetOptions("config=s"           => \$config,
                  "statefile=s"        => \$stateFile,
                  "help"               => \&usage,
                  "pingdelay=i"        => \$pingDelay,
                  "port=i"             => \$port,
                  "version"            => \&version,
                  "notifyExpired!"     => \$notifyExpired,
                 )) {
    &usage();
  }

  Log::Log4perl->init($config);

  my $login = getUserName();
  $log->warn("STARTING as $login");
  $log->warn("port: $port");
  $log->warn("dhost: " . hostname());

  my $server = RSVPD::RSVPServer->new(file => $stateFile);
  # load state from disk
  if (-f $stateFile) {
    $server->load();
  } else {
    $server->init();
  }

  # start the server
  start($server, $port, $pingDelay);
  die("Impossibly, start() has returned!");
}

######################################################################
# Convert the answer to a given command into a format suitable to be
# sent over the wire
#
# @param client         The RSVP client hash
# @param cmd            The command this response is for
# @param response       A reference to the response
#
# @return a string ready to be sent over the wire
##
sub encodeResponse {
  my ($client, $cmd, $response) = assertNumArgs(3, @_);

  if ($client->{jsonUsed}) {
    my $json = to_json($response->encode(), { space_after => 1, utf8 => 1 });
    if ($log->is_debug()) {
      $log->debug("RESPONSE: " . $json);
    }
    return ($cmd . " " . length($json) . "\n" . $json);
  } else {
    my $serialized = ($cmd . " " . unpack("h*", Dumper($response)) . "\n");
    if ($log->is_debug()) {
      $log->debug("AFREEZE $cmd: " . Dumper($response));
    }
    return ($serialized . "DONE\n");
  }
}

######################################################################
# Check whether a client receive buffer contains a complete JSON-encoded
# RSVP request. If it does, the complete request is consumed from the
# buffer and decoded into an RSVP command and parameter hash.
#
# @param client  The RSVP client hash
#
# @return a list containing the command name and a hashref of parameters,
#         or undef if a command is not available (or was invalid)
##
sub parseJSONRequest {
  my ($client) = assertNumArgs(1, @_);

  # Check whether the request buffer contains all the expected JSON data.
  if (length($client->{requestBuffer}) < $client->{jsonLength}) {
    return undef;
  }

  # Consume the complete JSON command from the start of the buffer.
  my $json = substr($client->{requestBuffer}, 0, $client->{jsonLength}, "");
  $client->{jsonLength} = 0;

  if ($log->is_debug()) {
    $log->debug("RSVPD GOT: " . $json);
  }
  my $request = eval {
    return from_json($json, { utf8 => 1 });
  };
  if ($EVAL_ERROR) {
    $log->error("parseJSONRequest failed with: $EVAL_ERROR");
    return undef;
  }

  if (!defined($request->{cmd}) || !defined($request->{params})) {
    $log->error("parseJSONRequest ignoring malformed request: $json");
    return undef;
  }
  return ($request->{cmd}, $request->{params});
}

######################################################################
# Convert a string received over the wire into the command and the
# parameters it encodes.
#
# @param line   The string received over the wire
#
# @return a list containing the command name and a hashref of parameters
##
sub decodeMessage {
  my ($line) = assertNumArgs(1, @_);

  my ($cmd, $serialized) = ($line =~ /^(\S+)\s*(\S*)/);
  if (!$serialized) {
    # XXX What can we do here?
    $log->warn("Got null serialization for $cmd");
  }
  my $ref = eval(pack("h*", $serialized));
  if ($log->is_debug()) {
    $log->debug("$cmd: " . Dumper($ref));
  }
  return ($cmd, $ref);
}

######################################################################
# Check whether a client receive buffer contains a complete RSVP request. If
# it does, consumes the request from the buffer and decodes it into an RSVP
# command and parameter hash.
#
# @param client  The RSVP client hash
#
# @return a list containing the command name and a hashref of parameters,
#         or undef if a command is not available (or was invalid)
##
sub parseRequest {
  my ($client) = assertNumArgs(1, @_);

  # If a JSON-encoding request is still pending, try to parse it.
  if ($client->{jsonLength} > 0) {
    return parseJSONRequest($client);
  }

  # Try to consume a fully-formed request or request header from the buffer.
  if ($client->{requestBuffer} !~ s/^([^\n]*)\n//) {
    # We're expecting an RSVP command, but haven't received all of it yet.
    return undef;
  }
  my $line = $1;
  if ($log->is_debug()) {
    $log->debug("RSVPD GOT: $line");
  }

  # Handle the case of the RSVP command being a JSON request header.
  if ($line =~ /^json\s*(\d*)/) {
    # For the JSON header, the argument is just the request length, not hex.
    $client->{jsonLength} = $1;
    # Make a note to send the response back to the client as JSON, not Dumper.
    $client->{jsonUsed} = 1;
    # Attempt to parse any JSON input remaining in the request buffer.
    return parseJSONRequest($client);
  }

  # Otherwise, decode the line as a classic RSVP/Dumper request.
  return decodeMessage($line);
}

######################################################################
# Close this client
#
# @param client         The client to close the connection to
##
sub closeClientConnection {
  my ($client) = assertNumArgs(1, @_);

  my $fh = $client->{socket};
  $log->debug("Closing client $fh");
  $Select->remove($fh);
  eval {
    $fh->close();
  };
  delete $Clients{$fh};
}

######################################################################
# Send a response to the client
#
# @param client         the client to send to
# @param response       The response to send
##
sub clientSend {
  my ($client, $response) = assertNumArgs(2, @_);

  my $fh = $client->{socket};
  my $retryCount = 0;
  while ($response ne "") {
    my $rv = eval { return $fh->send($response, 0); };
    if (!$fh || !$fh->connected()
        || ($retryCount++ > $MAX_RESPONSE_TRIES)
        || ($ERRNO && ($ERRNO != POSIX::EWOULDBLOCK))) {
      # EOF???
      closeClientConnection($client);
      last;
    }
    if (!defined($rv)) {
      # Couldn't write: very rare
      sleep(0.1);
      next;
    }
    # Truncate what did get out
    substr($response, 0, $rv) = '';
  }
}

######################################################################
# Loop getting commands from a specific client
##
sub serviceClient {
  my ($server, $client) = assertNumArgs(2, @_);

  # Read the data available on the socket.
  my $data = '';
  my $rv = $client->{socket}->recv($data, POSIX::BUFSIZ, 0);
  if (!defined($rv) || (length($data) == 0)) {
    # End of the file
    closeClientConnection($client);
    return;
  }

  # Append the data we just read to to the client request buffer.
  $client->{requestBuffer} .= $data;

  # Process all complete requests in the buffer.
  while (1) {
    my ($cmd, $params) = parseRequest($client);
    if (!defined($cmd)) {
      return;
    }

    my $response = $server->dispatch($cmd, $params);
    clientSend($client, encodeResponse($client, $cmd, $response));
  }
}

######################################################################
# Check all reserved machines to see if their reservations have
# expired, and if so notify the user reserving them.
##
sub checkReservations {
  my ($server) = assertNumArgs(1, @_);

  foreach my $host (values %{$server->{hosts}}) {
    if ($host->isReserved() && ($Time > $host->getExpiryTime())) {
      notifyExpiration($host);
    }
  }
}

######################################################################
# Notify the owner of a machine that their reservation on it has
# expired.  Keep track of how recently the user has been notified of
# the same expiration to avoid flooding the user.
#
# @param host           The host whose reservation has expired
##
sub notifyExpiration {
  my ($host) = assertNumArgs(1, @_);

  # if the next notification time is in the future, do nothing (yet)
  my $nextNotify = $host->getNextNotifyTime();
  my $currTime = time();
  if ($currTime < $nextNotify) {
    return;
  }
  $host->setNextNotifyTime($currTime + $notifyInterval);
  if ($host->isDead()) {
    return;
  }

  my $hostname = $host->getName();
  my $expireTime = scalar(localtime($host->getExpiryTime()));
  my $message = <<EOF;
Your reservation on $hostname expired at $expireTime.
Please either release the machine or renew your reservation.

The Management.
EOF

  # initially, send both mail and a chat message
  my $user = $host->getUser();
  eval {
    sendChat(undef, $user, "RSVP notification", $message);
  };
  if ($EVAL_ERROR) {
    $log->error("sendChat failed with: $EVAL_ERROR");
  }
  if ($nextNotify == 0) {
    eval {
      sendMail("rsvpd", $user, "Expired reservation on $host", undef,
               $message);
    };
    if ($EVAL_ERROR) {
      $log->error("sendMail failed with: $EVAL_ERROR");
    }
  }
}

######################################################################
# Establish the server
##
sub start {
  my ($server, $port, $pingDelay) = assertNumArgs(3, @_);

  # Open the socket
  $log->info("Server up, listening on $port");
  my $socket = IO::Socket::INET->new(Proto     => 'tcp',
                                     LocalPort => $port,
                                     Listen    => SOMAXCONN,
                                     Reuse     => 1)
    || die("$0: Error, socket: $!");

  $Select = IO::Select->new($socket);

  my $nextPingTime = 0;
  while (1) {
    # Anything to read?
    foreach my $fh ($Select->can_read($pingDelay)) {
      $Time = time();           # Cache the time
      if ($fh == $socket) {
        # Accept a new connection
        my $clientfh = $socket->accept();
        if (!$clientfh) {
          next;
        }
        $Select->add($clientfh);
        my $flags = fcntl($clientfh, F_GETFL, 0)
          || die("Can't get flags");
        fcntl($clientfh, F_SETFL, $flags | O_NONBLOCK)
          || die("Can't nonblock");
        my $client = {
                      socket        => $clientfh,
                      requestBuffer => undef,
                      jsonLength    => 0,
                      jsonUsed      => 0,
                     };
        $Clients{$clientfh} = $client;
      } else {
        # Input traffic on other client
        serviceClient($server, $Clients{$fh});
      }
    }

    # Action or timer expired, only do this if time passed
    if ($Time != time()) {
      $Time = time();           # Cache the time
      if ($notifyExpired) {
        checkReservations($server);
      }

      # Regulate our pinging
      if ($Time >= $nextPingTime) {
        $server->pingHosts();
        $nextPingTime = time() + $pingDelay;
      }
    }
  }
}

##############################################################################
sub version {
  print 'Version: $Id: //eng/main/src/tools/rsvp/rsvpd/rsvpd.pl#48 $ ';
  print "\n";
  exit(1);
}

######################################################################
sub usage {
  print '$Id: //eng/main/src/tools/rsvp/rsvpd/rsvpd.pl#48 $ ', "\n";
  $SIG{__WARN__} = sub{};       #pod2text isn't clean.
  pod2text($0);
  exit(1);
}

######################################################################
__END__

=pod

=head1 NAME

rsvpd - machine reservation system rsvpd daemon

=head1 SYNOPSIS

B<rsvpd>
[ B<--help> ]
[ B<--version> ]
[ B<--port=>I<port> ]
[ B<--statefile=>I<file> ]
[ B<--config=>I<file> ]
[ B<--pingdelay=>I<seconds> ]

=head1 DESCRIPTION

rsvpd is a daemon to choose machines for the machine reservation
system. It needs to run as user I<rsvp> to check for lingering
processes on reserved machines.

=head1 ARGUMENTS

=over 4

=item --help

Displays this message and program version and exits.

=item --version

Displays program version and exits.

=item --port

Specifies the port number that the rsvpd listens on.

=item --statefile

Specifies the name of the file in which to store information about the
status of machines in the system.  This file will be read on startup
and periodically written to during normal operation.  The default
value is 'hosts.state'.

=item --config

Specifies the name of the file used to configure log4perl.  Defaults
to /etc/rsvpd/log.conf.

=item --pingdelay

Specifies the time to wait (in seconds) before pinging all hosts for
liveness.  The default value is 60 seconds.

=back

=head1 SEE ALSO

C<rsvpclient.pl>

=head1 AUTHORS

Permabit Tools Team, <dev-tools@permabit.com>, with code from
Schedule::Load, by Wilson Snyder <wsnyder@wsnyder.org>

=cut
######################################################################
