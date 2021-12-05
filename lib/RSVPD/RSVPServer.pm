##
# Module encompassing the core logic of the Permabit RSVP Daemon
#
# @synopsis
#
#     use RSVPServer;
#     $server = RSVPServer->new(file => /tmp/hosts.state);
#
# @description
#
# C<RSVPServer> contains the guts of the rsvpd server, factored out so
# that it can be unit tested reasonably.
#
# $Id: //eng/main/src/tools/rsvp/rsvpd/RSVPServer.pm#36 $
##
package RSVPD::RSVPServer;

use strict;
use Carp;
use RSVPD::Class;
use Data::Dumper;
use English;
use List::Util qw(shuffle);
use Log::Log4perl;
use Net::Ping 2.28;
use Permabit::Assertions qw(assertMinMaxArgs assertNumArgs);
use Permabit::Utils qw(canonicalizeHostname sendMail sendChat);
use RSVPD::Response qw(error success);
use Socket;
use Storable qw(dclone);

use base qw(Exporter);
our $VERSION = 1.0;
our @EXPORT = qw ( $DEFAULT_CLASS $DEFAULT_RESERVE_CLASS );

my $log = Log::Log4perl->get_logger(__PACKAGE__);

my %commands
  = ('add_class'           => [\&addClass,
                               ['class', 'members', 'description'], []],
     'add_host'            => [\&addHost,
                               ['host', 'classes'], []],
     'add_resource'        => [\&addResource,
                               ['resource', 'class'], []],
     'add_resource_class'  => [\&addResourceClass,
                               ['class', 'description'], []],
     'add_next_user'       => [\&addNextUser,
                               ['host', 'user', 'expire', 'msg'], []],
     'del_class'           => [\&delClass,
                               ['class'], []],
     'del_host'            => [\&delHost,
                               ['host'], []],
     'del_next_user'       => [\&delNextUser,
                               ['host', 'user'], []],
     'get_current_user'    => [\&getCurrentUser,
                               ['host'], []],
     'list_hosts'          => [\&listHosts,
                               ['class', 'user', 'verbose'],
                               ['next', 'hostRegexp']],
     'list_classes'        => [\&listClasses,
                               ['class'], []],
     'modify_host'         => [\&modifyHost,
                               ['host', 'user', 'addClasses', 'delClasses'],
                               []],
     'release_resource'    => [\&releaseResource,
                               ['resource', 'user', 'msg'], ['key', 'force']],
     'release_rsvp'        => [\&releaseReservation,
                               ['host', 'user', 'msg'], ['key', 'force']],
     'renew_rsvp'          => [\&renewReservation,
                               ['host', 'user', 'expire', 'msg'], []],
     'revive_host'         => [\&reviveHost,
                               ['host', 'all'], []],
     'rsvp_class'          => [\&reserveHostsByClass,
                               ['class', 'numhosts', 'user', 'expire', 'msg'],
                               ['key', 'randomize']],
     'rsvp_host'           => [\&reserveHostByName,
                               ['host', 'user', 'expire', 'msg'],
                               ['key', 'resource']],
     'verify_rsvp'         => [\&verifyReservation,
                               ['host', 'user'], []],
    );

# The default class which must always exist.
our $DEFAULT_CLASS = "ALL";

# The default class from which to reserve hosts
our $DEFAULT_RESERVE_CLASS = 'FARM';

##
# @paramList{new}
my %properties
  = (
     # @ple Hashref of class name to Class entries for all classes
     classes            => {},
     # @ple Time a host can be unreachable before it is marked dead
     deadTime           => 120,
     # @ple Hashref of hostname to Host entries for all hosts
     hosts              => {},
     # @ple The file from which to load state.
     file               => undef,
     # @ple The email address to which host problems are reported
     opsContact         => 'ops-notify@permabit.com',
     # @ple The number of seconds before timing out a process checking
     # connection
     procCheckTimeout   => 60,
    );
##

##########################################################################
# Create a new RSVPServer
#
# @params{new}
##
sub new {
  my ($invocant) = shift;
  my $class = ref($invocant) || $invocant;
  my $self = bless {
                    # Clone %properties so original isn't modified
                    %{ dclone(\%properties) },
                    # Overrides previous values
                    @_,
                   }, $class;
  if (!$self->{file}) {
    croak("Must set file in new RSVPServer objects");
  }
  return $self;
}

######################################################################
# Reload host and class state from file.  This will reset the hosts
# table.
##
sub load {
  my ($self) = assertNumArgs(1, @_);

  # hosts must be declared "our" so Data::Dumper writes into it
  our (%hosts, %classes);
  do $self->{file};
  $self->{hosts} = \%hosts;
  $self->{classes} = \%classes;

  foreach my $host (values %{$self->{hosts}}) {
    $log->info("load($host, " . ($host->getUser() || '')
               . ", [" . join(' ', $host->getClasses()) . "])");
  }

  # Make sure the default class exists
  $self->init();
}

######################################################################
# Create the default classes
##
sub init {
  my ($self) = assertNumArgs(1, @_);

  if (!$self->{classes}->{$DEFAULT_CLASS}) {
    $self->{classes}->{$DEFAULT_CLASS}
      = RSVPD::Class->new(name => $DEFAULT_CLASS,
                          description => "Default Class");
  }

  if (!$self->{classes}->{$DEFAULT_RESERVE_CLASS}) {
    $self->{classes}->{$DEFAULT_RESERVE_CLASS}
      = RSVPD::Class->new(name => $DEFAULT_RESERVE_CLASS,
                          description => "Default reservation class");
  }
}

######################################################################
# Save host state to a file.
##
sub saveState {
  my ($self) = assertNumArgs(1, @_);
  my $file = $self->{file};
  my $newFile = "$file.new";

  my $output = IO::File->new("> $newFile")
    || die("Couldn't open $newFile: $ERRNO\n");
  print $output "# RSPVPServer Statefile as of " . localtime() . "\n";
  # Use Data::Dumper so that we can simply eval the file later
  my $dumper = Data::Dumper->new([ $self->{hosts}, $self->{classes} ],
                                 [ qw(*hosts *classes) ]);
  $dumper->Purity(1);
  $dumper->Indent(3);
  print $output $dumper->Dump() , "\n";
  $output->close() || die("Couldn't close $newFile: $ERRNO\n");

  # atomically replace the old state so inconsistency can't happen
  rename($newFile, $file)
    || die("Couldn't replace state file $file with $newFile: $!");
}

######################################################################
# Dispatch the given command
#
# @param command        The name of the command to dispatch
# @param params         The parameters to pass to the command
#
# @return A hashref containing the result of the operation
##
sub dispatch {
  my ($self, $command, $params) = assertNumArgs(3, @_);

  if (!$commands{$command}) {
    # unknown request.  complain and tell client
    $log->warn("UNKNOWN COMMAND: '$command'");
    return Response::error("unknown request type: $command");
  } else {
    my $operation = $commands{$command}->[0];
    my @requiredParams = @{$commands{$command}->[1]};
    my @optionalParams = @{$commands{$command}->[2]};
    my $numParams = scalar(keys %{$params});
    foreach my $param (@optionalParams) {
      if (grep($_ eq $param, keys(%{$params}))) {
        $numParams--;
      }
    }
    if (scalar(@requiredParams) != $numParams) {
      return Response::error("Wrong params to $command, requires: "
                             . join(',', @requiredParams) . " optional: "
                             . join(',', @optionalParams));
    } else {
      foreach my $param (@requiredParams) {
        if (!defined($params->{$param})) {
          return Response::error("$param not included in params to $command");
        }
      }
    }
    return &$operation($self, $params);
  }
}

######################################################################
# Add a new class
##
sub addClass {
  my ($self, $params) = assertNumArgs(2, @_);
  my $answer = $self->_addClass($params, 0);
  my $memberNames = join(',', @{$params->{members}});
  $log->info("addClass($params->{class}, $memberNames) => $answer");
  return $answer;
}

######################################################################
# Add a new resource class
##
sub addResourceClass {
  my ($self, $params) = assertNumArgs(2, @_);
  my $answer = $self->_addClass($params, 1);
  $log->info("addResourceClass($params->{class}) => $answer");
  return $answer;
}

######################################################################
# Helper method to add a new class.
#
# @param params     The class params describing the class to add.
# @param isResource Is this a resource class or not.
##
sub _addClass {
  my ($self, $params, $isResource) = assertNumArgs(3, @_);
  my $className = $params->{class};
  my $type = $isResource ? "resourceClass" : "class";

  if (exists($self->{classes}->{$className})) {
    return Response::error("$type $className already exists");
  }
  if ($className !~ /[\w]+/) {
    return Response::error("invalid $type name: '$className'");
  }
  if ($isResource && $params->{members}
      && (scalar(@{$params->{members}} != 0))) {
    return Response::error("$type $className can't be composite: "
                           . join(" ", @{$params->{members}}));
  }

  my @members;
  foreach my $member (@{$params->{members}}) {
    my $class = $self->{classes}->{$member};
    if (!defined($class)) {
      return Response::error("member class '$member' does not exist");
    } elsif ($class->isResource()) {
      return Response::error("member class '$member' can not be a resource");
    } else {
      push(@members, $class);
    }
  }

  my $desc = $params->{description};
  $self->{classes}->{$className} = RSVPD::Class->new(name        => $className,
                                                     description => $desc,
                                                     resource    => $isResource,
                                                     members     => \@members);
  $self->saveState();
  return Response::success("added $type $className");
}

######################################################################
# Add a new host
##
sub addHost {
  my ($self, $params) = assertNumArgs(2, @_);
  my $hostname = canonicalizeHostname($params->{host});
  my $answer = $self->addHostOrResource($hostname, $params->{classes}, 0);
  $log->info("addHost($hostname) => $answer");
  return $answer;
}

######################################################################
# Add a new, unpingable resource.
##
sub addResource {
  my ($self, $params) = assertNumArgs(2, @_);
  my $answer
    = $self->addHostOrResource($params->{resource}, [$params->{class}], 1);
  $log->info("addResource($params->{resource}, $params->{class}) => $answer");
  return $answer;
}

######################################################################
# Utility method to add either a host or a resource.
##
sub addHostOrResource {
  my ($self, $host, $classNames, $isResource) = assertNumArgs(4, @_);
  my $answer;
  my $type = $isResource ? "Resource" : "Host";

  if (exists($self->{hosts}->{$host})) {
    $answer = Response::error("$type $host already exists");
  } elsif ($host !~ /[\w\.]+/) {
    # XXX What is this regexp trying to do???
    $answer = Response::error("invalid $type: $host");
  } else {
    # verify and lookup the provided classes
    if (!@{$classNames}) {
      $classNames = [$DEFAULT_CLASS];
    }

    my @classes;
    foreach my $className (@{$classNames}) {
      my $class = $self->{classes}->{$className};
      if (!$class) {
        $answer = Response::error("class $className doesn't exist");
        last;
      } elsif ($class->isComposite()) {
        $answer = Response::error("class $className is composite");
        last;
      } elsif ($class->isResource() != $isResource) {
        $answer
          = Response::error("Type of class $className and host $host differ");
        last;
      } else {
        push(@classes, $class);
      }
    }
    if (!$answer) {
      $self->{hosts}->{$host} = RSVPD::Host->new(hostname => $host,
                                                 classes => \@classes);
      $answer = Response::success("added $host");
      $self->saveState();
    }
  }

  return $answer;
}

######################################################################
# delete a class
##
sub delClass {
  my ($self, $params) = assertNumArgs(2, @_);
  my $className = $params->{class};
  my $answer;

  if ($className eq $DEFAULT_CLASS) {
    return Response::error("can't delete default class $DEFAULT_CLASS");
  }

  if (exists($self->{classes}->{$className})) {
    my $delClass = $self->{classes}->{$className};
    delete($self->{classes}->{$className});

    my @deletedResources;

    # Remove it from all hosts and classes
    foreach my $host (values %{$self->{hosts}}) {
      if ($delClass->isResource()) {
        if ($delClass->containsHost($host)) {
          # resources should be removed with their resource class
          push(@deletedResources, $host->getName());
          delete($self->{hosts}->{$host->getName()});
        }
      } else {
        $host->removeClass($delClass);
      }
    }
    foreach my $class (values %{$self->{classes}}) {
      $class->removeMember($delClass);
    }

    my $msg = "deleted $className";
    if (@deletedResources) {
      $msg .= " and resources " . join(', ', @deletedResources);
    }
    $answer = Response::success("$msg");
    $self->saveState();
  } else {
    $answer = Response::error("class does not exist: $className");
  }

  $log->info("delClass($className) => $answer");
  return $answer;
}

######################################################################
# delete a host
##
sub delHost {
  my ($self, $params) = assertNumArgs(2, @_);
  my $answer;
  my $hostname = canonicalizeHostname($params->{host});

  if (exists($self->{hosts}->{$hostname})) {
    delete($self->{hosts}->{$hostname});
    $answer = Response::success("deleted $hostname");
    $self->saveState();
  } else {
    $answer = Response::error("host $hostname does not exist");
  }

  $log->info("delHost($hostname) => $answer");
  return $answer;
}

######################################################################
# List all hosts
#
# @oparam params{class}   If set, list the hosts that are members of
#                          that class.
# @oparam params{user}    If set, the user whose hosts should be listed.
# @oparam params{next}    If set, the next user should be listed.
# @oparam params{verbose} If set, include class membership instead of
#                         expiry time and message
##
sub listHosts {
  my ($self, $params) = assertNumArgs(2, @_);
  $log->debug("processing list_hosts");
  if ($params->{hostRegexp}
      && (!eval { "" =~ /$params->{hostRegexp}/; 1 })) {
    return Response::error("Illegal host regexp: $params->{hostRegexp}");
  }

  my @hosts = (sort {$a->compareTo($b)} values %{$self->{hosts}});

  my $classExpr = $params->{class};
  if ($classExpr) {
    # List all hosts belonging to this class
    my $class = $self->_parseClassExpr($classExpr);
    if (! defined($class)) {
      return Response::error("No such class $classExpr");
    }
    @hosts = grep {$class->containsHost($_)} @hosts;
  }
  if ($params->{user}) {
    @hosts = grep {$_->reservedBy($params->{user})} @hosts;
  }
  if (!$classExpr && !$params->{user}) {
    # Default list doesn't include resources
    @hosts = grep {!$_->isResource()} @hosts;
  }
  if ($params->{hostRegexp}) {
    @hosts = grep {$_->getName() =~ /^$params->{hostRegexp}$/} @hosts;
  }

  my $listClasses = $params->{verbose};
  my @hostList = map {$_->toArray($listClasses, $params->{next})} @hosts;
  return Response::success("success", \@hostList);
}

######################################################################
# list all classes
##
sub listClasses {
  my ($self, $params) = assertNumArgs(2, @_);
  my @classes = (sort {$a->compareTo($b)} values %{$self->{classes}});
  my @classList = map {$_->toArray()} @classes;
  return Response::success("success", \@classList);
}

######################################################################
# Modify the list of classes a host belongs to.
##
sub modifyHost {
  my ($self, $params) = assertNumArgs(2, @_);
  my $hostname = canonicalizeHostname($params->{host});
  if (!exists($self->{hosts}->{$hostname})) {
    return Response::error("host $hostname not found");
  }

  my (@addClasses, @delClasses);
  my $numResources = 0;
  my $numNormals = 0;
  foreach my $className (@{$params->{addClasses}}) {
    my $class = $self->{classes}->{$className};
    if (!$class) {
      return Response::error("class $className doesn't exist");
    } elsif ($class->isComposite()) {
      return Response::error("class $className is composite");
    } else {
      if ($class->isResource()) {
        $numResources++;
      } else {
        $numNormals++;
      }
      push(@addClasses, $class);
    }
  }
  foreach my $className (@{$params->{delClasses}}) {
    my $class = $self->{classes}->{$className};
    if (!$class) {
      return Response::error("class $className doesn't exist");
    } else {
      push(@delClasses, $class);
    }
  }

  my $host = $self->{hosts}->{$hostname};
  foreach my $class ($host->getClasses()) {
    if (grep {$_->equals($class)} @delClasses) {
      next;
    }
    if ($class->isResource() != $numResources) {
      return
        Response::error("Can't have both resources and non-resources in list");
    }
    if ($class->isResource()) {
      $numResources++;
    }
  }

  if ($numResources > 1) {
    return Response::error("Resources may only belong to 1 resource class");
  }
  if (($numResources > 0) && ($numNormals > 0)) {
    return
      Response::error("Host can't be in both resource and normal classes");
  }

  foreach my $class (@addClasses) {
    $host->addClass($class)
  }
  foreach my $class (@delClasses) {
    $host->removeClass($class)
  }
  $self->saveState();
  my $classes = "added=" . join(',', @{$params->{addClasses}})
    . ", removed=" . join(',', @{$params->{delClasses}});
  my $answer = Response::success("modified host $hostname, $classes");
  $log->info("modifyClass($hostname, $params->{user}, $classes) => $answer");
  return $answer;
}

######################################################################
# reserve a single named host
##
sub reserveHostByName {
  my ($self, $params) = assertNumArgs(2, @_);
  my $hostname = canonicalizeHostname($params->{host});
  my $answer = $self->checkReservationParams($hostname, $params->{user});
  if ($answer->isSuccess()) {
    my $host = $self->{hosts}->{$hostname};
    if ($host->isReserved()) {
      $answer = Response::error("host $hostname already reserved by "
                                . $host->getUser(), 1);
    } elsif ($host->isResource() && !$params->{resource}) {
      $answer = Response::error("host $hostname is a resource and must "
                                . "be reserved using 'reserve_resources'");
    } elsif (!$host->isResource() && $params->{resource}) {
      $answer = Response::error("host $hostname is not a resource and must "
                                . "be reserved using 'reserve'");
    } else {
      # all systems go
      $host->clear();
      $host->setUser($params->{user});
      $host->setExpiryTime($params->{expire});
      $host->setMessage($params->{msg});
      $host->setKey($params->{key});
      $answer = Response::success("reserved $hostname");
      $self->saveState();
    }
  }
  $params->{key} ||= '';
  $log->info("reserveHost($hostname, $params->{user}, $params->{expire}, "
             . "$params->{msg}, $params->{key}) => $answer");
  return $answer;
}

######################################################################
# reserve mutiple hosts in a given class
##
sub reserveHostsByClass {
  my ($self, $params) = assertNumArgs(2, @_);
  my $className = $params->{class} || $DEFAULT_RESERVE_CLASS;

  if ($params->{user} eq "root") {
    return Response::error("user root may not reserve hosts");
  }

  my $class = $self->_parseClassExpr($className);
  if (! defined($class)) {
    return Response::error("class does not exist: '$className'");
  }

  # attempt to find n unreserved hosts belonging to the class
  my @hosts = (sort {$b->compareTo($a)} values %{$self->{hosts}});
  my @freehosts = grep {!$_->isReserved() && $class->containsHost($_)} @hosts;

  # if randomize is set, randomize the list
  if ($params->{randomize}) {
    @freehosts = shuffle(@freehosts);
  }

  my $answer = $self->_reserveHosts(\@freehosts, $params);

  $params->{randomize} ||= "";
  $params->{key}       ||= "";

  $log->info("reserveHostsByClass($className, $params->{numhosts}, "
             . "$params->{user}, $params->{expire}, $params->{msg}, "
             . "$params->{randomize}, $params->{key}) => $answer");
  return $answer;
}

######################################################################
# Parse a class expression
#
# The expression is a comma-separated list of one or more classes.
#
# @param classExpr  The expression describing the desired classes
#
# @return           A Class matching hosts described by classExpr, undef
#                   if no matching hosts.
##
sub _parseClassExpr {
  my ($self, $classExpr) = assertNumArgs(2, @_);

  my @classNames = split(/\s* , \s*/x, $classExpr);
  my @classes = ();
  foreach my $c (@classNames) {
    my $class = $self->{classes}->{$c};
    if (defined($class)) {
      push @classes, $class;
    } else {
      return undef;
    }
  }

  if (scalar(@classes) == 1) {
    return $classes[0];
  } else {
    return RSVPD::Class->new(name => $classExpr,
	                     members => \@classes,
	                     description => $classExpr);
  }
}

######################################################################
# Attempt to reserve numhosts hosts from the given listref of hosts.
#
# @param hosts     The listref of available hosts.
# @param params    The user parameters.
#
# @return          The answer.
##
sub _reserveHosts {
  my ($self, $hosts, $params) = assertNumArgs(3, @_);
  my $answer;

  if ($params->{numhosts} !~ /^\d+$/) {
    return Response::error("numhosts must be numeric: $params->{numhosts}");
  }

  if ($params->{numhosts} <= 0) {
    $answer = Response::error("invalid numhosts: $params->{numhosts}");
  } elsif (scalar(@{$hosts}) < $params->{numhosts}) {
    # didn't find enough open hosts
    $answer
      = Response::error("not enough free hosts to get $params->{numhosts}, "
                        . "have " . scalar(@{$hosts}) . " free", 1);
  } elsif (!$params->{user}) {
    # User undefined or ""
    $answer = Response::error("Invalid user '$params->{user}'");
  } else {
    # found enough open hosts
    my @hostnames;
    for (my $i = 0; $i < $params->{numhosts}; $i++) {
      my $host = $hosts->[$i];
      $host->clear();
      $host->setUser($params->{user});
      $host->setExpiryTime($params->{expire});
      $host->setMessage($params->{msg});
      $host->setKey($params->{key});
      # We iterate over the hosts backwards, so return the result
      # backwards too
      unshift(@hostnames, $host->getName());
    }
    $answer = Response::success("reserved " . join(" ",@hostnames),
                                \@hostnames);
    $self->saveState();
  }
  return $answer;
}

######################################################################
# Release one or more reserved resources. The host must be a resource
# type or an error will result.
##
sub releaseResource {
  my ($self, $params) = assertNumArgs(2, @_);
  my $answer = $self->_releaseReservation($params->{resource},
                                          $params->{user}, $params->{key},
                                          $params->{force}, 1);
  $params->{force} ||= 0;
  $params->{key} ||= '';
  $log->info("releaseResource($params->{resource}, $params->{user}"
             . ", $params->{key}, $params->{force}, $params->{msg})"
             . " => $answer");
  return $answer;
}

######################################################################
# Release one or more reserved hosts.
##
sub releaseReservation {
  my ($self, $params) = assertNumArgs(2, @_);
  my $hostname = canonicalizeHostname($params->{host});
  my $answer = $self->_releaseReservation($hostname, $params->{user},
                                          $params->{key}, $params->{force}, 0);
  $params->{force} ||= 0;
  $params->{key} ||= '';
  $log->info("releaseReservation($hostname, $params->{user}, $params->{key},"
             . " $params->{force}, $params->{msg}) => $answer");
  return $answer;
}

######################################################################
# Helper method to release one reserved host or resource. If there is
# a next user waiting to reserve the host or resource then reserve it
# for the next user.
#
# @param object         Either the class or resource to be released
# @param user           The user trying to release the object
# @param key            The key used when creating the reservation
# @param forceRelease   Ignore the key
# @param isResource     Whether or not the object is a resource
##
sub _releaseReservation {
  my ($self, $object, $user, $key, $forceRelease, $isResource)
    = assertNumArgs(6, @_);

  my $type = $isResource ? "resource" : "host";
  my $host = $self->{hosts}->{$object};
  my $answer;
  if (!defined($host)) {
    return Response::error("$type '" . ($object || '') . "' does not exist");
  } elsif ($isResource != $host->isResource()) {
    return Response::error("$object is not a type of $type");
  } elsif (!$host->reservedBy($user)) {
    return Response::error("$type $object not reserved by $user");
  } elsif (!$forceRelease && ($host->getKey() ne $key)) {
    my $expectedKey = $host->getKey() || '';
    return Response::error("Wrong key provided to release $type $object: "
                           . "expected '$expectedKey'");
  } else {
    if ($host->isNextUserSet()) {
      $host->reserveForNextUser();
      $answer = Response::success("released $object and reserved it for "
                                  . $host->getUser());
      my $expiry = $host->getExpiryTime();
      $expiry = $expiry ? localtime($expiry) : 'forever';
      my $currTime = localtime();

      my $message = <<EOF;
Reserved $host at $currTime until $expiry.

The Management.
EOF
      # send both mail and chat messages, and ignore errors
      my $user = $host->getUser();
      eval {
        sendChat(undef, $user, "RSVP notification", $message);
      };
      if ($EVAL_ERROR) {
        $log->error("Error sending chat message: $EVAL_ERROR");
      }
      eval {
        sendMail("rsvpd", $user, "Reserved $host", undef, $message);
      };
      if ($EVAL_ERROR) {
        $log->error("Error sending email: $EVAL_ERROR");
      }
    } else {
      $host->clear();
      $answer = Response::success("released $object");
    }
    $self->saveState();
  }
  return $answer;
}

######################################################################
# Verify one or more reserved hosts.
##
sub verifyReservation {
  my ($self, $params) = assertNumArgs(2, @_);
  my $answer;
  my $hostname = canonicalizeHostname($params->{host});
  my $host = $self->{hosts}->{$hostname};

  # verify a single reserved host
  if (!defined($host)) {
    $answer = Response::error("host $hostname does not exist");
  } elsif (!$host->reservedBy($params->{user})) {
    $answer = Response::error("host $hostname not owned by $params->{user}");
  } else {
    $answer = Response::success("verified $hostname");
  }

  $log->info("verifyReservation($hostname, $params->{user} => $answer");
  return $answer;
}

######################################################################
# Renew one or more reservations.
##
sub renewReservation {
  my ($self, $params) = assertNumArgs(2, @_);
  my $answer;

  my $hostname = canonicalizeHostname($params->{host});
  my $host = $self->{hosts}->{$hostname};

  # renew a single reserved host
  if (!defined($host)) {
    # host not found
    $answer = Response::error("host $hostname does not exist");
  } elsif (!$host->reservedBy($params->{user})) {
    # someone else has it
    $answer
      = Response::error("host $hostname not reserved by $params->{user}");
  } else {
    # all systems go
    $host->setExpiryTime($params->{expire});
    if ($params->{msg}) {
      $host->setMessage($params->{msg});
    }
    $answer = Response::success("renewed $hostname");
    $self->saveState();
  }

  $log->info("renewReservation($hostname, $params->{user}, $params->{expire})"
             . " => $answer");
  return $answer;
}

######################################################################
# Add a next user to a host that is already reserved
##
sub addNextUser {
  my ($self, $params) = assertNumArgs(2, @_);
  my $hostname = canonicalizeHostname($params->{host});

  my $answer = $self->checkReservationParams($hostname, $params->{user});
  if ($answer->isSuccess()) {
    my $host = $self->{hosts}->{$hostname};
    if (!$host->isReserved()) {
      $answer = Response::error("host $hostname is not reserved");
    } elsif ($host->getUser() eq  $params->{user}) {
      $answer = Response::error("host $hostname already reserved by "
                                . $host->getUser());
    } elsif ($host->isNextUserSet()
             && ($host->getNextUser() ne $params->{user})) {
      $answer = Response::error("host $hostname already has next user "
                                . $host->getNextUser());
    } else {
      # all systems go
      $host->setNextUser($params->{user});
      $host->setNextExpiryTime($params->{expire});
      $host->setNextMessage($params->{msg});
      $answer = Response::success("next user of $hostname is '$params->{user}'");
      $self->saveState();
    }
  }
  $log->info("addNextUser($hostname, $params->{user}, $params->{expire}, "
             . "$params->{msg}) => $answer");
  return $answer;
}

######################################################################
# Get the reservation owner of a host or undef if not reserved
##
sub getCurrentUser {
  my ($self, $params) = assertNumArgs(2, @_);
  my $hostname = canonicalizeHostname($params->{host});
  if (! defined($self->{hosts}->{$hostname})) {
    return Response::error("$hostname is not in rsvp");
  }
  my $host = $self->{hosts}->{$hostname};
  return Response::success("success", $host->getUser());
}

######################################################################
# Delete the next user from a host
##
sub delNextUser {
  my ($self, $params) = assertNumArgs(2, @_);
  my $hostname = canonicalizeHostname($params->{host});

  my $answer = $self->checkReservationParams($hostname, $params->{user});
  if ($answer->isSuccess()) {
    my $host = $self->{hosts}->{$hostname};
    if ($params->{user} eq $host->getNextUser()) {
      $host->clearNextUser();
      $answer = Response::success("cleared next user of $hostname");
      $self->saveState();
    } else {
      $answer = Response::error("'$params->{user}' cannot delete next user "
                                . $host->getNextUser());
    }
  }
  $log->info("delNextUser($hostname, $params->{user} => $answer");
  return $answer;
}

######################################################################
# Check the validity of reservation params.
##
sub checkReservationParams {
  my ($self, $hostname, $user, $expire) = assertMinMaxArgs(3, 4, @_);

  my $answer;
  if (!exists($self->{hosts}->{$hostname})) {
    # host isn't here
    $answer = Response::error("host $hostname not found");
  } elsif (!$user) {
    # User undefined or ""
    $answer = Response::error("Invalid user $user");
  } elsif ($user eq "root") {
    $answer = Response::error("user root may not reserve hosts");
  } elsif ($expire && ($expire !~ /^\d+$/)) {
    # Invalid expiry
    $answer = Response::error("Invalid expiry time $expire");
  } else {
    $answer = Response::success("");
  }
  return $answer;
}


######################################################################
# revive one or more hosts
##
sub reviveHost {
  my ($self, $params) = assertNumArgs(2, @_);
  my $answer;
  my @revivedHosts;

  if ($params->{all}) {
    # revive all hosts
    foreach my $host (values %{$self->{hosts}}) {
      if ($host->revive()) {
        push(@revivedHosts, $host->getName());
      }
    }
    $answer = Response::success("revived " . join(',', @revivedHosts));
  } else {
    my $hostname = canonicalizeHostname($params->{host});
    # revive a single host
    if (exists($self->{hosts}->{$hostname})) {
      my $host = $self->{hosts}->{$hostname};
      if ($host->revive()) {
        push(@revivedHosts, $hostname);
        $answer = Response::success("revived $hostname");
      } else {
        $answer = Response::error("host $hostname did not need to be revived");
      }
    } else {
      $answer = Response::error("host $hostname does not exist");
    }
  }
  $self->saveState();

  my $hosts = join(',', @revivedHosts);
  $log->info("reviveHost($hosts) => $answer");
  return $answer;
}

######################################################################
# Ping all hosts to make sure that they are still reachable.
#
# @param server         The RSVPServer object
##
sub pingHosts {
  my ($self) = assertNumArgs(1, @_);
  my $currTime = time();

  my $pinger = _getPinger();
  $pinger->{port_num} = getservbyname("time", "tcp");
  foreach my $host (values %{$self->{hosts}}) {
    # don't try to ping resources
    if (!$host->isResource()) {
      my $hostName = $host->getName();
      # ping() can fail if the host is unknown, which can happen under Vagrant
      # configurations if the VM is shut down.
      my @results = gethostbyname($hostName);
      if ($#results == -1) {
        $log->warn("hostname $hostName unknown, skipping");
        next;
      }
      eval {
        $pinger->ping($hostName);
      };
      if ($EVAL_ERROR) {
        $log->warn("Error pinging $hostName: $EVAL_ERROR");
      }
    }
  }
  my $pingStartTime = time();
  my %pinged;
  while (my ($hostname, $rtt, $ip) = $pinger->ack()) {
    my $host = $self->{hosts}->{$hostname};
    $pinged{$host} = 1;
    # host is pingable
    $host->setLastPingTime($currTime);
    # if it was previously lost, mark it found again
    if ($host->isDead()) {
      $host->revive();
      $self->saveState();
    }
  }
  my $pingDuration = time() - $pingStartTime;
  if ($pingDuration > 4) {
    $log->warn("Took $pingDuration s to ping all hosts");
  }

  # Check for unresponsive hosts
  foreach my $host (values %{$self->{hosts}}) {
    my $lastPing = $host->getLastPingTime();
    if (($lastPing > 0)
        && !$host->isDead() && !$host->isResource() && !$host->isReserved()
        && (($currTime - $lastPing) > $self->{deadTime})) {
      $log->warn("Marking $host dead due to unresponsiveness"
                 . " (last ping $lastPing, now $currTime)");
      $host->markDead();
      $self->saveState();
    }
  }
}

######################################################################
# Get a Net::Ping object to ping all stores with, using SYN pings
#
# @return The new object
##
sub _getPinger {
  return Net::Ping->new("syn", 5)
}

1;
