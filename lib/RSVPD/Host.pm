##
# Module to represent a host as handled by the rsvp daemon.
#
# @synopsis
#
#     use Host;
#     $host = Host->new(hostname => "farm-126");
#     $host->setUser("bob");
#     $host->setMsg("i got it");
#     $host->setExpiryTime(100);
#     $host->markDead();
#     $host->revive();
#     $origUser = $host->getUser();
#
# @description
#
# C<Host> is effectively a data structure containing the information
# regarding a machine that has been added to the rsvp daemon, along
# with methods for accessing and manipulating that data.
#
# $Id: //eng/main/src/tools/rsvp/rsvpd/Host.pm#12 $
##
package RSVPD::Host;

use strict;
use English;
use Carp;
use Log::Log4perl;
use Permabit::Assertions qw(assertNumArgs);
use Permabit::Utils qw(shortenHostName);
use RSVPD::RSVPServer;
use Storable qw(dclone);

my $log = Log::Log4perl->get_logger(__PACKAGE__);

# Overload stringification to print the hostname
use overload q("") => \&as_string;

##
# @paramList{new}
my %properties
  = (
     # @ple A listref of Class names to which this host belongs
     classes            => [],
     # @ple The time at which this host reservation expires
     expiry             => 0,
     # @ple The hostname of this host
     hostname           => undef,
     # @ple A key used to ensure that only the reserving user can
     # release this host
     key                => undef,
     # @ple The time at which this host was last pinged
     lastPingTime       => undef,
     # @ple The message provided when reserving this host
     msg                => '',
     # @ple The time at which this server should next be notified???
     nextNotify         => 0,
     # @ple The next user who gets the reservation after the current user
     nextUser           => undef,
     # @ple The message provided when reserving this host for next user
     nextMsg            => '',
     # @ple The time at which this host reservation expires for next user
     nextExpiry         => 0,
     # @ple The expiry time of this host prior to it being marked DEAD
     oldExpiry          => 0,
     # @ple The reservation message of this host prior to it being
     # marked DEAD
     oldMsg             => undef,
     # @ple The owner of this host prior to it being marked DEAD
     oldUser            => undef,
     # @ple The user who has reserved this host
     user               => undef,
    );
##

##########################################################################
# Create a new Host
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
  if (!$self->{hostname}) {
    croak("Must set hostname in new Host objects");
  }
  $self->{lastPingTime} = time();
  return $self;
}

##########################################################################
# Clear all state except the hostname from this Host.
##
sub clear {
  my ($self) = assertNumArgs(1, @_);
  $self->{expiry} = 0;
  $self->{key} = undef;
  $self->{msg} = undef;
  $self->{nextNotify} = 0;
  $self->{nextUser} = undef;
  $self->{nextExpiry} = 0;
  $self->{nextMsg} = undef;
  $self->{oldExpiry} = 0;
  $self->{oldMsg} = undef;
  $self->{oldUser} = undef;
  $self->{user} = undef;
}

##########################################################################
# Save all current information.
##
sub _cacheValues {
  my ($self) = assertNumArgs(1, @_);
  $self->{oldUser} = $self->{user};
  $self->{oldExpiry} = $self->{expiry};
  $self->{oldMsg} = $self->{msg};
}

##########################################################################
# Returns true if this host is a member of any resource classes.
##
sub isResource {
  my ($self) = assertNumArgs(1, @_);
  return grep {$_->isResource()} @{$self->{classes}};
}

##########################################################################
# Mark this server dead.
##
sub markDead {
  my ($self) = assertNumArgs(1, @_);
  $self->_cacheValues();
  $self->{user} = "DEATH";
  $self->{expiry} = 0;
  $self->{msg} = "Lost contact at: " . scalar(localtime());
}

##########################################################################
# If this host is dead, reset the user, expiry time, and next
# notification time for this server to their values prior to it being
# marked dead.
#
# @return 1 if the host has been revived, otherwise 0.
##
sub revive {
  my ($self) = assertNumArgs(1, @_);

  if ($self->isDead()) {
    $self->{user} = $self->{oldUser};
    $self->{oldUser} = undef;
    $self->{expiry} = $self->{oldExpiry};
    $self->{oldExpiry} = undef;
    $self->{msg} = $self->{oldMsg};
    $self->{oldMsg} = undef;
    my $user = ($self->{user}) ? "for $self->{user}!" : '';
    $log->warn("$self->{hostname} has been revived $user");
    return 1;
  }
  return 0;
}

##########################################################################
# Reserve this host for the next user
##
sub reserveForNextUser {
  my ($self) = assertNumArgs(1, @_);

  $self->{user} = $self->{nextUser};
  $self->{expiry} = $self->{nextExpiry};
  $self->{msg} = $self->{nextMsg};
  $self->{key} = undef;
  $self->clearNextUser();
}

##########################################################################
# Clear next user state
##
sub clearNextUser {
  my ($self) = assertNumArgs(1, @_);

  $self->{nextUser} = undef;
  $self->{nextExpiry} = 0;
  $self->{nextMsg} = undef;
}

##########################################################################
# Is this host reserved?
##
sub isReserved {
  my ($self) = assertNumArgs(1, @_);
  return defined($self->{user});
}

##########################################################################
# Is there a next user waiting to reserve this host?
##
sub isNextUserSet {
  my ($self) = assertNumArgs(1, @_);
  return defined($self->{nextUser});
}

##########################################################################
# Is this host reserved by the given user?
##
sub reservedBy {
  my ($self, $user) = assertNumArgs(2, @_);
  return (defined($self->{user}) && ($self->{user} eq $user));
}

##########################################################################
# Is this host dead?
##
sub isDead {
  my ($self) = assertNumArgs(1, @_);
  return ($self->{user} && ($self->{user} eq "DEATH"));
}

##########################################################################
# Get the expiry time for this host
##
sub getExpiryTime {
  my ($self) = assertNumArgs(1, @_);
  return $self->{expiry};
}

##########################################################################
# Set the expiry time for this host
##
sub setExpiryTime {
  my ($self, $expiry) = assertNumArgs(2, @_);
  $self->{expiry} = $expiry;
}

##########################################################################
# Get the last ping time for this host
##
sub getLastPingTime {
  my ($self) = assertNumArgs(1, @_);
  return $self->{lastPingTime};
}

##########################################################################
# Set the last ping time for this host
##
sub setLastPingTime {
  my ($self, $time) = assertNumArgs(2, @_);
  $self->{lastPingTime} = $time;
}

##########################################################################
# Get the reservation message for this host
##
sub getMessage {
  my ($self) = assertNumArgs(1, @_);
  return $self->{msg};
}

##########################################################################
# Set the reservation message for this host
##
sub setMessage {
  my ($self, $message) = assertNumArgs(2, @_);
  $self->{msg} = $message;
}

##########################################################################
# Get the name of this host
##
sub getName {
  my ($self) = assertNumArgs(1, @_);
  return $self->{hostname};
}

##########################################################################
# Get the next notification time for this host
##
sub getNextNotifyTime {
  my ($self) = assertNumArgs(1, @_);
  return $self->{nextNotify};
}

##########################################################################
# Set the next notification time for this host
##
sub setNextNotifyTime {
  my ($self, $notify) = assertNumArgs(2, @_);
  $self->{nextNotify} = $notify;
}

##########################################################################
# Get the user reserving this host
##
sub getUser {
  my ($self) = assertNumArgs(1, @_);
  return $self->{user};
}

##########################################################################
# Set the user reserving this host
##
sub setUser {
  my ($self, $user) = assertNumArgs(2, @_);
  $self->{user} = $user;
}

##########################################################################
# Get the next user for this host
##
sub getNextUser {
  my ($self) = assertNumArgs(1, @_);
  return $self->{nextUser};
}

##########################################################################
# Set the next user for this host
##
sub setNextUser {
  my ($self, $nextUser) = assertNumArgs(2, @_);
  $self->{nextUser} = $nextUser;
}

##########################################################################
# Get the expiry time for this host for the next user
##
sub getNextExpiryTime {
  my ($self) = assertNumArgs(1, @_);
  return $self->{nextExpiry};
}

##########################################################################
# Set the expiry time for this host for the next user
##
sub setNextExpiryTime {
  my ($self, $expiry) = assertNumArgs(2, @_);
  $self->{nextExpiry} = $expiry;
}

##########################################################################
# Get the reservation message for this host for the next user
##
sub getNextMessage {
  my ($self) = assertNumArgs(1, @_);
  return $self->{nextMsg};
}

##########################################################################
# Set the reservation message for this host for the next user
##
sub setNextMessage {
  my ($self, $message) = assertNumArgs(2, @_);
  $self->{nextMsg} = $message;
}

##########################################################################
# Get the key for this host
##
sub getKey {
  my ($self) = assertNumArgs(1, @_);
  return $self->{key};
}

##########################################################################
# Set the key for this host
##
sub setKey {
  my ($self, $key) = assertNumArgs(2, @_);
  $self->{key} = $key;
}

##########################################################################
# Add the given class to the list of classes this host belongs to.
##
sub addClass {
  my ($self, $class) = assertNumArgs(2, @_);
  if ($class->isComposite()) {
    croak("$self added to composite class $class");
  }
  my $contains = grep {$_->equals($class)} @{$self->{classes}};
  if (!$contains) {
    my @classes = @{$self->{classes}};
    push(@classes, $class);
    @classes = sort {$a->compareTo($b)} @classes;
    $self->{classes} = \@classes;
  }
}

##########################################################################
# Remove the given class from the list of classes this host belongs to.
##
sub removeClass {
  my ($self, $class) = assertNumArgs(2, @_);
  @{$self->{classes}} = grep {!$_->equals($class)} @{$self->{classes}};
}

##########################################################################
# Return a list of the Classes to which this Host belongs.
##
sub getClasses {
  my ($self) = assertNumArgs(1, @_);
  return @{$self->{classes}};
}

##########################################################################
# Compare this Host with the specified host for order.  Currently this
# will sort foo-XXX type machines by number and all others by name.
# Hosts that are members of the DEFAULT_RESERVE_CLASS are sorted after
# comparable hosts that are not.
##
sub compareTo {
  my ($self, $other) = assertNumArgs(2, @_);

  # Return servers in DEFAULT_RESERVE_CLASS last
  my $selfReserveDefault = $self->_inClass($DEFAULT_RESERVE_CLASS);
  my $otherReserveDefault = $other->_inClass($DEFAULT_RESERVE_CLASS);
  if ($selfReserveDefault != $otherReserveDefault) {
    return $selfReserveDefault ? 1 : -1;
  }

  # For numbered machines, sort by number
  my ($selfPrefix, $selfNumber) = $self->{hostname} =~ /^(\w+)-(\d+)$/;
  my ($otherPrefix, $otherNumber) = $other->{hostname} =~ /^(\w+)-(\d+)$/;
  if ($selfPrefix && $otherPrefix && ($selfPrefix eq $otherPrefix)) {
    return $selfNumber <=> $otherNumber;
  }

  # Sort by name
  return ($self->{hostname} cmp $other->{hostname});
}

##########################################################################
# Return whether a host is in a specified class
#
# @param    self    A host object
# @param    want    The desired class
#
# @return   1 if the host is a member of class "want" else 0
##
sub _inClass {
  my ($self, $want) = assertNumArgs(2, @_);
  foreach my $class (@{$self->{classes}}) {
    if ("$class" eq "$want") {
      return 1;
    }
  }
  return 0;
}

##########################################################################
# Return a string description of this Host, for debugging only
##
sub toString {
  my ($self) = assertNumArgs(1, @_);
  my $string = $self->{hostname};
  $string .= ",";
  $string .= $self->{user} || '';
  $string .= ",";
  $string .= $self->{expiry} || 0;
  $string .= ",";
  $string .= $self->{msg} || '';
  $string .= ",";
  $string .= $self->{nextUser} || '';
  $string .= ",";
  $string .= $self->{nextExpiry} || 0;
  $string .= ",";
  $string .= $self->{nextMsg} || '';
  $string .= ",";
  $string .= $self->{oldUser} || '';
  $string .= ",";
  $string .= $self->{oldExpiry} || 0;
  $string .= ",";
  $string .= $self->{nextNotify} || 0;
  $string .= ",";
  $string .= $self->{oldMsg} || '';
  $string .= ",";
  $string .= $self->{key} || '';
  my @classes = map {$_->{name}} @{$self->{classes}};
  $string .= ",(" . join(',', @classes);
  $string .= ")";
  return $string;
}

######################################################################
# Return an arrayref representation of this Host.  This is the current
# wire representation used for RSVPServer::listHosts().  If
# listClasses is not specified, the list contains the hostname,
# reserving user, reservation expiration time, and reservation
# message.  Otherwise, it is the hostname, reserving user, and the
# list of classes to which the host belongs.
#
# @param listClasses  If class membership should be listed
# @param listNext     If next user information should be listed
#
# @return An arrayref representation of this Host.
##
sub toArray {
  my ($self, $listClasses, $listNext) = assertNumArgs(3, @_);
  if ($listClasses) {
    my $classes = join(", ", map {$_->getName()} @{$self->{classes}});
    return [$self->{hostname}, $self->{user}, $classes];
  } elsif ($listNext) {
    return [$self->{hostname}, $self->{user}, $self->{nextUser},
            $self->{nextExpiry}, $self->{nextMsg}];
  } else {
    return [$self->{hostname}, $self->{user}, $self->{expiry},
            $self->{msg}];
  }
}

######################################################################
# Stringification operator returns just the hostname, to clear up host
# vs hostname confusion.
#
# @return A string representation of this Host.
##
sub as_string {
  my $self = shift;
  return $self->{hostname};
}

1;
