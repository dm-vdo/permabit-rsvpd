##
# Module to represent a class as handled by the rsvp daemon.
#
# @synopsis
#
#     use Class;
#     $farms = Class->new(name => "farms");
#     $oneDisk = Class->new(name => "one-disk");
#     $farms->addMember($oneDisk);
#
# @description
#
# C<Class> is effectively a data structure containing the information
# regarding a class of machines known to the rsvp daemon, along with
# methods for accessing and manipulating that data.
#
# $Id: //eng/main/src/tools/rsvp/rsvpd/Class.pm#6 $
##
package RSVPD::Class;

use strict;
use English;
use Carp;
use Log::Log4perl;
use Permabit::Assertions qw(assertNumArgs assertNumDefinedArgs);
use Storable qw(dclone);

my $log = Log::Log4perl->get_logger(__PACKAGE__);

# Overload stringification to print the hostname
use overload q("") => \&as_string;

##
# @paramList{new}
my %properties
  = (
     # @ple A listref of other Classes that are members of this Class
     members            => [],
     # @ple The name of this class
     name               => undef,
     # @ple Whether this class represents an unpingable resource
     resource           => 0,
     # @ple Description of this class
     description        => "",
    );
##

##########################################################################
# Create a new Class
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
  if (!$self->{name}) {
    croak("Must set name in new Class objects");
  }
  return $self;
}

##########################################################################
# Get the name of this class.
##
sub getName {
  assertNumArgs(1, @_);
  my ($self) = @_;
  return $self->{name};
}

##########################################################################
# Add a member class to this class.
##
sub addMember {
  assertNumDefinedArgs(2, @_);
  my ($self, $other) = @_;
  if (!$self->containsMember($other)) {
    push(@{$self->{members}}, $other);
  }
}

##########################################################################
# Remove a member class from this class.
##
sub removeMember {
  assertNumDefinedArgs(2, @_);
  my ($self, $other) = @_;
  @{$self->{members}} = grep {!$_->equals($other)} @{$self->{members}};
}

##########################################################################
# Return the number of members of a composite.
##
sub numMembers {
  my ($self) = assertNumArgs(1, @_);
  return scalar(@{$self->{members}});
}

##########################################################################
# Check if this class is composite.
##
sub isComposite {
  my ($self) = assertNumArgs(1, @_);
  return $self->numMembers();
}

##########################################################################
# Returns true if this class is a type of unpingable resource.
##
sub isResource {
  assertNumArgs(1, @_);
  my ($self) = @_;
  return $self->{resource};
}

##########################################################################
# Check whether or not the given Host belongs to this class.
#
# @param host   The Host in question
#
# @return 1 if the Host belongs to this class, 0 otherwise.
##
sub containsHost {
  assertNumArgs(2, @_);
  my ($self, $host) = @_;
  my $contains = grep {$_->equals($self)} $host->getClasses();
  if ($contains) {
    return 1;
  }
  if ($self->isComposite()) {
    foreach my $member (@{$self->{members}}) {
      if (!$member->containsHost($host)) {
        return 0;
      }
    }
    return 1;
  }
}

##########################################################################
# Check whether or not the given Class is a member of this class.
#
# @param other   The Class in question
#
# @return 1 if the class is a member of this class, 0 otherwise.
##
sub containsMember {
  assertNumArgs(2, @_);
  my ($self, $other) = @_;
  if ($self->equals($other)) {
    return 1;
  }
  if ($self->isComposite()) {
    foreach my $member (@{$self->{members}}) {
      if ($member->equals($other)) {
        return 1;
      }
    }
  }
  return 0;
}

##########################################################################
# Compare two Classes for equality.
##
sub equals {
  assertNumArgs(2, @_);
  my ($self, $other) = @_;
  return ($self->{name} eq $other->{name});
}


##########################################################################
# Compare this Class with the specified class for order. Classes are
# sorted first by member count, and secondly by class name.
##
sub compareTo {
  assertNumArgs(2, @_);
  my ($self, $other) = @_;
  my $selfMemberCnt = scalar(@{$self->{members}});
  my $otherMemberCnt = scalar(@{$other->{members}});
  if ($selfMemberCnt == $otherMemberCnt) {
    return ($self->{name} cmp $other->{name});
  }
  return $selfMemberCnt <=> $otherMemberCnt;
}

######################################################################
# Return an arrayref representation of this Class.  This is the current
# wire representation used for RSVPServer::listClasses().
#
# @return An arrayref representation of this Class.
##
sub toArray {
  assertNumArgs(1, @_);
  my ($self) = @_;
  my @memberNames = map {$_->{name}} @{$self->{members}};

  my $description = $self->{description} || " ";
  my @array = ($self->{name}, $description, $self->{resource}, @memberNames);
  return \@array;
}

##########################################################################
# Return a string description of this Class, for debugging only
##
sub toString {
  assertNumArgs(1, @_);
  my ($self) = @_;
  my $string = $self->{name};
  if ($self->isComposite()) {
    my @memberNames = map {$_->{name}} @{$self->{members}};
    $string .= " -> " . join(',', @memberNames);
  }
  return $string;
}

######################################################################
# Stringification operator returns just the name, to clear up class vs
# classname confusion.
#
# @return A string representation of this Class
##
sub as_string {
  my $self = shift;
  return $self->{name};
}

1;
