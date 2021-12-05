##
# Module to represent a response from the rsvp daemon.
#
# @synopsis
#
#     use Response;
#     $response = Response->error("broken!");
#     $response = Response->error("try again!", 1);
#     $response = Response->success("good job");
#     $response = Response->success("good job", \@data);
#
# @description
#
# C<Response> is a data structure containing the response to a request
# to the rsvp daemon.
#
# $Id: //eng/main/src/tools/rsvp/rsvpd/Response.pm#3 $
##
package RSVPD::Response;

use strict;
use warnings FATAL => qw(all);
use English;
use Carp;
use Log::Log4perl;
use Permabit::Assertions qw(assertMinMaxArgs assertNumArgs);
use Storable qw(dclone);

use base qw(Exporter);

# Overload stringification to print the message
use overload q("") => \&as_string;

our @EXPORT = qw(error success);
our $VERSION = 1.1;

my $log = Log::Log4perl->get_logger(__PACKAGE__);

##
# @paramList{new}
my %properties
  = (
     # @ple The data embodied in this response, if any
     data              => undef,
     # @ple The message of this response
     message           => "",
     # @ple If this is an error response, is it temporary?
     temporary         => 0,
     # @ple The type of this response ('success' or 'ERROR')
     type              => undef,
    );
##

##########################################################################
# Create a new Response
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
  if (!$self->{type}) {
    croak("Must set type in new Response objects");
  }
  return $self;
}

##########################################################################
# Create a new Error response
#
# @param message        The error message
# @oparam temporary     This is a temporary error condition,
#                       command may be retried.
##
sub error {
  my ($message, $temporary) = assertMinMaxArgs(1, 2, @_);
  $temporary ||= 0;
  return RSVPD::Response->new(type      => "ERROR",
                              message   => $message,
                              temporary => $temporary);
}

##########################################################################
# Create a new Success response
#
# @param message        The success message
# @oparam data          Any associated data
##
sub success {
  my ($message, $data) = assertMinMaxArgs(1, 2, @_);
  return RSVPD::Response->new(type     => "success",
                              message  => $message,
                              data     => $data);
}

##########################################################################
# Returns whether or not this is an Error response.
##
sub isError {
  assertNumArgs(1, @_);
  my ($self) = @_;
  return ($self->{type} eq "ERROR");
}

##########################################################################
# Returns whether or not this is a Success response.
##
sub isSuccess {
  assertNumArgs(1, @_);
  my ($self) = @_;
  return ($self->{type} eq "success");
}

##########################################################################
# Returns the message for this response.
##
sub getMessage {
  assertNumArgs(1, @_);
  my ($self) = @_;
  return $self->{message};
}

##########################################################################
# Return whether or not there is data in this response
##
sub hasData {
  assertNumArgs(1, @_);
  my ($self) = @_;
  return $self->{data};
}

######################################################################
# Return an hashref representation of this Response.
#
# @return An arrayref representation of this Response.
##
sub encode {
  assertNumArgs(1, @_);
  my ($self) = @_;
  return { type      => $self->{type},
           message   => $self->{message},
           data      => $self->{data},
           temporary => $self->{temporary},
         };
}

######################################################################
# Stringification operator returns just the message for logging
# purposes.
#
# @return A string representation of this Response.
##
sub as_string {
  my $self = shift;
  return "$self->{type}: $self->{message}";
}

1;
