#!/usr/bin/perl

##
# Client for the machine reservation system
#
# @synopsis
#
# rsvpclient [--help] [--version] [--port PORT] [--dhost HOST] [--user USER]
#            add HOSTNAME [HOSTNAME...] [--classes CLASSES]
#            del HOSTNAME [HOSTNAME...]
#            list [--csv] [--verbose] [--class CLASS] [--mine] [--next] [--free]
#               [HOST_REGEXP]
#            modify HOSTNAME [HOSTNAME...] [--add CLASSES] [--del CLASSES]
#            release HOSTNAME [HOSTNAME...] [--msg MESSAGE] [--key KEY]
#               [--force]
#            renew HOSTNAME [HOSTNAME...] [--dur[ation] DURATION]
#                [--msg MESSAGE]
#            reserve HOSTNAME [HOSTNAME...] [--wait] [--dur[ation] DURATION]
#                [--msg MESSAGE] [--key KEY] [--resource]
#            reserve NUMHOSTS [--wait] [--dur[ation] DURATION] [--msg MESSAGE]
#                [--class CLASS] [--randomize] [--key KEY]
#            verify HOSTNAME [HOSTNAME...]
#
#            add_next_user HOSTNAME [HOSTNAME...] [--dur[ation] DURATION]
#                [--msg MESSAGE]
#            del_next_user HOSTNAME [HOSTNAME...]
#
#            add_class CLASS [--members CLASSES] [--msg DESCRIPTION]
#            del_class CLASS
#            list_classes [--csv]
#
#            list_os_classes
#
#            add_resource_class CLASS [--msg DESCRIPTION]
#            del_resource_class CLASS
#
#            add_resource RESOURCE [RESOURCE...] --class CLASS
#            del_resource RESOURCE [RESOURCE...]
#            reserve_resources NUMRESOURCES --class CLASS [--wait]
#                [--dur[ation] DURATION] [--msg MESSAGE] [--key KEY]
#            release_resource RESOURCE [RESOURCE...] [--msg MESSAGE]
#                [--key KEY] [--force]
#
#            move_to_maintenance HOSTNAME [--msg MESSAGE] [--force]
#                [--assignee USER]
#
# @description
#
# rsvpclient is the client for the machine reservation system.  It can
# be used to query reservations, reserve machines, manage
# reservations, and add/delete machines from the system.
#
# @level{+}
#
# @item ARGUMENTS
#
# @level{+}
#
# @item --help
#
# Displays this message and program version and exits.
#
# @item --version
#
# Displays program version and exits.
#
# @item --port PORTNUMBER
#
# Specifies the port number to use.
#
# @item --dhost HOSTNAME
#
# Specifies the host name of the reservation daemon.  If not specified, the
# value of the PRSVP_HOST environment variable is used.
#
# @item --user USER
#
# Specifies to act on behalf of the given user when talking to the
# daemon.  If none is specified, defaults to the current user.
#
# @level{-}
#
# @item USER_COMMANDS
#
# @level{+}
#
# @item list
#
# Displays a list of all machines in the system and their
# reservations.  If --csv is specified on the command-line, the output
# is as a list of comma-separated values.  If --class is specified,
# only hosts in the given class will be listed.  Otherwise, if --user
# is specified, only hosts reserved by the given user will be listed.
# The --mine option is a shortcut for --user=<current user>.  If
# --free is provided, only unreserved hosts will be listed. If
# --verbose is specified, the classes to which each host belongs will
# be specified. If --next is specified, the next user information is
# displayed.  An optional regexp (perl syntax) can be provided, which
# will be used to filter the machines listed.
#
# The --class option may be a single class name or a comma-separated
# list of class names. If the list is given, only hosts that belong to
# all the listed classes will be shown.
#
# @item list_classes
#
# Displays a list of all classes in the system, their description, and
# the classes that compose them.  If --csv is specified on the
# command-line, the output is as a list of comma-separated values.
#
# @item list_os_classes
#
# Display the list of classes that will over-ride the "reserve this O/S"
# behavior described in "reserve."
#
# @item reserve
# @item rsvp
#
# Reserves one or more machines.  When a hostname or list of hostnames
# is specified, an attempt is made to reserve those particular
# machines.  If a number is given instead of a hostname, an attempt is
# made to reserve that many machines.  The --class option may be used,
# if reserving by number, to specify the class to reserve the hosts
# from. When reserving multiple machines, the list of reserved
# machines is returned when successful.  If a requested machine is
# unavailable or insufficiently many free machines are available, an
# error message is printed. If a hostname is not given and --class does
# not include one of the O/S classes and rsvpclient is running on a
# Debian Linux system, the O/S of the host(s) reserved will match the
# O/S of the machine on which rsvpclient is run.
#
# The --class option may be a single class name or a comma-separated
# list of class names. If the list is given, only hosts that belong to
# all the listed classes will be reserved.
#
# The optional --wait flag specifies that the client should retry the
# reservation attempt periodically if it fails due to a temporary
# error (eg, there are not enough machines available).
#
# The optional --randomize flag works along with --class and
# specifies that the server should not try to return the highest 
# number host in the given --class, but instead return a random host 
# from the available hosts in the given --class.
#
# The optional --msg argument specifies a message to be added to the
# status listing.  This message may be any text string, but it is
# encouraged to be a useful string stating the purpose of the
# reservation, any special conditions of the reservation, etc.
#
# The optional --dur or --duration argument specifies how long the
# desired reservation is for, starting from the current time.  The
# duration is in the form B<x>dB<y>hB<z>mB<w>s, where x,y,z, and w
# represent the days, hours, minutes, and seconds of time,
# respectively.  For example, "1d", "1d2s", "2h30m", and "1m30s" are
# valid duration specifiers.  If no duration is specified, the
# reservation is for one day.  After the given period, the user will
# begin to receive warnings via email and chat message from the reservation
# server asking him to release the machine or renew the reservation.
#
# The optional --key argument provides a weak form of security to
# ensure that the session that reserved a machine is the same one that
# releases it.
#
# The optional --resource argument allows reserving resources by name.
#
# Examples:
#
# @level{+}
#
# @item rsvpclient reserve 1
#
# Reserve a single machine for one day.  The name of the reserved
# machine is returned.
#
# @item rsvpclient reserve 2 --class FARM,SMP --wait
#
# Reserve two SMP machines from class FARM for one day.  The names of the
# reserved machines are returned.  If 2 hosts in class FARM are not
# available, this will retry until they become available.
#
# @item rsvpclient reserve --duration=2h --key mInE 1
#
# Reserve a single machine for two hours.  The name of the reserved
# machine is returned.
#
# @item rsvpclient reserve foo
#
# Reserve the machine foo for an indefinite period of time.
#
# @item rsvpclient reserve --duration 3d --msg='my demo' foo bar
#
# Reserve the machines foo and bar for three days with the message, "demo."
#
# @level{-}
#
# NOTE: The "rsvp" command is an alias for "reserve."
#
# @item renew
#
# Renews a reservation for a given duration and host or list of hosts.
# If no duration is given, the reservation is extended for one day.
# The duration is specified as a time from the current time.
#
# @item rel
#
# @item release
#
# Releases a reservation for a given host or list of hosts.  If the
# hostname is given as "all," all reservations held by the user are
# released.  If the optional --msg parameter is provided, the message
# will be passed to the reservation deamon for logging, to help match
# releases with the corresponding reservation.  If the machine was
# reserved with a --key flag, that same key must provided here, or the
# --force flag can be provided to ignore the key.  Please use --force
# sparingly, since it makes keys useless.
#
# @item verify
#
# Verifies a reservation for a given host or lists of hosts.  Prints
# "verified HOST" if a reservation is held on the given machine.
#
# @item release_resource
#
# Releases a reservation for a given resource or list of resources.
# If the optional --msg parameter is provided, the message will be
# passed to the reservation deamon for logging, to help match releases
# with the corresponding reservation. If the resource was reserved
# with a --key flag, that same key must provided here, or the --force
# flag can be provided to ignore the key.  Please use --force
# sparingly, since it makes keys useless.
#
# @item reserve_resources
#
# Reserves the requested number of resources from the specified
# resource class.  The --class parameter is required and must specify
# an existing resource class. When reserving multiple resources, the
# list of reserved resources is returned when successful.  The --key
# parameter specifies a key that must be provided upon release.  If
# not enough resources are available, an error message is printed.
#
# @item add_next_user
#
# A next user is designated to get the reservation on one or more
# specified machines after the current reservation is released. If the
# reservation is renewed by the current user, or if it is not released
# (after the reservation expires), the next user waits until the
# machine is free to be reserved.
#
# @item del_next_user
#
# Remove the pending next user reservation from one or more specified
# machines.
#
# @level{-}
#
# @item ADMINISTRATIVE_COMMANDS
#
# @level{+}
#
# @item add
#
# Add the specified hostname or list of hostnames to the list of
# reservable machines.  If the optional --classes option is given, the
# host will be added to those classes.  The classes must exist and
# they must not be composite.
#
# @item add_class
#
# Add the specified class to the list of classes.  If the optional
# --msg parameter is given, it is used to describe the class.  If no
# members are specified via the --members option, the constructed
# class will be a simple "leaf class" that can be referenced by hosts.
# Otherwise, the class will be a composite class consisting of the
# classes specified by the --members option. Any member classes must
# already exist.
#
# @item add_resource
#
# Add the specified resource or list of resources to the list of
# resources.  Resources are treated as if they were hosts, except that
# they will not be pinged or The required --class option must specify
# a resource class to which the resource will belong.
#
# @item add_resource_class
#
# Add the specified class to the list of resource classes.  Resource
# objects may only be added to resource classes.  If the optional
# --msg parameter is given, it is used to describe the class.  A
# resource class can not be composite nor part of a composite class.
#
# @item del
#
# Delete the specified hostname or list of hostnames from the list of
# reservable machines.
#
# @item del_class
#
# Delete the specified class from the list of existing classes.  It
# will be removed from the class membership of any hosts that were in
# it, as well as from the membership of any composite classes that
# included it.
#
# @item del_resource
#
# Delete the specified resource or list of resources from the list of
# existing resources.
#
# @item del_resource_class
#
# Delete the specified resource class from the list of existing
# classes.  Any resources that were part of this class will also be
# removed by this command.
#
# @item modify
#
# Modify the list of classes to which a host or list of hosts belong.
# Any classes listed by add will be added to the class list, and any
# listed by del will be removed.  Classes that the host is already a
# member of will not be duplicated, and classes that the host is not a
# member of will be ignored.
#
# @item move_to_maintenance
#
# Move a single machine to maintenance automatically creating an jira
# issue with the reason specified with --msg.  If the machine was
# reserved, then the owner will be sent an email and chat message.  If the
# --force option is used, then a --msg is not required, the email and
# chat message will not be sent, and the ticket will not be connected to the
# mover.
#
# @level{-}
#
# @item ENVIRONMENT_VARIABLES
#
# @level{+}
#
# @item PRSVP_HOST
#
# The host on which the reservation daemon is running.
#
# @level{-}
#
# @item SEE_ALSO
#
# C<rspvd>
#
# @level{-}
#
# @author
#
# Permabit Tools Team <dev-tools@at@permabit.com>, with code from
# Schedule::Load, by Wilson Snyder <wsnyder@at@wsnyder.org>
#
# $Id: //eng/main/src/perl/bin/rsvpclient.pl#46 $
##

use FindBin;
# Use version of Permabit::Utils in current tree, if it exists, and
# fall back to version built by nightly.  These are in the opposite
# order, since they get "unshift"'d onto the @INC list.
use lib "/permabit/build/perl/lastrun/lib";
use lib "$FindBin::Bin/../lib";

use diagnostics;
use strict;
use warnings FATAL => qw(all);

use English qw(-no_match_vars);
use Getopt::Long;
use Log::Log4perl qw(:easy);
use Pdoc::Generator qw(pdoc2help pdoc2usage);
use Permabit::Assertions qw(assertMinArgs assertNumArgs);
use Permabit::Constants;
use Permabit::RSVP qw(listOsClasses);
use Permabit::Utils qw(getUserName);
use Pod::Text;

# A mapping from command names (from the command line) to the methods
# that should be invoked.
my %COMMANDS = (
                'add'                 => \&addHost,
                'add_resource'        => \&addResource,
                'add_class'           => \&addClass,
                'add_next_user'       => \&addNextUser,
                'add_resource_class'  => \&addResourceClass,
                'del'                 => \&delHost,
                'del_next_user'       => \&delNextUser,
                'del_resource'        => \&delHost,
                'del_class'           => \&delClass,
                'del_resource_class'  => \&delClass,
                'list'                => \&listHosts,
                'list_classes'        => \&listClasses,
                'list_os_classes'     => \&printOsClasses,
                'modify'              => \&modifyHost,
                'move_to_maintenance' => \&moveToMaintenance,
                'rel'                 => \&releaseHost,
                'release'             => \&releaseHost,
                'release_resource'    => \&releaseResource,
                'reserve_resources'   => \&reserveResources,
                'renew'               => \&renewReservation,
                'reserve'             => \&reserveHost,
                'rsvp'                => \&reserveHost,
                'verify'              => \&verifyReservation,
                );

# A map of the command line options that can be specified by the user
# to control commands.
my %options
  = ('addClasses'       => undef,
     'class'            => undef,
     'classes'          => undef,
     'csv'              => 0,
     'delClasses'       => undef,
     'duration'         => 0,
     'force'            => 0,
     'free'             => 0,
     'key'              => undef,
     'message'          => '',
     'members'          => undef,
     'resource'         => undef,
     'next'             => 0,
     'randomize'        => 0,
     'user'             => undef,
     'verbose'          => 0,
     'wait'             => 0,
    );

my $rsvp;

main();

######################################################################
# Add a host to rsvp
##
sub addHost {
  _checkOptions("add", "classes");
  my (@hosts) = assertMinArgs(1, @_);
  foreach my $host (@hosts) {
    $rsvp->addHost(host => $host,
                   classes => $options{classes});
  }
}

######################################################################
# Add a class.
##
sub addClass {
  _checkArgs("add_class", 1, @_);
  _checkOptions("add_class", "message", "members");
  my ($class) = @_;
  $rsvp->addClass(class  => $class,
                  description => $options{message},
                  members => $options{members});
}

######################################################################
# Add a resource to rsvp
##
sub addResource {
  _checkOptions("add_resource", "class");
  my (@resources) = assertMinArgs(1, @_);
  foreach my $resource (@resources) {
    $rsvp->addResource(resource => $resource,
                       class => $options{class});
  }
}

######################################################################
# Add a resource class.
##
sub addResourceClass {
  _checkArgs("add_resource_class", 1, @_);
  _checkOptions("add_resource_class", "message");
  my ($class) = @_;
  $rsvp->addResourceClass(class  => $class,
                          description => $options{message});
}

######################################################################
# Delete a host.
##
sub delHost {
  _checkOptions("del");
  my (@hosts) = assertMinArgs(1, @_);
  foreach my $host (@hosts) {
    $rsvp->delHost(host => $host);
  }
}

######################################################################
# Delete a class.
##
sub delClass {
  _checkArgs("del_class", 1, @_);
  _checkOptions("del_class");
  my ($class) = @_;
  $rsvp->delClass(class => $class);
}

######################################################################
# List hosts.
##
sub listHosts {
  _checkOptions("list", "csv", "class", "free", "next", "verbose");
  my $numArgs = $#_ + 1;
  if ($numArgs > 1) {
    print STDERR "Too many arguments to list: $numArgs\n\n";
    pdoc2usage();
  }
  my ($hostRegexp) = shift;
  my $hostListRef = $rsvp->listHosts(user => $options{user},
                                     class => $options{class},
                                     hostRegexp => $hostRegexp,
                                     next => $options{next},
                                     verbose => $options{verbose});
  my @hostList = @{$hostListRef};
  if (!@hostList) {
    return;
  }
  if (!$options{csv}) {
    my $header;
    my $now = localtime();
    $header = "List created at $now\n";
    if ($options{verbose}) {
      $header .= "hostname             rsvp by    classes\n";
    } elsif ($options{next}) {
      $header .= "hostname        rsvp by      next user               next "
        . "message\n";
    } else {
      $header .= "hostname             rsvp by    until                     "
        . "message\n";
    }
    print $header;
    print "-----------------------------------------------------------------\n";
  }
  foreach my $line (@hostList) {
    my @host = @{$line};
    if ($options{free} && $host[1]) {
      next;
    }
    for (my $i = 0; $i < scalar(@host); $i++) {
      $host[$i] ||= '';
    }
    if ($options{csv}) {
      if ($options{verbose}) {
        # We feel it's more useful in csv mode to have the class list not
        # contain spaces so the data can more readily be used as input
        # to rsvpclient.
        $host[2] = join(',', split(/,\s*/, $host[2]));
      }
      print makeCSV(@host) . "\n";
    } elsif ($options{verbose}) {
      printf("%-20s %-10s %s\n", $host[0], $host[1], $host[2]);
    } elsif ($options{next}) {
      printf("%-15s %-12s %-23s %s\n", $host[0], $host[1], $host[2], $host[4]);
    } else {
      printf("%-20s %-10s %-24s  %s\n",
             $host[0], $host[1], _shorttime($host[2]), $host[3]);
    }
  }
}

######################################################################
# List classes
##
sub listClasses {
  _checkArgs("list_classes", 0, @_);
  _checkOptions("list_classes", "csv", "class");
  my $classListRef = $rsvp->listClasses(class => $options{class});
  my @classList = @{$classListRef};

  if ($options{class}) {
    if ($options{csv}) {
      print makeCSV($classList[0], @{$classList[1]}) . "\n";
    } else {
      print("Hosts: " . join(", ", @{$classList[1]}) . "\n");
    }
  } else {
    if (!$options{csv} && (scalar(@classList) > 0)) {
      print "Name                 Type      Description             Members\n";
      print "-----------------------------------------------------------------"
        . "\n";
    }
    foreach my $line (@classList) {
      my @class = @{$line};
      $class[2] = $class[2] ? "resource" : "class";
      if ($options{csv}) {
        print makeCSV(@class) . "\n";
      } else {
        my ($name, $desc, $type, @hosts) = @class;
        printf("%-20s %-8s  %-23s %s\n", $name, $type, $desc,
               join(", ", @hosts));
      }
    }
  }
}

######################################################################
# Given a list of values, produces a proper comma seperated list.
##
sub makeCSV {
  my (@elements) = assertMinArgs(1, @_);
  my $csvString;
  eval {
    require Text::CSV;
    my $csv = Text::CSV->new(auto_diag => 2);
    $csv->combine(@elements);
    $csvString = $csv->string();
  };
  if ($EVAL_ERROR) {
    # The Text::CSV module isn't installed everywhere so fall back to a
    # more simple implementation (but should still be correct unless
    # people have really funky reservation messages).
    $csvString = join(',', map { /,/ ? qq("$_") : $_ } @elements);
  }
  return $csvString;
}

######################################################################
# Print OS classes
##
sub printOsClasses {
  foreach my $class (listOsClasses()) {
    print "$class\n";
  }
}

######################################################################
# Modify the classes a host is a member of
##
sub modifyHost {
  _checkOptions("modify", "addClasses", "delClasses");
  my (@hosts) = assertMinArgs(1, @_);
  foreach my $host (@hosts) {
    $rsvp->modifyHost(host => $host,
                      addClasses => $options{addClasses},
                      delClasses => $options{delClasses});
  }
}

######################################################################
# Add a next user to a host that is already reserved by some other user
##
sub addNextUser {
  _checkOptions("add_next_user", "duration", "message");
  my (@hosts) = assertMinArgs(1, @_);
  foreach my $host (@hosts) {
    $rsvp->addNextUser(host   => $host,
                       expire => $options{duration},
                       msg    => $options{message});
  }
}

######################################################################
# Delete the next user of a host
##
sub delNextUser {
  _checkOptions("del_next_user");
  my (@hosts) = assertMinArgs(1, @_);
  foreach my $host (@hosts) {
    $rsvp->delNextUser(host   => $host);
  }
}

######################################################################
# Release a list of hosts
##
sub releaseHost {
  _checkOptions("release", "message", "key", "force");
  my (@hosts) = assertMinArgs(1, @_);
  foreach my $host (@hosts) {
    if ($host eq "all") {
      $rsvp->releaseHost(all   => 1,
                         force => $options{force},
                         key   => $options{key},
                         msg   => $options{message});
      last;
    }
    $rsvp->releaseHost(host  => $host,
                       force => $options{force},
                       key   => $options{key},
                       msg   => $options{message});
  }
}

######################################################################
# Release a single resource
##
sub releaseResource {
  _checkOptions("release_resource", "message", "key", "force");
  my (@resources) = assertMinArgs(1, @_);
  foreach my $resource (@resources) {
    $rsvp->releaseResource(resource => $resource,
                           force    => $options{force},
                           key      => $options{key},
                           msg      => $options{message});
  }
}

######################################################################
# Reserve the requested number of resources.
##
sub reserveResources {
  _checkArgs("reserve_resources", 1, @_);
  _checkOptions("reserve_resources", "class", "duration", "message", "wait",
                "key");
  my ($numResources) = @_;
  $rsvp->reserveResources(numresources => $numResources,
                          class        => $options{class},
                          expire       => $options{duration},
                          key          => $options{key},
                          msg          => $options{message},
                          wait         => $options{wait},
                         );
}

######################################################################
# Renew a reservation.
##
sub renewReservation {
  _checkOptions("renew", "duration", "message");
  my (@hosts) = assertMinArgs(1, @_);
  foreach my $host (@hosts) {
    $rsvp->renewReservation(host   => $host,
                            expire => $options{duration},
                            msg    => $options{message},
                           );
  }
}

######################################################################
# Reserve a host.
##
sub reserveHost {
  _checkOptions("reserve", "class", "duration", "message", "wait", "key",
                "randomize", "resource");
  my ($arg) = assertMinArgs(1, @_);

  if ($arg =~ /^\d+$/) {
    _checkArgs("rsvp", 1, @_);
    $rsvp->reserveHosts(numhosts  => $arg,
			class     => $options{class},
                        expire    => $options{duration},
                        key       => $options{key},
                        msg       => $options{message},
                        randomize => $options{randomize},
                        wait      => $options{wait},
                       );
  } else {
    my @hosts = @_;
    foreach my $host (@hosts) {
      $rsvp->reserveHostByName(host     => $host,
                               expire   => $options{duration},
                               key      => $options{key},
                               msg      => $options{message},
                               resource => $options{resource},
                               wait     => $options{wait},
                              );
    }
  }
}

######################################################################
# Verify a list of reserved hosts.
##
sub verifyReservation {
  _checkOptions("verify");
  my (@hosts) = assertMinArgs(1, @_);
  foreach my $host (@hosts) {
    $rsvp->verify(host => $host);
  }
}

######################################################################
# Move a host or hosts into maintenance and create a Jira issue.
##
sub moveToMaintenance {
  my (@hosts) = assertNumArgs(1, @_);

  _checkOptions("move_to_maintenance", "force", "message", "assignee");

 $rsvp->moveToMaintenance(
			   hosts    => \@hosts,
			   force    => $options{force},
			   message  => $options{message},
			   assignee => $options{assignee},

                         );
}

######################################################################
# Print the version of rsvpclient
##
sub version {
  print '$Id: //eng/main/src/perl/bin/rsvpclient.pl#46 $ ' . "\n";
  exit (1);
}

######################################################################
# Verify that there are exactly the given number of arguments all
# of which are defined or print a usage message and exit.
#
# @param        name            The name of the command being run
# @param        expected        The expected number of parameters
# @param        arguments       @at@_, as seen by the method
##
sub _checkArgs {
  my $command = shift;
  my $expected = shift;
  my $actual = $#_ + 1;
  if ($expected != $actual) {
    print STDERR "Wrong number of arguments to $command: $actual\n\n";
    pdoc2usage();
  }
}

######################################################################
# Verify that only allowed command line options have been passed to
# this command.
#
# @param        name            The name of the command being run
# @param        expected        The allowed options for this command
##
sub _checkOptions {
  my ($command, @expected) = assertMinArgs(1, @_);
  # The user option is allowed for all commands
  push(@expected, "user");

  foreach my $key (keys %options) {
    if (grep {$_ eq $key} @expected) {
      # This key is expected
      next;
    }
    if ($options{$key}) {
      print STDERR
        "The --$key option is not allowed for the '$command' command \n\n";
      pdoc2usage();
    }
  }
}

######################################################################
# Produce an English representation of the argument (in time() format)
##
sub _shorttime {
  my ($time) = assertNumArgs(1, @_);
  if (!$time) {
    return "";
  }
  return scalar localtime($time);
}

######################################################################
# Parse and validate a duration time string from the client.
##
sub _parseDuration {
  my ($dur) = assertNumArgs(1, @_);
  if ($dur =~ m|^((\d+)d)?((\d+)h)?((\d+)m)?((\d+)s)?$|) {
    my $day = $2 || 0;
    my $hr  = $4 || 0;
    my $min = $6 || 0;
    my $sec = $8 || 0;
    return (time() + $sec + ($MINUTE * $min) + ($HOUR * $hr) + ($DAY * $day));
  } else {
    die("Invalid duration: $dur\n");
  }
}

######################################################################
# Parse a comma separated list and, if there are any entries, add the
# array ref to the options hash.
##
sub _parseList {
  my ($key, $list) = assertNumArgs(2, @_);

  my @array = split(',', join(',', @{$list}));
  if (@array) {
    $options{$key} = [@array];
  }
}

##############################################################################
# main
##
sub main {
  my %params;
  $params{verbose} = 1;
  my (@classes, @members, @addClasses, @delClasses);
  if (!GetOptions(
                  "add=s"       => \@addClasses,
		  "assignee=s"  => \$options{assignee},
                  "csv!"        => \$options{csv},
                  "class=s"     => \$options{class},
                  "classes=s"   => \@classes,
                  "del=s"       => \@delClasses,
                  "dhost=s"     => sub {shift; $params{dhost} = shift},
                  "duration|dur=s" => \$options{duration},
                  "force!"      => \$options{force},
                  "free!"       => \$options{free},
                  "help!"       => sub { pdoc2help(); },
                  "key=s"       => \$options{key},
                  "members=s"   => \@members,
                  "mine!"       => sub {shift; $options{user} = getUserName()},
                  "msg=s"       => \$options{message},
                  "next!"       => \$options{next},
                  "port=i"      => sub {shift; $params{dport} = shift},
		  "component=s" => \$options{component},
                  "quiet!"      => sub {shift; $params{verbose} = 0},
                  "randomize!"  => \$options{randomize},
                  "resource!"   => \$options{resource},
                  "user=s"      => \$options{user},
                  "verbose!"    => \$options{verbose},
                  "debug"       => \$options{debug},
                  "version!"    => \&version,
                  "wait!"       => \$options{wait},
                 )) {
    pdoc2usage();
  }
  Log::Log4perl->easy_init({
                            file   => 'STDOUT',
                            layout => '%m%n',
                            level  => $options{debug} ? $DEBUG : $INFO,
                           });
  delete($options{debug});

  # Clean up comma separated lists
  _parseList('classes', \@classes);
  _parseList('members', \@members);
  _parseList('addClasses', \@addClasses);
  _parseList('delClasses', \@delClasses);

  if ($options{duration}) {
    $options{duration} = _parseDuration($options{duration});
  }
  if ($options{user}) {
    $params{user} = $options{user};
  }
  # initialize rsvp object
  $rsvp = new Permabit::RSVP(%params);

  if (scalar(@ARGV) == 0) {
    pdoc2usage();
  }
  my $cmd = splice(@ARGV, 0, 1);
  if (!$COMMANDS{$cmd}) {
    die("Unknown command $cmd\n");
  }
  my $command = $COMMANDS{$cmd};
  eval {
    &$command(@ARGV);
  };
  if ($EVAL_ERROR) {
    print STDERR $EVAL_ERROR;
    exit(1);
  }
  exit(0);
}
