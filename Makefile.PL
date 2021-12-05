use ExtUtils::MakeMaker qw(WriteMakefile);

WriteMakefile(
  NAME         => 'rsvpd',
  MIN_PERL_VERSION => 5.006,
  META_MERGE => {
    license => ["gpl_2"],
  },
  EXE_FILES => [ "bin/rsvpd.pl",
                 "bin/rsvpclient.pl", ],
  BUILD_REQUIRES => { "ExtUtils::MakeMaker" => 6.48 },
  VERSION_FROM => "lib/RSVPD/RSVPServer.pm",
  clean        => { FILES => '*.bak *.old mibs/*.dump lib/*/*~' },
);
