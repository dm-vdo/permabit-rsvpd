# Disable auto-generation of debug_package
%global debug_package %{nil}

Name:           perl-rsvpd
Version:        0.2
Release:        4%{?dist}
Summary:        Permabit machine reservation system
License:        GPL+ or Artistic
URL:            https://gitlab.cee.redhat.com/vdo/open-sourcing/tools/permabit-rsvpd
Source0:        %{url}/-/archive/main/permabit-rsvpd-main.tar.gz
# Module Build
BuildRequires:  coreutils
BuildRequires:  findutils
BuildRequires:  make
BuildRequires:  perl-generators
BuildRequires:  perl-interpreter
BuildRequires:  perl(ExtUtils::MakeMaker)
# Module Runtime
BuildRequires:  perl(Carp)
%if ! 0%{?rhel} && ! 0%{?eln}
BuildRequires:  perl(Clone) >= 0.43
%endif
BuildRequires:  perl(Cwd)
BuildRequires:  perl(Exporter)
BuildRequires:  perl(File::Find::Rule)
BuildRequires:  perl(Scalar::Util)
BuildRequires:  perl(strict)
BuildRequires:  perl(vars)
BuildRequires:  perl(warnings)
# Test Suite
BuildRequires:  perl(Config)
BuildRequires:  perl(Data::Dumper)
BuildRequires:  perl(IPC::Run3) >= 0.048
%if ! 0%{?rhel} && ! 0%{?eln}
BuildRequires:  perl(Test::Deep) >= 1.130
%endif
BuildRequires:  perl(Test::Exception) >= 0.43
BuildRequires:  perl(Test::More) >= 0.88
%if ! 0%{?rhel} && ! 0%{?eln}
BuildRequires:  perl(Test::Warnings) >= 0.030
%endif
BuildRequires:  perl(constant)
# Optional Tests
BuildRequires:  perl(JSON)
%if ! 0%{?rhel} && ! 0%{?eln}
BuildRequires:  perl(Scalar::Properties)
%endif
BuildRequires:  perl(Test::Pod) >= 1.00
# Dependencies
BuildRequires:  systemd
Requires:       perl(:MODULE_COMPAT_%(eval "`perl -V:version`"; echo $version))
Requires(pre):  shadow-utils
%{?systemd_requires}

%description
RSVP Server Side to the Permabit machine reservation system.

%prep
%setup -n permabit-rsvpd-main

%build
perl Makefile.PL INSTALLDIRS=vendor
make %{?_smp_mflags}

%pre
getent group rsvp >/dev/null || groupadd -r rsvp
getent passwd rsvp >/dev/null || \
  useradd -r -g rsvp -s /sbin/nologin -c "RSVPD Daemon user account" rsvp
exit 0

%post
%systemd_post rsvpd.service

%preun
%systemd_preun rsvpd.service

%postun
%systemd_postun_with_restart rsvpd.service

%install
make pure_install DESTDIR=%{buildroot}
find %{buildroot} -type f -name .packlist -delete
%{_fixperms} -c %{buildroot}

%{__ln_s} rsvpd.pl $RPM_BUILD_ROOT/%{_bindir}/rsvpd
%{__ln_s} rsvpclient.pl $RPM_BUILD_ROOT/%{_bindir}/rsvpclient

%{__install} -d $RPM_BUILD_ROOT/%{_sysconfdir}/rsvpd
%{__install} -d $RPM_BUILD_ROOT/%{_unitdir}/rsvpd
%{__install} -d $RPM_BUILD_ROOT/%{_presetdir}/rsvpd
%{__install} -m 0644 log.conf $RPM_BUILD_ROOT/%{_sysconfdir}/rsvpd
%{__install} -m 0644 rsvpd.conf $RPM_BUILD_ROOT/%{_sysconfdir}/rsvpd
%{__install} -m 0644 rsvpd.service $RPM_BUILD_ROOT/%{_unitdir}
%{__install} -m 0644 01-rsvpd.preset $RPM_BUILD_ROOT/%{_presetdir}
%{__install} -m 0755 -d $RPM_BUILD_ROOT/var/log/rsvpd
%{__install} -m 0755 -d $RPM_BUILD_ROOT/var/lib/rsvpd

%check
make test

%files
#defattr(-,root,root)
%dir %{perl_vendorlib}/RSVPD
%{perl_vendorlib}/RSVPD/Class.pm
%{perl_vendorlib}/RSVPD/Host.pm
%{perl_vendorlib}/RSVPD/Response.pm
%{perl_vendorlib}/RSVPD/RSVPServer.pm
%{_bindir}/rsvpd.pl
%{_bindir}/rsvpd
%{_mandir}/man1/rsvpd.pl.1.gz
%{_sysconfdir}/rsvpd/log.conf
%{_sysconfdir}/rsvpd/rsvpd.conf
%{_unitdir}/rsvpd.service
%{_presetdir}/01-rsvpd.preset
%dir %attr(0755, rsvp, rsvp) /var/log/rsvpd
%dir %attr(0755, rsvp, rsvp) /var/lib/rsvpd

%package -n perl-rsvpclient
Summary: Client utility for perl-rsvpd

%description -n perl-rsvpclient
This is the client utility to interact with perl-rsvpd, the Permabit machine
reservation system.

%files -n perl-rsvpclient
%{_bindir}/rsvpclient.pl
%{_bindir}/rsvpclient

%changelog
* Sat Apr 16 2022 Andy Walsh <awalsh@redhat.com> - 0.2-4
- Actually made preset take effect

* Sat Apr 02 2022 Andy Walsh <awalsh@redhat.com> - 0.2-3
- Fixed preset name to enable the right service

* Thu Mar 17 2022 Andy Walsh <awalsh@redhat.com> - 0.2-2
- Updated sources

* Sun Dec 05 2021 Andy Walsh <awalsh@redhat.com> - 0.2-1
- Fixed up Response issue

* Sun Dec 05 2021 Andy Walsh <awalsh@redhat.com> - 0.1-1
- Initial build
