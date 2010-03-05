#!/usr/bin/env perl

my $script_dir;
BEGIN {
  my $rel_script_dir = `dirname $0`;
  chomp($rel_script_dir);
  chomp($script_dir = `(cd $rel_script_dir; pwd)`);
  unshift(@INC, $script_dir);
}

use strict;
use VGXP;
use POSIX ":sys_wait_h";
use Digest::MD5;
use warnings;
use IPC::Open3;
use Symbol;
use Getopt::Std;
use File::Basename;

my $pmaster_pid;
sub handle_sigint{
  kill 9, $pmaster_pid if $pmaster_pid;
}
$SIG{INT} = &handle_sigint;

#my $script_dir = File::Basename::dirname $0;
#die "Can't cd to $script_dir: $!\n" unless chdir $script_dir;

sub subst_variable($$){
  my ($vars, $varname) = @_;
  if(defined $ENV{$varname}){
    sv($vars, $ENV{$varname});
  } elsif(defined $vars->{$varname}){
    sv($vars, $vars->{$varname});
  } else {
    print STDERR "Definition of $varname is not found!\n";
    "";
  }
}
sub sv($$){
  my ($vars, $str) = @_;
  $str =~ s/%([\w\d_]+)%/subst_variable($vars, $1)/eg;
  $str;
}

sub copy_or_apply_template($@){
  my ($vars, $apply_template, $tmpl, $target) = @_;
  open(my $rfh, $tmpl) || die("Cannot open $tmpl for read");
  local $/;
  my $content = <$rfh>;
  $content = sv($vars, $content) if $apply_template;
  close $rfh;
  my $content2 = "";
  open($rfh, $target) && do {
    $content2 = <$rfh>;
    close $rfh;
  };
  if($content eq $content2){
    printf STDERR "IGNORE $target will not be modified.\n";
  } else {
    printf STDERR "%s %s %s\n", ($apply_template ? "APPLY_TEMPLATE" : "COPY"), $tmpl, $target;
    my $tmpfile = "$target.$$";
    open(my $wfh, ">", $tmpfile) || die("Cannot open $tmpfile for write");
    print $wfh $content;
    close $wfh;
    rename $tmpfile, $target;
  }
}

sub read_vars($$){
  my ($vars, $file) = @_;
  my $rfh;
  open $rfh, $file;
  while(<$rfh>){
    chomp;
    if(/^\s*([\w\d_]+)\s*=\s*(\S.*)/){
      $vars->{$1} = sv($vars, $2);
    }
  }
  close $rfh;
}

my $vars = {};
my $hostname = `hostname -f`;
chomp $hostname;
$vars->{HOSTNAME} = $hostname;
$vars->{SCRIPT_DIR} = $script_dir;
read_vars($vars, "$script_dir/vgxpvars.txt");

## Parse args
my %opts = ();
getopts("d:w:m:i:h", \%opts);
if($opts{h}){
  print STDERR << "EEE";
$0 [-d dir] [-w URL] [-m filename] [-i interval] [-h]
  -d: install dir
  -w: base URL
  -m: filename of master program
  -i: check interval (in seconds)
EEE
  exit 1;
}
$vars->{INSTALL_DIR} = $opts{d} if $opts{d};
$vars->{CODEBASE} = $opts{w} if $opts{w};
$vars->{MASTER_PATH} = $opts{m} if $opts{m};
$vars->{PMASTER_CHECK_INTERVAL} = $opts{i} if $opts{i};

##

my @remote_files = qw(CM.pm SSHSocket.pm agent.pl);
my @remote_files2 = qw(python pinfo.py);
my $portconf = sprintf "%s/%s", sv($vars, '%INSTALL_DIR%'), sv($vars, '%PORTCONF_FILENAME%');
my $pmaster = sv($vars, '%MASTER_PATH%');
my $check_interval = sv($vars, '%PMASTER_CHECK_INTERVAL%');
my $fname_notify = sv($vars, '%NOTIFY_FILENAME%');
my $install_dir = sv($vars, '%INSTALL_DIR%');

##

# Copy files

sub copy_files($){
  my ($vars) = @_;
  open(my $fh, "$script_dir/copyfiles.txt");
  while(<$fh>){
    next if /^\s*\#/;
    chomp;
    my @a = split;
    $a[1] = sv($vars, $a[1]);
    $a[2] = sv($vars, $a[2]);
    if(-d $a[2]){
      my $basename = File::Basename::basename($a[1]);
      $a[2] .= "/$basename";
    }
    copy_or_apply_template($vars, @a);
  }
}

sub my_mkdir($){
  my ($dir) = @_;
  my $parent = File::Basename::dirname $dir;
  die "$parent not found" if ! -d $parent;
  if(! -d $dir){
    print "MKDIR Creating directory $dir\n";
    system("mkdir $dir");
    die("Cannot make directory") if ! -d $dir;
  }
}

my_mkdir $install_dir;
copy_files($vars);
system(sv($vars, "chmod +x $install_dir/%PROXYCGI_FILENAME%"));

# Launch VGXP

$ENV{PERLLIB} = $script_dir;
my $log_dir = sv($vars, '%LOG_DIR%');
my_mkdir $log_dir;

while(1){
  &VGXP::gxp_check();
  #system("gxpc quit --session_only");
  my @cmd = ('gxpc', 'mw', #'-h', 'hongo-', #'(hongo-|tohoku|hiro-|kototoi-|kyoto-)',
             '--master', "$pmaster $portconf $log_dir", 'perl');#"$agent");
  my ($cmd_in, $cmd_out, $cmd_err) = (gensym, gensym, gensym);
  my $open3pid = open3($cmd_in, $cmd_out, $cmd_err, @cmd);
  #print STDERR "Open3: pid = $open3pid\n";
  for my $fname (@remote_files){
    open my $fh, "<", "$script_dir/$fname";
    while(<$fh>){
      print $cmd_in $_;
    }
    close $fh;
  }
  print $cmd_in "__DATA__\n", $remote_files2[0], $/;
  open my $fh, "<", sprintf("%s/%s", $script_dir, $remote_files2[1]);
  while(<$fh>){
    print $cmd_in $_;
  }
  close $fh;
  close $cmd_in;
  my $mw_err = <$cmd_err>;
  close $cmd_out;
  close $cmd_err;
  $mw_err =~ s/^(\d+)$/$pmaster_pid = $1;""/egm;
  $mw_err =~ s/^(.*)$/print STDERR "ERR: $1\n" if length($1)/egm;
  wait;
  while(1){
    sleep($check_interval);
    if(-f $fname_notify){
      unlink $fname_notify;
      print STDERR scalar(localtime), " Notify\n";
      system("pkill $pmaster");
      kill "INT", $pmaster_pid;
    }elsif(kill(0,$pmaster_pid)){
      next;
    }else{
      print STDERR "Dead child ", scalar(localtime), $/;
    }
    $pmaster_pid = 0;
    sleep(3);
    last;
  }
}
