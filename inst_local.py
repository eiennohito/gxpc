# Copyright (c) 2008 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2007 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2006 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2005 by Kenjiro Taura. All rights reserved.
#
# THIS MATERIAL IS PROVIDED AS IS, WITH ABSOLUTELY NO WARRANTY 
# EXPRESSED OR IMPLIED.  ANY USE IS AT YOUR OWN RISK.
# 
# Permission is hereby granted to use or copy this program
# for any purpose,  provided the above notices are retained on all 
# copies. Permission to modify the code and to distribute modified
# code is granted, provided the above notices are retained, and
# a notice that the code was modified is included with the above
# copyright notice.
#
import base64,glob,os,random,signal,socket,stat,string
import sys,time,types
import ioman,expectd,opt
"""
This file is script which, when invoked, copies a specified
set of files to a remote node and brings up a specified command
there. Here is how it basically works.

1. When this script is invoked, if a file $GXP_DIR/REMOTE_INSTALLED
   exists, it first tries to run an rsh-like command, to check if the
   remote system already has necessary things installed.

   ($GXP_DIR/REMOTE_INSTALLED does not exist in the gxp3 installation
   directory. It is created by the remote installation process, so
   the above check means we never use gxp3 installation directory that
   happens to be the same path in the remote host).

2. When it does, we are done.

3. Otherwise we go ahead and install the things. Here is how.

4. We run an rsh-like command to run a python (bootstrapping)
   script that installs things. This bootstrapping script is
   created locally and fed into the python's standard input
   (i.e., we run 'python -' and sends the script to its stdin).

"""
#
# List of space-separated strings that are used as python interpreters
# I never exprienced other than this.
#
default_python = "python"

#
# Overall, we first try to run
#   <python> <default_first_script> <default_first_args_template>
# to see if this immediately brings up remote gxpd.
# %(target_label)s and %(root_gupid)s are replaced by options given
# in the command line
#

default_first_script = "$GXP_DIR/gxpd.py"

default_first_args_template = [ "--listen", "none:",
                                "--parent", "$GXP_GUPID",
                                "--target_label", "%(target_label)s",
                                "--root_gupid", "%(root_gupid)s" ]

#
# An arbitrary string that is used to indicate that the remote gxpd
# successfully brought up
#

default_hello = "hogehoge"

#
# When remotely installing gxp, here is where.
# 

default_target_prefix = "~/.gxp_tmp"

#
# Source files that the installer brings to the remote host.
#

default_src_files = [ "$GXP_DIR",
                      "$GXP_DIR/gxpc",
                      "$GXP_DIR/gxpc.py",
                      "$GXP_DIR/ioman.py",
                      "$GXP_DIR/opt.py", 
                      "$GXP_DIR/gxpd.py",
                      "$GXP_DIR/expectd.py",
                      "$GXP_DIR/ifconfig.py",
                      "$GXP_DIR/inst_local.py",
                      "$GXP_DIR/inst_remote_stub.py",
                      "$GXP_DIR/inst_remote.py",
                      "$GXP_DIR/gxpm.py",
                      "$GXP_DIR/gxpbin",
                      "$GXP_DIR/gxpbin/opt.py",
                      "$GXP_DIR/gxpbin/ifconfig.py",
                      "$GXP_DIR/gxpbin/bomb",
                      # "$GXP_DIR/gxpbin/acp",
                      "$GXP_DIR/gxpbin/bcp",
                      "$GXP_DIR/gxpbin/psfind",
                      "$GXP_DIR/gxpbin/nodefind",
                      "$GXP_DIR/gxpbin/nicer",
                      "$GXP_DIR/gxpbin/micer",
                      "$GXP_DIR/gxpbin/gxp_sched",
                      "$GXP_DIR/gxpbin/gxp_mom",
                      "$GXP_DIR/gxpbin/xmake",
                      "$GXP_DIR/gxpbin/xmake.mk",
                      "$GXP_DIR/gxpbin/mksh",
                      "$GXP_DIR/gxpbin/qsub_wrap",
                      "$GXP_DIR/gxpbin/mount_all",
                      "$GXP_DIR/gxpbin/su_cmd",
                      # "$GXP_DIR/gxpbin/gxp_sched_dev",
                      # "$GXP_DIR/gxpbin/gxp_mom_dev",
                      # "$GXP_DIR/gxpbin/sched_common.py",
                      # "$GXP_DIR/gxpbin/gather",
                      # "$GXP_DIR/gxpbin/make_sched.py",
                      # "$GXP_DIR/gxpbin/make_mom.py",
                      # "$GXP_DIR/gxpbin/xcp",
                      # "$GXP_DIR/gxpbin/ucpw",
                      # "$GXP_DIR/gxpbin/ucp_common.py",
                      ]

#
# The script that will be invoked on the remote host to move everything
# to the remote host
#

default_inst_remote_file = "$GXP_DIR/inst_remote.py"
default_inst_remote_stub_file = "$GXP_DIR/inst_remote_stub.py"

#
# After the installation is done, we invoke
#  <python> <second_script> <second_args_template>
#

# default_second_script = "$GXP_DIR/gxpd.py"
default_second_script = "%(inst_dir)s/$GXP_TOP/gxpd.py"
default_second_args_template = [ "--remove_self",
                                 "--listen", "none:",
                                 "--parent", "$GXP_GUPID",
                                 "--target_label", "%(target_label)s",
                                 "--root_gupid", "%(root_gupid)s" ]

#
# Default timeout values
#

default_hello_timeout = 40.0
default_install_timeout = 100.0

dbg = 0

# -------------------------------------------------------------------
# options
# -------------------------------------------------------------------

class inst_options(opt.cmd_opts):
    def __init__(self):
        opt.cmd_opts.__init__(self)
        # ---------------- 
        #    mandatory arguments that must be supplied each time
        # ---------------- 
        # target label of the gxpd that eventually starts
        self.target_label = ("s", None)
        # gupid of the root gxpd
        self.root_gupid = ("s", None)
        # sequence number
        self.seq = ("s", None)
        # list of rsh-like programs to run commands remotely
        self.rsh = ("s*", [])

        # ---------------- 
        # optional arguments that can be omitted and have 
        # reasonable default values above
        # ---------------- 
        # (1) list of possible python paths 
        self.python = ("s*", []) # if empty, use [ default_python ]
        # (2) the command which is run to test if things have 
        # already been installed, so there is no need to install
        self.first_script = ("s", default_first_script)
        self.first_args_template = ("s*", default_first_args_template)
        # (3) a string the remote node is supposed to say when
        # things brought up
        self.hello = ("s", default_hello)
        # (4) the directory (on the remote node) to install things to
        self.target_prefix = ("s", default_target_prefix)
        # (5) source files to copy to the remote node
        self.src_file = ("s*", default_src_files)
        # (6) template from which installation (bootstrapping) script
        # is created
        self.inst_remote_file = ("s", default_inst_remote_file)
        self.inst_remote_stub_file = ("s", default_inst_remote_stub_file)
        # (7) script and arguments to eventually run after installation
        self.second_script = ("s", default_second_script)
        self.second_args_template = ("s*", default_second_args_template)
        # (8) control timeout and verbosity
        self.hello_timeout = ("f", default_hello_timeout)
        self.install_timeout = ("f", default_install_timeout)
        self.dbg = ("i", 0)
    def postcheck(self):
        if len(self.python) == 0:
            self.python.append(default_python)

# -----------
# installer
# -----------

class installer(expectd.expectd):

    def Em(self, m):
        self.stderr.write_stream(m)
        # self.stderr.write_msg(m)

    def Wm(self, m):
        # self.stdout.write_stream(m)
        self.stdout.write_msg(m)

    def find_ancestor(self, path, top_dirs):
        """
        check if any of the directories in top_dirs is an ancestor
        of path. 
        e.g., path="/a/b/c" and top_dirs = [ "/a", "/x", "/y/z" ],
        it returns "/a", "/b/c"
        """
        for top in top_dirs:
            if string.find(path, top) == 0:
                return top,path[len(top) + 1:]
        return None,None
        
    def expand(self, path, dic):
        """
        expand $VAR and ~ , and collapse ../ and ./ things
        """
        if dic: path = path % dic
        return os.path.normpath(os.path.expandvars(os.path.expanduser(path)))

    def expands(self, paths, dic):
        """
        template : list of strings. each string may contain %(key)s
        dic : dictionary mapping keys to strings.
        apply for each string the mapping
        """
        A = []
        for path in paths:
            A.append(self.expand(path, dic))
        return A

    def read_file(self, file):
        fp = open(file, "rb")
        r = fp.read()
        fp.close()
        return r

    def subst_cmd3(self, cmd, rsh_template):
        S = []
        for t in rsh_template:
            S.append(t % { "cmd" : cmd })
        return S

    def remote_installed(self):
        """
        true if this process is running the automatically installed gxpd
        """
        flag = os.path.join(os.environ["GXP_DIR"], "REMOTE_INSTALLED")
        if os.path.exists(flag):
            return 1
        else:
            return 0

    def find_top_dirs(self, inst_files):
        """
        find "top directories" among inst_files (normally default_src_files).
        top directory is a directory whose parent is not in the list
        (inst_files).
        e.g., if inst_files are: [ "a", "a/b", "a/c", "d/x" ],
        top_directories are "a" and "d"
        
        """
        top_dirs = []
        for path in inst_files:
            path = self.expand(path, None)
            # path is like /home/tau/proj/gxp3/hoge
            ancestor,_ = self.find_ancestor(path, top_dirs)
            if ancestor is None:
                top_dirs.append(path)
        return top_dirs

    def mk_installed_data(self, inst_files):
        """
        inst_files : list of filenames to install (normally,
        default_src_files)

        convert it into a list of self-contained information sent to
        the remote host that it can generate all contents from.
        e.g., for regular files, this procdure converts it to
        (parent_dir, "REG", mode, base64_encoded_contensht).

        along the way it finds "top directories" among them. top directory
        is a directory whose parent is not in the list (inst_files).
        e.g., if inst_files are: [ "a", "a/b", "a/c", "d/x" ],
        top_directories are "a" and "d"
        
        """
        top_dirs = self.find_top_dirs(inst_files)
        inst_data = []
        for path in inst_files:
            path = self.expand(path, None)
            # path is like /home/tau/proj/gxp3/hoge
            ancestor,rel_path = self.find_ancestor(path, top_dirs)
            assert ancestor is not None, (path, top_dirs, inst_files)
            inst_path = os.path.join(os.path.basename(ancestor), rel_path)
            mode = os.stat(path)[0]
            # see what kind of file is it
            if stat.S_ISREG(mode):
                content = base64.encodestring(self.read_file(path))
                inst_data.append((inst_path,
                                  "REG", stat.S_IMODE(mode), content))
            elif stat.S_ISDIR(mode):
                inst_data.append((inst_path, "DIR", stat.S_IMODE(mode), None))
            elif stat.S_ISFIFO(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "fifo (ignored)\n" % path)
            elif stat.S_ISLNK(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "symbolic link (ignored)\n" % path)
            elif stat.S_ISBLK(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "block device (ignored)\n" % path)
            elif stat.S_ISCHR(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "char device (ignored)\n" % path)
            elif stat.S_ISSOCK(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "socket (ignored)\n" % path)
            else:
                bomb()
        return inst_data

    def mk_program(self, O, code):
        """
        Return a string of python program which, when invoked without
        argument, installs all inst_files 
        under a randomly created directory under target_prefix.
        For example, say target_prefix='target', 
        src_files=[ 'abc', 'def' ], it will create
        target/RANDOM_DIR/abc and target/RANDOM_DIR/def.
        When successful, the program will write <code> into standard
        out. The actual logic is taken from inst_remote_file.
        """
        inst_data = self.mk_installed_data(O.src_file)
        main = "install(%r, %r, %r)" % (O.target_prefix, inst_data, code)
        inst_remote = self.read_file(self.expand(O.inst_remote_file, None))
        prog = ("%s\n%s\n" % (inst_remote, main))
        return prog

    def mk_program2(self, O, code):

        """
        Return a string of python program which, when invoked without
        argument, installs all inst_files 
        under a randomly created directory under target_prefix.
        For example, say target_prefix='target', 
        src_files=[ 'abc', 'def' ], it will create
        target/RANDOM_DIR/abc and target/RANDOM_DIR/def.
        When successful, the program will write <code> into standard
        out. The actual logic is taken from inst_remote_file.
        """
        # append main function to inst_remote_file (normally inst_remote.py)
        if self.remote_installed():
            inst_data = None            # perhaps we need no install
        else:
            inst_data = self.mk_installed_data(O.src_file)
        first_script = self.expand(O.first_script, None)
        first_args = self.expands(O.first_args_template, O.__dict__)
        second_script = self.expand(O.second_script, None)
        second_args = self.expands(O.second_args_template, O.__dict__)
        gxp_top = os.environ["GXP_TOP"]
        main = ("check_install_exec(%r, %r, %r, %r, %r, %r, %r, %r)"
                % (first_script, first_args, second_script, second_args,
                   O.target_prefix, gxp_top, inst_data, code))
        inst_remote_stub = self.read_file(self.expand(O.inst_remote_stub_file,
                                                      None))
        inst_remote = self.read_file(self.expand(O.inst_remote_file, None))
        inst_remote_and_main = ("%s\n%s\n" % (inst_remote, main))
        prog = ("%s%10d%s" % (inst_remote_stub,
                              len(inst_remote_and_main), inst_remote_and_main))
	# wp = open("progprog", "wb")
	# wp.write(prog)
	# wp.close()
        return len(inst_remote_stub),prog

    def expect_hello(self, hello, timeout, forward_err):
        OK       = ioman.ch_event.OK
        begin_mark = "BEGIN_%s " % hello
        end_mark = " END_%s" % hello
        if dbg>=2: self.Em("expect %s %s\n" % (begin_mark, timeout))
        s = self.expect([ begin_mark, ("TIMEOUT", timeout)], forward_err)
        if s != OK: return (s, None)
        if dbg>=2: self.Em("expect %s %s\n" % (end_mark, 2.0))
        s = self.expect([ end_mark, ("TIMEOUT", 2.0)], forward_err)
        if s != OK: return (s, None)
        return (OK, self.ev.data[:-len(end_mark)])

    def install(self, O):
        OK       = ioman.ch_event.OK
        EOF      = ioman.ch_event.EOF
        IO_ERROR = ioman.ch_event.IO_ERROR
        TIMEOUT  = ioman.ch_event.TIMEOUT
        #
        pythons = O.python
        # -----------
        # check if the remote system is ready without installation
        if self.remote_installed():
            # We first try to bring up the remote gxpd.py, hoping the 
            # remote host has gxp in the same path as the local host does. 
            # However, this introduces version inconsistency problem, 
            # so we do this only when the local gxp is automatically 
            # installed (copied under ~/.gxp_tmp) by the installer.
            python = pythons[0]
            script = self.expand(O.first_script, None)
            args = self.expands(O.first_args_template, O.__dict__)
            cmd = "%s %s %s" % (python, script, string.join(args, " "))
            sub = self.subst_cmd3(cmd, O.rsh)

            if dbg>=2: self.Em("First bring up %s\n" % sub)
            self.spawn(sub)
            s,g = self.expect_hello(O.hello, O.hello_timeout, 0)
            if s != EOF: return g      # good he is ready
            if dbg>=2: self.Em("First bring up NG (go installation)\n")
            # if dbg>=2: self.Em("Killing %s\n" % self.proc.pid)
            self.kill()
            # if dbg>=2: self.Em("Waiting %s\n" % self.proc.pid)
            self.wait(0)
            # if dbg>=2: self.Em("Wait done\n")
        # -----------
        # failed (or not tried). we begin from something very simple 
        # and proceed step by step. first check if we are able to login
        # we run a command like 'ssh echo hello123456' and wait
        # for the string hello123456 to come from its stdout
        h = "hello%06d" % random.randint(0, 999999)
        sub = self.subst_cmd3(("echo %s" % h), O.rsh)
        if dbg>=2: self.Em("Login echo %s\n" % sub)
        self.spawn(sub)
        if self.expect([ "%s\n" % h, ("TIMEOUT", O.hello_timeout)], 1) != OK:
            if dbg>=1: self.Em("Login echo NG\n")
            return None
        if dbg>=2: self.Em("Login echo OK\n")
        self.wait(1)
        # -----------
        # OK, now we know we are able to login the remote node.
        # Next, search for python interpter by running
        # a command like
        # 'ssh python -c ...; /usr/local/bin/python -c ...'
        checks = []
        check_py = (r"""exec %s -c 'import sys; """
                    """print "%s",sys.hexversion'""")
        for p in pythons:
            checks.append(check_py % (p,p))
            checks.append(";")
        sub = self.subst_cmd3(string.join(checks, ""), O.rsh)
        if dbg>=2: self.Em("Search for python %s\n" % sub)
        self.spawn(sub)
        if self.expect([("TIMEOUT", O.hello_timeout)], 1) != EOF \
               or self.ev.data == "":
            if dbg>=1: self.Em("Search for python NG\n")
            return None
        python = string.split(self.ev.data)[0]
        if dbg>=2: self.Em("Search for python OK : %s\n" \
                           % string.strip(self.ev.data))
        assert python in pythons, (python, python)
        self.wait(1)
        # -----------
        # OK, now we know we are able to run a good python 
        # interpreter on the remote node. go ahead and
        # install the program.
        code = "INSTALL%09d" % random.randint(0, 999999999)
        prog = self.mk_program(O, code)
        sub = self.subst_cmd3(("%s -" % python), O.rsh)
        if dbg>=2: self.Em("Install %s\n" % sub)
        self.spawn(sub)
        self.send(prog)
        self.send_eof()
        if self.expect([code, ("TIMEOUT",
                               O.hello_timeout + O.install_timeout)], 1) != OK:
            if dbg>=1: self.Em("Install NG\n")
            return None
        if self.expect([" OK\n", ("TIMEOUT", O.hello_timeout)], 1) != OK:
            if dbg>=1: self.Em("Install NG\n")
            return None
        inst_dir = self.ev.data[:-4]
        script = self.expand(O.second_script, { "inst_dir" : inst_dir })
        if dbg>=2: self.Em("Install OK : '%s'\n" % script)
        self.wait(1)
        # -----------
        # finally we are ready to run IT on the remote node
        args = self.expands(O.second_args_template, O.__dict__)
        cmd = "%s %s %s" % (python, script, string.join(args, " "))
        sub = self.subst_cmd3(cmd, O.rsh)
        if dbg>=2: self.Em("Bring up again %s\n" % sub)
        self.spawn(sub)
        s,g = self.expect_hello(O.hello, O.hello_timeout, 1)
        if s == OK: return g
        if dbg>=2: self.Em("Bring up again NG\n")
        self.kill()
        return None

    def mk_python_cmdline(self, pythons, stub_sz):
        """
        return a shell command string like:

        if type --path <python1> ; then
           <python1> -c "import os; exec(os.read(0, <stub_sz>);" ;
        elif type --path <python2> ; then
           <python2> -c "import os; exec(os.read(0, <stub_sz>);" ;
        elif ...
           ...
        fi

        Essentially, we search for a usable python interpreter and
        executes whichever is found first.
        the strange code given to the python with -c option reads
        stub_sz bytes from the standard input and then exec it.
        what is actually read there is inst_remote_stub.py. It is
        another simple program that reads the specified number of
        bytes from the standard input and then executes it. the
        difference is that it can wait for as many bytes as specified
        even if read prematurely returns (see inst_remote_stub.py).
        what is eventually executed is inst_remote.py
        """
        P = []
        for python in pythons:
            body = ('%s -c "import os; exec(os.read(0, %d));" '
                    % (python, stub_sz))
            if len(P) == 0:
                p = ('if type %s > /dev/null; then %s ;' % (python, body))
            else:
                p = ('elif type %s > /dev/null; then %s ;' % (python, body))
            P.append(p)
        if len(P) > 0: 
            p = (' else echo no python interpreter found "(%s)" 1>&2 ; fi'
                 % string.join(pythons, ","))
            P.append(p)
        return ("/bin/sh -c '%s'" % string.join(P, ""))

    def install2(self, O):
        OK       = ioman.ch_event.OK
        EOF      = ioman.ch_event.EOF
        IO_ERROR = ioman.ch_event.IO_ERROR
        TIMEOUT  = ioman.ch_event.TIMEOUT
        #
        code = "INSTALL%09d" % random.randint(0, 999999999)
        stub_sz,prog = self.mk_program2(O, code)
        python_cmd = self.mk_python_cmdline(O.python, stub_sz)
        sub = self.subst_cmd3(python_cmd, O.rsh)
        if dbg>=2: self.Em("Install and exec %s\n" % sub)
        self.spawn(sub)
        self.send(prog)
        if self.expect([code, ("TIMEOUT", O.hello_timeout)], 1) != OK:
            if dbg>=1: self.Em("Install NG\n")
            return None
        if self.expect([" OK\n", " WD\n",
                        ("TIMEOUT", O.hello_timeout)], 1) != OK:
            if dbg>=1: self.Em("Install NG\n")
            return None
        if self.ev.data[-4:] == " WD\n":
            inst_data = self.mk_installed_data(O.src_file)
            inst_data_str = "%r" % inst_data
            inst_data_msg = "%10d%s" % (len(inst_data_str), inst_data_str)
            self.send(inst_data_msg)
        s,g = self.expect_hello(O.hello, O.hello_timeout, 1)
        if s == OK: return g
        if dbg>=2: self.Em("Bring up NG\n")
        # self.kill()
        return None

    def show_argv(self, argv):
        for a in argv:
            self.Em("'%s' " % a)
        self.Em("\n")

    def main(self, argv):
        global dbg
        if dbg>=2: self.show_argv(argv)
        O = inst_options()
        if O.parse(argv[1:]) == -1: return
        dbg = O.dbg
        
        ioman.set_log_filename("log-%s" % O.seq)
        g = self.install2(O)
        if g is not None:
            # say
            #  "Brought up on GUPID ACCESS_PORT TARGET_LABEL HOSTNAME\n"
            # e.g.,
            #  "Brought up on hongo100-tau-2008-07-06-14-40-00-3878 None hongo hongo100\n"
            self.Wm("Brought up on %s %s\n" % (g, O.seq))
        else:
            self.kill_x(signal.SIGINT)
            try:
                time.sleep(2.0)
            except KeyboardInterrupt:
                pass
            self.kill_x(signal.SIGKILL)
        self.wait(1)
        self.flush_outs()

def main():
    installer().main(sys.argv)

if __name__ == "__main__":
    main()

