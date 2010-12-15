#!/usr/bin/env python

# in order to support join/leave
# we need to update session.peer_tree



import errno,math,os,re,select,signal,socket,string,sys,time,types
import gxpc,gxpm,ioman,opt

import cPickle,cStringIO

dbg=0

def Ws(s):
    sys.stdout.write(s)

def Es(s):
    sys.stderr.write(s)


class jobsched_cmd_opts(opt.cmd_opts):
    """
    command line options we accept
    """
    def __init__(self):
        #             (type, default)
        # types supported
        #   s : string
        #   i : int
        #   f : float
        #   l : list of strings
        #   None : flag
        opt.cmd_opts.__init__(self)
        self.conf  = ("s", "gxp_js.conf") # config file
        self.attrs = ("s*", [])           # --attr x=y
        self.help  = (None, 0)
        self.c     = "conf"
        self.a     = "attrs"
        self.h     = "help"

class jobsched_tokenizer:
    """
    config file tokenizer
    """
    def __init__(self):
        self.elem_reg_str = self.elem_regexp()
        self.elem_reg = re.compile(self.elem_reg_str)
        self.eq_reg_str = "(?P<eq>\+?\=)"
        self.eq_reg = re.compile(self.eq_reg_str)

    def elem_regexp(self):
        # not white space, not double quote character (")
        non_ws_non_quote_non_equal_char = '[^\s\"\+\=]'
        # not double quote character (")
        non_quote_char = '[^\"]'
        # element: either a non-quoted list of chars containing no 
        # white spaces nor double quotes, or a quoted string
        elem = ('(?P<raw>%s+)|\"(?P<quoted>%s*)\"' 
                % (non_ws_non_quote_non_equal_char, non_quote_char))
        return elem

    def init(self, filename, lineno, line):
        self.filename = filename
        self.lineno = lineno
        self.line = line
        self.rest = line
        self.s = None
        self.val = None
        return self.next()

    def warn_parse_error(self):
        Es("%s:%d: parse error [%s]" 
           % (self.filename, self.lineno, self.line))


    def next(self):
        self.rest = self.rest.lstrip()
        # EOF
        if self.rest == "":
            self.s = ""
            self.val = None
            if dbg>=2:
                Es("  EOF\n")
            return self
        # += or =
        m = self.eq_reg.match(self.rest)
        if m:
            self.s = self.val = m.group("eq")
            if dbg>=2:
                Es("  next token from [%s] -> %s\n" % (self.rest, self.s))
            self.rest = self.rest[m.end():]
            return self
        # should be element+
        E = []
        any_quoted = 0
        orig_rest = self.rest
        while self.rest != "":
            if dbg>=2:
                Es("   next element from [%s]\n" % self.rest)
            m = self.elem_reg.match(self.rest)
            if m:
                r,q = m.group("raw", "quoted")
                if r is None:
                    any_quoted = 1
                    E.append(q)
                else:
                    E.append(r)
                self.rest = self.rest[m.end():]
            else:
                break
        if len(E) == 0:
            self.warn_parse_error()
            self.s = ""
            self.val = None
            return self
        self.s = "".join(E)
        if dbg>=2:
            Es("  next token from [%s] -> %s\n" 
               % (orig_rest, self.s))
        if any_quoted:
            self.val = self.s
        else:
            self.val = self.token_val(self.s)
        return self

    def safe_atoi(self, s):
        try:
            return string.atoi(s)
        except ValueError:
            return None

    def safe_atof(self, s):
        try:
            return string.atof(s)
        except ValueError:
            return None

    def token_val(self, s):
        """
        convert a string s into an 'appropriate'
        python object.
        if it looks like an int or a float, it returns
        the converted number.
        if it looks like 1M 1.5G, etc., it returns the
        appropriate number.
        otherwise it returns the string.
        """
        i = self.safe_atoi(s)
        if i is not None: return i
        f = self.safe_atof(s)
        if f is not None: return f
        m = s[-1:].lower()
        if m in "kmgtpez":
            x = self.safe_atoi(s[:-1])
            if x is None: x = self.safe_atof(s[:-1])
            if x is not None:
                if m == "k": return x * (2**10)
                if m == "m": return x * (2**20)
                if m == "g": return x * (2**30)
                if m == "t": return x * (2**40)
                if m == "p": return x * (2**50)
                if m == "e": return x * (2**60)
                if m == "z": return x * (2**70)
                bomb
        return s

class jobsched_config:
    """
    object representing configuration
    """
    db_fields = [ "opts",
                  "work_file", "work_fd", "work_py_module", "work_server_sock",
                  "work_proc_pipe", "work_proc_pipe2", "work_proc_sock",
                  "worker_prof_cmd",
                  "work_list_limit", "state_dir", "template_html",
                  "gen_html_overhead", "refresh_interval",
                  "cpu_factor", "mem_factor", "trans_dirs", "job_output",
                  "conf_file", "log_file", "host_job_attrs" ]

    
    def __init__(self, opts):
        self.opts = opts
        self.work_file = []     # list of strings
        self.work_fd = []       # list of ints
        self.work_py_module = []       # list of strings
        self.work_proc_pipe = []
        self.work_proc_pipe2 = []
        self.work_proc_sock = []
        self.work_proc_sock2 = []
        self.work_server_sock = []
        self.work_server_sock2= []

        self.worker_prof_cmd = "${GXP_DIR}/gxpbin/worker_prof"
        self.work_list_limit = 100
        self.state_dir = "state"
        self.template_html = "${GXP_DIR}/gxpbin/gxp_js_template.html"
        self.gen_html_overhead = 0.05
        self.refresh_interval = 60
        self.cpu_factor = 1.0
        self.mem_factor = 0.9
        self.trans_dirs = []
        self.job_output = [ 1, 2 ]

        # some make specific ones
        self.make_cmd = "make"
        self.make_exit_status_connect_failed = 125
        self.make_exit_status_server_died = 126
        self.make_local_exec_cmd = None

        self.ctl = None

        # probably you will not be interested in
        # the following configs, but just for the
        # sake of flexibiliy 
        self.conf_file = "gxp_js.conf"
        self.log_file = "gxp_js.log"

        # attrs["host","cpu"] = ...
        self.host_job_attrs = {}
        self.set_scope("host", ".*", re.compile(".*"))

    def __str__(self):
        S = []
        items = self.__dict__.items()
        items.sort()
        for k,v in items:
            if k != "host_job_attrs":
                S.append(("%s : %s" % (k, v)))
        host_job_items = self.host_job_attrs.items()
        host_job_items.sort()
        for k,V in host_job_items:
            S.append(" %s,%s :" % k)
            for reg_str,reg,val in V:
                S.append("  %s : %s" % (reg_str, val))
        return "\n".join(S)

    def warn(self, filename, lineno, line, msg):
        Es("%s:%d: warning: %s (%s)\n" 
           % (filename, lineno, msg, string.rstrip(line)))

    def set_scope(self, scope, reg_str, reg):
        self.cur_scope = scope
        self.cur_reg_str = reg_str
        # FIXIT: handle exception
        self.cur_reg = reg

    def set_host_job_attr(self, scope, reg_str, reg, key, val):
        if dbg>=2:
            Es(" host_job_attr %s %s : %s = %s\n" % (scope, reg_str, key, val))
        if not self.host_job_attrs.has_key((scope,key)):
            self.host_job_attrs[scope,key] = []
        self.host_job_attrs[scope,key].append((reg_str, reg, val))

    def set_host_job_attrs(self, scope, reg_str, reg, tok):
        n = 0
        while tok.s != "":
            key = tok.s
            eq = tok.next().s
            if eq == "=" or eq == "+=":
                val = tok.next().val
            else:
                val = tok.val
            self.set_host_job_attr(scope, reg_str, reg, key, val)
            tok.next()
            n = n + 1
        return n

    def parse_line(self, filename, lineno, line, tok):
        """
        parse a single line
        """
        if dbg>=2:
            Es("parse_line: %s:%d [%s]\n" % (filename, lineno, line))
        ls = line.lstrip()
        if ls[:1] == "#": return
        if ls.rstrip() == "": return 
        tok.init(filename, lineno, line)
        x = tok.s
        if dbg>=2:
            Es(" 1st token: %s\n" % x)
        if x == "host" or x == "job":
            # host REGEXP KEY VAL KEY VAL ...
            scope = x
            reg_str = tok.next().s
            # FIXIT: exception handling
            reg = re.compile(reg_str)
            tok.next()
            if dbg>=2:
                Es(" setting attributes %s %s\n" % (scope, reg_str))
            if self.set_host_job_attrs(scope, reg_str, reg, tok) == 0:
                self.set_scope(scope, reg_str, reg)
        elif x == "trans_dirs":
            tok.next()
            rest = tok.rest
            rhs_items = []
            while tok.s != "":
                rhs_items.append(tok.val)
                tok.next()
            assert (len(rhs_items) > 0), tok.rest
            self.trans_dirs.append((rhs_items[0], rhs_items[1:]))
        elif hasattr(self, x):
            # generic attributes (cpu_factor, mem_factor, etc)
            orig = getattr(self, x)
            rest = tok.rest
            eq = tok.next().s
            if type(orig) is types.ListType:
                # this attribute is a list attribute
                if eq == "=" or eq == "+=": 
                    tok.next()
                else:
                    eq = "="
                rhs_items = []
                while tok.s != "":
                    rhs_items.append(tok.val)
                    tok.next()
                if eq == "=":
                    if dbg>=2:
                        Es(" setting global attributes %s = %s\n"
                           % (x, rhs_items))
                    setattr(self, x, rhs_items)
                else:
                    if dbg>=2:
                        Es(" setting global attributes %s += %s\n"
                           % (x, rhs_items))
                    setattr(self, x, orig + rhs_items)
            else:
                if eq == "=" or eq == "+=": 
                    rest = tok.rest
                    tok.next()
                else:
                    eq = "="
                v = tok.token_val(rest.strip())
                if dbg>=2:
                    Es(" setting global attributes %s = '%s'\n" % (x, v))
                setattr(self, x, v)
        else:
            self.set_host_job_attrs(self.cur_scope, self.cur_reg_str,
                                    self.cur_reg, tok)
            
    def parse_list(self, filename, lines, tok):
        i = 1
        for line in lines:
            if self.parse_line(filename, i, line, tok) == -1:
                return -1
            i = i + 1
        return 0

    def parse_fp(self, filename, fp, tok):
        return self.parse_list(filename, fp.readlines(), tok)

    def parse_file(self, filename, tok):
        x = []
        if os.path.exists(filename):
            fp = open(filename, "rb")
            x = self.parse_fp(filename, fp, tok)
            fp.close()
        else:
            Es("warning: config file %s does not exist\n"
               % filename)
        return x

    def parse_cmdline(self, attrs, tok):
        # attrs : attributes given in the command line
        self.parse_list("<cmdline>", attrs, tok)

    def parse(self):
        tok = jobsched_tokenizer()
        # process attributes given in the command line
        # via --attrs x=y  (or simply -a x=y) first
        if self.parse_file(self.opts.conf, tok) == -1:
            return -1
        if self.parse_cmdline(self.opts.attrs, tok) == -1:
            return -1
        return 0

    def get_host_or_job_attr(self, host_or_job, attr, name, default):
        attrs = self.host_job_attrs.get((host_or_job,attr))
        if attrs is None: return default
        for regexp_str,regexp,val in attrs:
            if regexp.match(name): return val
        return default

    def get_man_attr(self, gupid, key, default):
        """
        gupid : worker unique name
        key : like "cpu", "mem", etc.
        """
        return self.get_host_or_job_attr("host", key, gupid, default)

    def get_job_attr(self, cmd, key, default):
        """
        cmd : cmd line
        key : like "cpu", "mem", etc.
        """
        return self.get_host_or_job_attr("job", key, cmd, default)

    def mk_man_capacity(self, gupid):
        D = {}
        for host_or_job,key in self.host_job_attrs.keys():
            if host_or_job == "host":
                x = self.get_man_attr(gupid, key, None)
                if x is not None: D[key] = x
        return D

def dbg_jobsched_config():
    o = jobsched_cmd_opts()
    if o.parse([ "-a", "conf_file=gxp_js.conf" ]) == -1:
        return -1
    Es("\n")
    c = jobsched_config(o)
    c.parse()
    Ws("%s\n" % c)
    return c

# --------------------
# main things
# --------------------

class man_state:
    """
    worker (man) state
    """
    active = "0_active"
    leaving = "1_leaving"
    gone = "2_gone"

class man_join_leave_record:
    """
    whenever a man joins or leaves, the user does 
      gxpc js -a ctl=join or gxpc js -a ctl=leave
    this creates a process and gxp_js.py will receive
    its IO and exit status. for each join/leave operation,
    we record them
    """
    def __init__(self):
        self.io = { 1 : cStringIO.StringIO(), 
                    2 : cStringIO.StringIO() }
        self.done_io = {}
        self.wait_status = None

    def completed(self):
        if len(self.io) > 0: return 0
        if self.wait_status is None: return 0
        return 1

    def record_die(self, wait_status):
        """
        called when gxp got notificatin that
        the initial hello process exited (gxpd 'waited' it).
        some outputs from the process may still be coming.
        """
        # assert wait_status is not None
        if self.wait_status is None:
            self.wait_status = wait_status

    def record_io(self, fd, payload, eof):
        """
        called when gxp got notificatin that
        the initial hello process outputs something
        (payload) to its file descriptor (fd).
        eof = 1 iff this is the last data (we can 
        assume no more data will be coming from the
        same fd)
        """
        # record whatever we got.
        # this is normally a string telling us
        # about the spec of this worker (e.g.,
        # cpu 5 mem 4g
        # if io[fd] does not exist, this man
        # has already been working, so we ignore
        # it
        # if not self.io.has_key(fd): return
        if payload != "":
            self.io[fd].write(payload)
        if eof:
            # this is the last msg. we indicate
            # it by moving the record from io to
            # done_io.
            s = self.io[fd].getvalue()
            del self.io[fd]
            assert not self.done_io.has_key(fd)
            self.done_io[fd] = s


class Man:
    """
    a worker, or a man
    """
    db_fields = [ "man_idx", "name", "n_runs", "capacity", 
                  "time_last_heartbeat" ]
    default_capacity = { "cpu" : 1 }

    def __init__(self, man_idx, name, capacity, cur_time, server):
        self.man_idx = man_idx  # serial number
        self.name = name        # name (gupid)
        self.capacity = capacity # dictionary of label : integer
        self.capacity_left = {} # set in finalize_capacity
        self.state = man_state.active
        # created time
        self.create_time = cur_time
        # last time at which I heard from him
        self.time_last_heartbeat = cur_time
        self.runs_running = {} # run_idx -> run
        self.n_runs = 0
        # volatile
        self.server = server
        if 0:
            self.io = { 1 : cStringIO.StringIO(), 
                        2 : cStringIO.StringIO() }
            self.done_io = {}
            self.wait_status = None
        else:
            # join/leave records (rid -> man_join_leave_record)
            self.jl_recs = {} 

    def __str__(self):
        S = []
        S.append("%s" % self.name)
        for k,v in self.capacity.items():
            vl = self.capacity_left.get(k)
            S.append(" %s: %s/%s" % (k, vl, v))
        return "\n".join(S)

    def ensure_jl_rec(self, rid):
        if not self.jl_recs.has_key(rid):
            self.jl_recs[rid] = man_join_leave_record()
        
    def completed(self, rid):
        self.ensure_jl_rec(rid)
        return self.jl_recs[rid].completed()

    def record_io(self, rid, fd, payload, eof):
        self.ensure_jl_rec(rid)
        self.jl_recs[rid].record_io(fd, payload, eof)

    def record_die(self, rid, status):
        self.ensure_jl_rec(rid)
        self.jl_recs[rid].record_die(status)

    def finalize_capacity(self, conf, rid):
        """
        called when we detect that the initial hello process
        completely terminated (got the wait notification and 
        all outpupt fds got EOFs)
        """
        if self.server.logfp:
            self.server.LOG("finalize_capacity of %s\n" 
                            % self.name)
        # parse its standard output, expecting worker spec
        # like "cpu 8 mem 4g"
        # get its standard output
        fields = self.jl_recs[rid].done_io[1].split()
        n = len(fields)
        if n % 2 == 1: n = n - 1
        # split into key-value pairs
        # assuming key1 val1 key2 val3 ...
        for i in range(0, n, 2):
            key = fields[i]
            val = int(fields[i + 1])
            # use this value only when not given
            # in the config file
            if not self.capacity.has_key(key):
                if self.server.logfp:
                    self.server.LOG("setting %s %s to %d\n" 
                                    % (self.name, key, val))
                self.capacity[key] = val
        # supply global default
        for k,v in Man.default_capacity.items():
            if not self.capacity.has_key(k):
                self.capacity[k] = v
        # multiply xxx_factor
        for k,v in self.capacity.items():
            # cpu or mem
            k_factor = ("%s_factor" % k)
            if hasattr(conf, k_factor):
                factor = getattr(conf, k_factor)
                if type(v) is types.IntType:
                    self.capacity[k] = int(v * factor)
                else:
                    self.capacity[k] = v * factor
        self.capacity_left = self.capacity.copy()

    def modify_resource(self, requirement, sign):
        for k,v in requirement.items():
            self.capacity_left[k] = self.capacity_left[k] + sign * v

    def has_affinity(self, affinity):
        for a,p in affinity.items():
            if hasattr(self, a):
                av = getattr(self, a)
                if p.match(av) is None: return 0
        return 1

    def has_resource(self, requirement):
        for k,v in requirement.items():
            x = self.capacity_left.get(k)
            if x is None or x < v: return 0
        return 1

    def get_td_name(self):
        """
        called by html generator to generate an element
        of the table indicating its name and the status
        (by <td class="...">)
        """
        if self.state == man_state.active \
                and len(self.runs_running) == 0:
            return ("man_free", self.name)
        else:
            return (("man_%s" % self.state), self.name)

    def get_td_capacity(self):
        C = []
        keys = self.capacity.keys()
        keys.sort()
        for k in keys:
            c = self.capacity[k]
            cl = self.capacity_left.get(k)
            C.append("%s: %s / %s" % (k, cl, c))
        return "<br>".join(C)

class man_generator:
    """
    generate a new man
    """
    def __init__(self, conf, server):
        self.next_man_idx = 0
        self.conf = conf        # jobsched_config object
        self.server = server

    def make_man(self, gupid):
        man_idx = self.next_man_idx
        self.next_man_idx = man_idx + 1
        capacity = self.conf.mk_man_capacity(gupid)
        man = Man(man_idx, gupid, capacity, 
                  self.server.cur_time(), self.server)
        return man

class men_monitor:
    def __init__(self, conf, server):
        self.conf = conf
        self.server = server
        self.time_last_ping = 0.0

class run_status:
    queued = "queued"
    running = "running"
    finished = "finished"
    worker_died = "worker_died"
    worker_left = "worker_left"
    no_throw = "no_throw"
    interrupted = "interrupted"

class Run:
    """
    state of a running task
    """
    def init(self, work, run_idx, io_dir, job_output):
        self.work_idx = work.work_idx # work idx
        self.run_idx = run_idx
        self.status = run_status.queued
        self.exit_status = None
        self.term_sig = None
        self.man_name = None         # set by find_matches (Man object)
        self.time_start = None       # set when thrown
        self.time_end = None         # set when returned
        self.time_since_start = None # set whenever profiled
        # following are all set when returned
        self.worker_time_start = None # time when started by worker
        self.worker_time_end = None   # time when finished by worker
        self.utime = None             # user time
        self.stime = None             # sys time
        self.maxrss = None
        self.ixrss = None
        self.idrss = None
        self.isrss = None
        self.minflt = None              # minor faults
        self.majflt = None              # major faults
        self.io_dir = io_dir
        # volatile (not persistent) fields. not put in database
        self.work = work
        self.man = None
        # FIXIT: make them configurable
        self.io = {}
        for fd in job_output:
            self.io[fd] = (cStringIO.StringIO(), None, None)
        self.done_io = {}
        self.hold_limit = float("inf")
        return self

    def __str__(self):
        return "%s" % self.work.cmd

    def record_die(self, wait_status, rusage,
                   worker_time_start, worker_time_end):
        """
        called when we receive die notification of the process.
        outputs may still follow so at this point we cannot abandon
        this object.
        """
        exit_status = term_sig = None
        if os.WIFEXITED(wait_status):
            exit_status = os.WEXITSTATUS(wait_status)
        elif os.WIFSIGNALED(wait_status):
            term_sig = os.WTERMSIG(wait_status)
        self.status = run_status.finished
        self.exit_status = exit_status
        self.term_sig = term_sig
        self.worker_time_start = worker_time_start
        self.worker_time_end = worker_time_end
        if rusage:
            self.utime = rusage[0]
            self.stime = rusage[1]
            self.maxrss = rusage[2]
            self.ixrss = rusage[3]
            self.idrss = rusage[4]
            self.isrss = rusage[5]
            self.minflt = rusage[6]
            self.majflt = rusage[7]

    def add_io(self, fd, payload, eof):
        """
        called when we got notificatiion that
        the process emitted something (payload)
        from its file descriptor (fd).
        eof = 1 iff this is the last data from 
        the fd.
        """
        # inline   : string IO object
        # filename : filename or None
        # wp       : file object for filename or None
        if self.work.server.logfp:
            self.work.server.LOG("add_io : run=%s fd=%d eof=%d payload=[%s]\n" 
                                 % (self, fd, eof, payload))
            
        inline,filename,wp = self.io[fd]
        if payload != "":
            # record whatever is output
            inline.write(payload)
            s = inline.getvalue()
            max_inline_io = 128 * 1024  # 128KB
            if len(s) > max_inline_io:
                # on-memory data too large, flush into file
                if wp is None:
                    filename = "run_%d_%d_%d" % (self.work_idx, self.run_idx, fd)
                    filename = os.path.join(self.io_dir, filename)
                    wp = open(filename, "wb")
                wp.write(s)
                wp.flush()
                # and free the memory
                inline.truncate()
            self.io[fd] = (inline, filename, wp)
        if eof:
            # got EOF, so we indicate it by deleting
            # the entry from io and move the record
            # to done_io
            s = inline.getvalue()
            if wp: 
                wp.write(s)
                wp.close()
                inline.truncate()
            # mark io from fd has done
            del self.io[fd]
            s = inline.getvalue()
            self.done_io[fd] = (s, filename)
                    
    def get_io_filenames(self):
        fds = {}
        for fd in self.io.keys(): fds[fd] = None
        for fd in self.done_io.keys(): fds[fd] = None
        fds = fds.keys()
        fds.sort()
        S = {}
        for fd in fds:
            x = self.io.get(fd)
            if x:
                _,filename,_ = x
            else:
                _,filename = self.done_io.get(fd)
            S[fd] = filename
        return S
        

    def get_io_inline(self):
        """
        FIXIT: should merge stdio and stderr
        """
        fds = {}
        for fd in self.io.keys(): fds[fd] = None
        for fd in self.done_io.keys(): fds[fd] = None
        fds = fds.keys()
        fds.sort()
        S = {}
        for fd in fds:
            x = self.io.get(fd)
            if x:
                inline,_,_ = x
                s = inline.getvalue()
            else:
                s,_ = self.done_io.get(fd)
                
            S[fd] = s
        return S

    def is_finished(self):
        """
        check if this guy has really finished
        ('wait'ed by gxpd and their out fds closed)
        """
        if len(self.io) > 0: return 0
        if self.status == run_status.queued: return 0
        if self.status == run_status.running: return 0
        return 1

    def finish(self, cur_time):
        self.time_end = cur_time
        if self.time_start is None:
            # none if the job has not started.
            # we consider them just started now
            self.time_start = cur_time
        self.time_since_start = self.time_end - self.time_start
        return self.work.finish_or_retry(self.status, self.exit_status,
                                         self.term_sig, self.man_name)

    def sync(self, cur_time):
        self.time_since_start = cur_time - self.time_start
        self.work.server.works.update_run(self)

    # db-related stuff
    # following fields of this object will go to database/csv file/html
    # for field x for which get_td_x method exists, 
    # get_td_worker_time method is called and  its return value used .
    # so result column will be obtained by self.get_td_result(), etc.
    db_fields = [ "run_idx", "result", "man_name", 
                  "time_start", "time_end", "time_since_start",
                  "worker_time_start", "worker_time_end", "worker_time",
                  "utime", "stime", "maxrss", "ixrss", "idrss", "isrss",
                  "minflt", "majflt", "io", "io_filename" ]

    def get_td_result(self):
        if self.status == run_status.finished:
            if self.exit_status is not None:
                if self.exit_status == 0:
                    return ("job_success", "exit 0")
                else:
                    return ("job_failed", ("exit %d" % self.exit_status))
            elif self.term_sig is not None:
                return ("job_killed", ("killed %d" % self.term_sig))
            else:
                assert 0, (self.status, self.exit_status, self.term_sig)
        else:
            return (("job_%s" % self.status), self.status)

    def get_td_worker_time(self):
        s = self.worker_time_start
        e = self.worker_time_end
        if s is None or e is None:
            assert s is None
            assert e is None
            return "-"
        else:
            return e - s

    def get_td_io(self):
        io = []
        for fd,(inline,_,_) in self.io.items():
            io.append(inline.getvalue())
        for fd,(inline_s,_) in self.done_io.items():
            io.append(inline_s)
        x = "".join(io)
        if len(x) == 0: return "<br>"
        return x

    def get_td_io_filename(self):
        filenames = []
        for fd,(_,filename,_) in self.io.items():
            if filename is not None:
                filenames.append('<a href="%s">%d</a>'
                                 % (filename, fd))
        x = ",".join(filenames)
        if len(x) == 0: return "-"
        return x

class Work:
    """
    a work or a job sent from clients
    """
    db_fields = [ "work_idx", "cmd", "pid", "dirs", "time_req" ]

    def init(self, cmd, pid, dirs, envs, req, affinity):
        # command line (string)
        self.cmd = cmd
        # pid (or None if not applicable/relevant)
        self.pid = pid
        # directories that should be tried for job's cwd
        self.dirs = dirs
        # environments that must be set for the job
        self.envs = envs.copy()
        # resource requirement of the work
        self.requirement = req
        self.affinity = affinity
        self.next_run_idx = 0
        return self

    def init2(self, work_idx, cur_time, server):
        self.envs["GXP_JOBSCHED_WORK_IDX"] = ("%d" % work_idx)
        self.envs["GXP_MAKE_WORK_IDX"] = ("%d" % work_idx)
        self.work_idx = work_idx
        self.time_req = cur_time # time requested
        # volatile fields
        self.server = server
        return self

    def make_run(self):
        """
        create a new run for this work
        """
        run_idx = self.next_run_idx
        self.next_run_idx = run_idx + 1
        run = Run().init(self, run_idx, 
                         self.server.conf.state_dir,
                         self.server.conf.job_output)
        self.server.runs_todo.append(run)
        self.server.works.add_run(self.work_idx, run)

    def retry(self):
        # retry
        msg = ("work '%s' will be retried\n" % self.cmd)
        if self.server.logfp: self.server.LOG(msg)
        self.make_run()

    def finish_or_retry(self, status, exit_status, term_sig, man_name):
        if status == run_status.worker_died \
                or status == run_status.worker_left:
            self.retry()
            return 0
        else:
            return self.server.wkg.finish_work(self.work_idx, exit_status, term_sig, man_name)

# 
# work generation framework
#

#
# format of stream (example)
# 
# echo hello
# echo hoge
# hostname
#
# env: x=y
# cmd: hostname
# end:
#
# stream ::= element*
# element ::= continuation_line* last_line
# continuation_line ::= char* '\' NEWLINE
# last_line ::= NEWLINE | char* any_char_but_backslash NEWLINE
#
# element is either:
# env: ... | cwd: ... | cmd: ... | end: | ...
# 

class work_stream_base:
    def __init__(self, server):
        self.server = server
        self.leftover = ""
        self.closed = 0
        # FIXIT: ugly reuse of tokenizer token_val
        self.tk = jobsched_tokenizer()

    def close(self):
        should_be_implemented_in_subclass

    def get_pkt(self):
        """
        read some bytes after select says something
        is readable.
        shall not block (so you should call read_pkt
        only once).
        on return, all data available should be 
        in self.lines and self.partial_line.
        self.closed flag must be set iff there are no
        chance that more data will come.
        """
        pkt = self.readpkt(1024 * 1024)
        if pkt == "":
            self.close()
            self.closed = 1
        else:
            self.leftover = self.leftover + pkt

    def read_elements(self):
        self.server.LOG("read_elements:\n")
        lines = self.leftover.splitlines(1)
        elements = []
        e = []
        for line in lines:
            # Es("line : [%s]\n" % line)
            e.append(line)
            # handle continuation lines
            if line[-2:] != "\\\n" \
                    and line[-2:] != "\\\r" \
                    and line[-3:] != "\\\r\n" \
                    and (line[-1:] == "\n" or
                         line[-1:] == "\r"):  
                x = "".join(e)
                # Es("elements.append(%s)\n" % x)
                elements.append(x)
                e = []
        self.leftover = "".join(e)
        # this is the last bytes
        if self.closed and self.leftover != "":
            elements.append(self.leftover)
            self.leftover = ""
        self.server.LOG("read_elements: %d elements returned\n" 
                        % len(elements))
        return elements

    def translate_dir(self, cwd):
        trans = self.server.conf.trans_dirs
        for src,dsts in trans:
            # look for src that match dire
            # canonicalize src so both end with "/"
            if src[-1:] != os.path.sep: src = src + os.path.sep
            if cwd[-1:] != os.path.sep: cwd = cwd + os.path.sep
            n = len(src)
            if cwd[:n] == src:
                dirs = []
                for dst in dsts:
                    new_dir = os.path.normpath(os.path.join(dst, cwd[n:]))
                    # remove trailing "/"
                    if new_dir != os.path.sep and new_dir[-1:] == os.path.sep:
                        new_dir = new_dir[:-1]
                    dirs.append(new_dir)
                return dirs
        if cwd != os.path.sep and cwd[-1:] == os.path.sep:
            cwd = cwd[:-1]
        return [ cwd ]

    def translate_dirs(self, cwds):
        dirs = []
        for cwd in cwds:
            for d in self.translate_dir(cwd):
                dirs.append(d)
        return dirs

    def read_works(self):
        """
        assume data is ready in self.lines + self.partial_line
        """
        self.server.LOG("read_works:\n")
        self.get_pkt()
        elements = self.read_elements()
        works = []
        cmd = None
        pid = None
        dirs = []
        envs = {}
        requirement = { "cpu" : 1 }     # bare minimum default
        affinity = {}
        leftover_elements = []
        for element in elements:
            leftover_elements.append(element)
            kw = element[:4].lower()
            rest = element[4:].strip()
            if kw == "pid:":
                assert (pid is None), pid
                pid = rest
            elif kw == "cwd:":
                dirs.append(rest)
            elif kw == "env:":
                var_val = string.split(rest, "=", 1)
                assert (len(var_val) == 2), element
                [ var,val ] = var_val
                envs[var] = val
            elif kw == "req:":
                var_val = string.split(rest, "=", 1)
                assert (len(var_val) == 2), element
                [ var,val ] = var_val
                # FIXIT: ugly reuse of tokenizer
                requirement[var] = self.tk.token_val(val)
            elif kw == "aff:":
                var_val = string.split(rest, "=", 1)
                assert (len(var_val) == 2), element
                [ var,val ] = var_val
                # FIXIT: handle errors
                affinity[var] = re.compile(val)
            else:
                assert (cmd is None), cmd
                if kw == "cmd:":
                    cmd = rest
                else:
                    cmd = element.strip()
                # if no cwd: is given, supply the current directory
                if len(dirs) == 0: dirs.append(self.server.cwd)
                dirs_t = self.translate_dirs(dirs)
                w = Work().init(cmd, pid, dirs_t, envs, requirement, affinity)
                works.append(w)
                cmd = None
                pid = None
                dirs = []
                envs = {}
                requirement = { "cpu" : 1 }     # bare minimum default
                affinity = {}
                leftover_elements = []
        if self.closed:
            assert (self.leftover == ""), self.leftover
            if len(leftover_elements) > 0:
                assert cmd is None
                msg = ("warning: premature end of stream "
                       "(pid=%s, dirs=%s, envs=%s)\n" 
                       % (pid, dirs, envs))
                Es(msg)
                self.server.LOG(msg)
        else:
            self.leftover = "".join(leftover_elements) + self.leftover
        self.server.LOG("read_works: %d works retruned\n" % len(works))
        return works
    def finish_work(self, work_idx, work, exit_status, term_sig, man_name):
        return 0                        # OK

class work_stream_fd(work_stream_base):
    def init(self, fd):
        self.fd = fd
        return 0
    def close(self):
        assert self.fd is not None
        os.close(self.fd)
        self.fd = None
    def fileno(self):
        assert self.fd is not None
        return self.fd
    def readpkt(self, sz):
        assert self.fd is not None
        return os.read(self.fd, sz)
    
class work_stream_fd_pair(work_stream_fd):
    def init(self, rfd, wfd):
        work_stream_fd.init(self, rfd)
        self.wfd = wfd
        return 0                # OK

    def safe_write(self, fd, msg):
        try:
            os.write(fd, msg)
            return 0                    # OK
        except OSError,e:
            return -1
        except IOError,e:
            return -1

    def finish_work(self, work_idx, work, exit_status, term_sig, man_name):
        if exit_status is None: exit_status = "-"
        if term_sig is None: term_sig = "-"
        payload = "%d: %s %s %s\n" % (work_idx, exit_status, term_sig, man_name)
        msg = "%9d %s" % (len(payload), payload)
        if dbg>=2:
            Es("write notification to %d [%s]\n" % (self.wfd, msg))
        return self.safe_write(self.wfd, msg)
        
class work_stream_file(work_stream_base):
    def init(self, filename):
        self.filename = filename
        try:
            self.fp = open(filename)
            return 0
        except OSError,e:
            Es("error: could not open work_file %s %s\n"
               % (filename, e.args))
            return -1
    def close(self):
        assert self.fp is not None
        self.fp.close()
        self.fp = None
    def fileno(self):
        assert self.fp is not None
        return self.fp.fileno()
    def readpkt(self, sz):
        assert self.fp is not None
        return self.fp.read(sz)

class work_stream_socket(work_stream_base):
    def init(self, so):
        self.so = so
        self.warnings_issued = 0
        return 0
    def close(self):
        assert self.so is not None
        self.so.close()
        # self.so.shutdown(socket.SHUT_RD)
        # we may still write notifications
        self.so = None
    def fileno(self):
        assert self.so is not None
        return self.so.fileno()
    def readpkt(self, sz):
        assert self.so is not None
        return self.so.recv(sz)

class work_stream_socket_bidirectional(work_stream_socket):
    def safe_send(self, so, msg):
        try:
            self.so.send(msg)
            return 0                    # OK
        except socket.error,e:
            return -1                   # NG
            if self.warnings_issued == 0:
                Es("warning: could not send task termination notification %s, "
                   "probably the client program has gone\n" % (e.args,))
                self.warnings_issued = 1

    def finish_work(self, work_idx, work, exit_status, term_sig, man_name):
        if exit_status is None: exit_status = "-"
        if term_sig is None: term_sig = "-"
        payload = "%d: %s %s %s\n" % (work_idx, exit_status, term_sig, man_name)
        msg = "%9d %s" % (len(payload), payload)
        if dbg>=2:
            Es("write notification [%s]\n" % msg)
        return self.safe_send(self.so, msg)

class work_stream_generator(work_stream_base):
    def init(self, generator_module):
        self.generator_module = generator_module
        try:
            mod = __import__(generator_module, globals(), locals(), [], -1)
        except ImportError,e:
            Es("error: could not import module %s %s. did you set PYTHONPATH?\n"
               % (generator_module, e.args))
            return -1
        # FIXIT: exception
        try:
            gen = getattr(mod, "gen")
        except AttributeError,e:
            Es("failed to obtain generator function from module %s %s\n"
               % (generator_module, e.args))
            return -1
        try:
            f = gen()
        except Exception,e:
            Es("error while calling %s.gen():\n\n%s\n"
               % (generator_module, self.get_exception_trace()))
            return -1
        self.fun_generator = f
        if hasattr(mod, "fin"):
            self.fun_finish = getattr(mod, "fin")
        else:
            self.fun_finish = None
        # dummy, always readalble fileno
        r,w = os.pipe()
        os.close(w)
        self.readable = r
        r,w = os.pipe()
        self.unreadable = r
        self.unwritable = w
        self.cur_fileno = self.readable
        self.executing = {}     # work_idx -> whatever we got from generator
        return 0

    def close(self):
        for fd in [ self.readable, self.unreadable, self.unwritable ]:
            assert fd is not None
            os.close(fd)
        self.readable = None
        self.unreadable = None
        self.cur_fileno = None

    def fileno(self):
        assert self.cur_fileno is not None
        return self.cur_fileno

    def get_exception_trace(self):
        import cStringIO,traceback
        type,value,trace = sys.exc_info()
        cio = cStringIO.StringIO()
        traceback.print_exc(trace, cio)
        return cio.getvalue()

    def read_works(self):
        assert self.cur_fileno == self.readable, \
            (self.cur_fileno, self.readable, self.unreadable)
        s = self.server
        works = []
        for i in range(100):
            try:
                x = self.fun_generator.next()
            except StopIteration,e:
                self.close()
                self.closed = 1
                break
            except Exception,e:
                Es("Error while calling next() on %s.gen():\n\n%s\n"
                   % (self.generator_module, self.get_exception_trace()))
                self.close()
                self.closed = 1
                break
            if x is None:
                # mark this unreadable
                self.cur_fileno = self.unreadable
                break
            pid = None
            dirs = []
            envs = {}
            req = { "cpu" : 1 }
            aff = {}
            if type(x) is types.StringType:
                cmd = x
            else:
                if type(x) is types.DictType:
                    d = x
                else:
                    d = x.__dict__
                cmd = d["cmd"]
                pid = d.get("pid", pid)
                dirs = d.get("dirs", dirs)
                envs = d.get("envs", envs)
                req = d.get("req", req)
                aff = d.get("aff", aff)
            w = Work().init(cmd, pid, dirs, envs, req, aff)
            self.executing[w] = x
            works.append(w)
        return works
    def finish_work(self, work_idx, work, exit_status, term_sig, man_name):
        x = self.executing[work]
        del self.executing[work]
        # mark this readable
        self.cur_fileno = self.readable
        if self.fun_finish:
            try:
                self.fun_finish(x, exit_status, term_sig, man_name)
                return 0
            except Exception,e:
                Es("Error while calling fin() on %s.fin():\n\n%s\n"
                   % (self.generator_module, self.get_exception_trace()))
                self.close()
                self.closed = 1
                return -1
        else:
            return 0
        
class work_generator:
    """
    generate work from one or more work_streams
    """
    def __init__(self, conf, server):
        self.conf = conf
        self.server = server
        self.work_stream = {}  # live works work_idx -> stream
        self.streams = {}       # work_stream -> None
        self.server_socks = {}  # server_socket -> no. of accepts
        self.child_pipes = {}   # file descriptor -> (child_pid, associated server_socket/None)
        self.child_status = {}  # finished child_pid -> wait_status
        self.idx = 0            # serial no given to works
        self.max_exit_status = None
        self.max_term_sig = None

    def closed(self):
        if len(self.streams) > 0: return 0
        if len(self.server_socks) > 0: return 0
        if len(self.child_pipes) > 0: return 0
        return 1

    def add_work_stream(self, x):
        self.streams[x] = None

    def mk_tmp_socket_name(self):
        session = self.server.session_file
        cookie = "%d" % os.getpid()
        if session is None:
            dire,base = None,None
        else:
            dire,base = os.path.split(session)
            session_pat = re.compile("(G|g)xp-[^-]+-session-[^-]+-[^-]+-(.*)")
            m = session_pat.match(base)
            base = m.group(2)
        return os.path.join(dire, ("jobsched-%s-%s" % (cookie, base)))

    def add_server_sock(self, addr, n_accepts, bidirectional):
        if addr == "":
            addr = self.mk_tmp_socket_name()
        ss = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        ss.bind(addr)
        ss.listen(50000)                # FIXIT: make it configurable
        self.server_socks[ss] = (n_accepts, bidirectional)
        return ss

    def add_proc_pipe(self, cmd, outfd):
        """
        run 'cmd' with its outfd connected to me with a pipe.
        it also makes another pipe to notice its death.
        """
        x,y = os.pipe()         # pipe to detect his death
        r,w = os.pipe()         # pipe to receive job
        pid = os.fork()
        if pid == 0:
            cmdline = [ "/bin/sh", "-c", cmd ]
            os.close(x)
            os.close(r)
            os.dup2(w, outfd)
            os.execvp(cmdline[0], cmdline)
        else:
            os.close(y)
            os.close(w)
            s = work_stream_fd(self.server)
            if s.init(r) == 0:
                self.add_work_stream(s)
            else:
                bomb
            self.child_pipes[x] = (pid, None)

    def add_proc_pipe2(self, cmd, outfd, infd):
        """
        run 'cmd' with its outfd connected to me with a pipe.
        it also makes another pipe to notice its death.
        """
        x,y = os.pipe()         # pipe to detect his death
        r1,w1 = os.pipe()       # pipe to receive job
        r2,w2 = os.pipe()       # pipe to send notification
        pid = os.fork()
        if pid == 0:
            cmdline = [ "/bin/sh", "-c", cmd ]
            os.close(x)
            os.close(r1)
            os.close(w2)
            os.dup2(w1, outfd)
            os.dup2(r2, infd)
            os.execvp(cmdline[0], cmdline)
        else:
            os.close(y)
            os.close(w1)
            os.close(r2)
            s = work_stream_fd_pair(self.server)
            if s.init(r1, w2) == 0:
                self.add_work_stream(s)
            self.child_pipes[x] = (pid, None)

    def add_proc_sock(self, cmd, addr, n_accepts, bidirectional):
        return self.add_proc_sock_no_sh([ "/bin/sh", "-c", cmd ], addr, n_accepts, bidirectional)

    def add_proc_sock_no_sh(self, cmdline, addr, n_accepts, bidirectional):
        """
        similar to add_proc_pipe but communicate works
        via a new socket.
        run 'cmd' with its environment variable set 
        so it knows the socket address I am listening to.
        
        """
        ss = self.add_server_sock(addr, n_accepts, bidirectional)
        x,y = os.pipe()         # pipe to detect his death
        addr = ss.getsockname()
        pid = os.fork()
        if pid == 0:
            ss.close()
            os.close(x)
            # cmdline = [ "/bin/sh", "-c", cmd ]
            os.environ["GXP_JOBSCHED_WORK_SERVER_SOCK"] = str(addr)
            os.execvp(cmdline[0], cmdline)
        else:
            os.close(y)
            self.child_pipes[x] = (pid, ss)

    def read_works(self, ws):
        """
        read works from a work_stream
        """
        works = ws.read_works()
        if ws.closed:
            del self.streams[ws]
        t = self.server.cur_time()
        for w in works:
            # FIXIT: track which work came from which stream,
            # so we can call appropriate finish function
            idx = self.idx
            w.init2(idx, t, self.server)
            self.work_stream[idx] = ws,w
            self.idx = idx + 1
        return works

    def cleanup_server_sock(self, ss):
        """
        close server unix socket ss I should no longer listen to.
        also delete it from server_sock dictionary.
        also delete it from the file system.
        """
        addr = ss.getsockname()
        if os.path.exists(addr):
            os.remove(addr)
        ss.close()
        del self.server_socks[ss]

    def accept_connection(self, ss):
        """
        accept connection to receive works.
        create a new work_stream object out of
        the accepted connection.
        """
        n_accepts,bidirectional = self.server_socks[ss]
        so,_ = ss.accept()
        if bidirectional:
            ws = work_stream_socket_bidirectional(self.server)
        else:
            ws = work_stream_socket(self.server)
        if ws.init(so) == -1: bomb
        if 0:
            # do not wait for work at this point but
            # later in process_incoming_events.
            # pros: never block
            # cons: too many sockets in streams, and poll will
            # incur too much overhead?
            self.streams[ws] = None
            n_received = 0
        else:
            # wait for work now.
            # pros: do not have to manage too many sockets with poll
            # cons: may block (should not be an issue if the sender
            # immediately send msg after connecting)
            # FIXIT: we go back and forth between server and wkg...
            n_received = self.server.receive_works(self, ws)
        # close after the specified number of connections
        # have been accepted. 
        self.server_socks[ss] = (n_accepts - 1, bidirectional)
        if n_accepts == 1:
            self.cleanup_server_sock(ss)
        return n_received

    def reap_child(self, r):
        """
        called after we get EOF from file descriptor r.
        find pid and server sock if any associated with it
        and call wait on the pid, so he does not leave
        as a zombie.
        """
        pid,ss = self.child_pipes[r]
        qid,status = os.waitpid(pid, 0)
        assert (pid == qid), (pid, qid)
        os.close(r)
        del self.child_pipes[r]
        self.child_status[pid] = status
        # ss is a server socket I opened for him,
        # so it is no longer necessary.
        if ss and self.server_socks.has_key(ss):
            self.cleanup_server_sock(ss)

    def finish_work(self, work_idx, exit_status, term_sig, man_name):
        """
        this is the place where we should notify the client
        of the finished work.
        """
        ws,w = self.work_stream[work_idx]
        r = ws.finish_work(work_idx, w, exit_status, term_sig, man_name)
        self.max_exit_status = max(self.max_exit_status, exit_status)
        self.max_term_sig = max(self.max_term_sig, term_sig)
        return r

    def determine_final_status(self):
        """
        determine the exit status
        """
        exit_status = None
        term_sig = None
        for wait_status in self.child_status.values():
            if os.WIFEXITED(wait_status):
                exit_status = max(exit_status, os.WEXITSTATUS(wait_status))
            elif os.WIFSIGNALED(wait_status):
                term_sig = max(term_sig, os.WTERMSIG(wait_status))
            else:
                bomb
        if term_sig is not None:
            return (None, term_sig)
        if exit_status is not None:
            return (exit_status, None)
        if self.max_term_sig is not None:
            return (None, self.max_term_sig)
        if self.max_exit_status is not None:
            return (self.max_exit_status, None)
        # this happens when no work is dispatched
        return (None, None)

def set_make_environ(conf):
    makefiles = os.environ.get("MAKEFILES")
    xmake_mk = os.path.join(os.environ["GXP_DIR"],
                            os.path.join("gxpbin", "xmake2.mk"))
    if makefiles is None:
        os.environ["MAKEFILES"] = xmake_mk
    else:
        os.environ["MAKEFILES"] = "%s %s" % (xmake_mk, makefiles)
    os.environ["GXP_MAKELEVEL"] = "1"
    x = ("%d" % conf.make_exit_status_connect_failed)
    os.environ["GXP_MAKE_EXIT_STATUS_CONNECT_FAILED"] = x
    x = ("%d" % conf.make_exit_status_server_died)
    os.environ["GXP_MAKE_EXIT_STATUS_SERVER_DIED"] = x
    if conf.make_local_exec_cmd is not None:
        os.environ["GXP_MAKE_LOCAL_EXEC_CMD"] = conf.make_local_exec_cmd
    # set include files
    gxp_make_pp_inc = os.path.join(os.environ["GXP_DIR"],
                                   os.path.join("gxpmake", "gxp_make_pp_inc.mk"))
    gxp_make_mapred_inc = os.path.join(os.environ["GXP_DIR"],
                                       os.path.join("gxpmake", "gxp_make_mapred_inc.mk"))
    os.environ["GXP_MAKE_PP"] = gxp_make_pp_inc
    os.environ["GXP_MAKE_MAPRED"] = gxp_make_mapred_inc

def mk_work_generator(conf, server, make_args):
    wkg = work_generator(conf, server)
    # FIXIT. handle cases where some of them failed.
    # will require two step initializtion to avoid exception
    # handling here.
    # FIXIT. have a way to specify child procs (make in particular)
    for wf in conf.work_file:
        s = work_stream_file(server)
        if s.init(wf) == 0: wkg.add_work_stream(s)
    for mo in conf.work_py_module:
        s = work_stream_generator(server)
        if s.init(mo) == 0: wkg.add_work_stream(s)
    for fd in conf.work_fd:
        s = work_stream_fd(server)
        if s.init(fd) == 0: wkg.add_work_stream(s)
    # FIXIT eliminate those hardwired numbers
    for cmd in conf.work_proc_pipe:
        wkg.add_proc_pipe(cmd, 1)
    for cmd in conf.work_proc_pipe2:
        wkg.add_proc_pipe2(cmd, 1, 0)
    for cmd in conf.work_proc_sock:
        wkg.add_proc_sock(cmd, "", float("inf"), 0) # bidirectional = no
    for cmd in conf.work_proc_sock2:
        wkg.add_proc_sock(cmd, "", float("inf"), 1) # bidirectional = yes
    if make_args is not None:
        make_cmdline = [ conf.make_cmd ] + make_args
        set_make_environ(conf)
        wkg.add_proc_sock_no_sh(make_cmdline, "", float("inf"), 1) # bidirectional = yes
    for addr in conf.work_server_sock:
        wkg.add_server_sock(addr, 1, 0) # bidirectional = no
    for addr in conf.work_server_sock2:
        wkg.add_server_sock(addr, 1, 1) # bidirectional = yes
    return wkg

class work_db_base:
    """
    work database. the idea is this database, per se, 
    is independent from the execution of the job.
    it exists only to generate database/csv file/html
    """
    def __init__(self, conf):
        """
        dire : directory work_db is in
        task_db : base name of task/run database
        """
        self.conf = conf
        # necessary?
        self.limit = conf.work_list_limit
        # list of (name, sort key)
        self.table_spec = [
            # some most early ended failed runs
            ("failed", self.run_failed_order_by_time_end),
            # some longest runs (finished or running)
            ("long",   self.run_started_order_by_rev_time_since_start),
            # some most recently started runs
            ("recent", self.run_started_order_by_rev_time_start)
            ]
        self.table_spec_dict = {}
        for name,sort_key in self.table_spec:
            self.table_spec_dict[name] = sort_key

    def run_failed_order_by_time_end(self, run):
        # queued or running runs have never failed
        if run.status == run_status.queued: return None
        if run.status == run_status.running: return None
        # succeeded
        if run.exit_status == 0: return None
        return run.time_end

    def run_started_order_by_rev_time_since_start(self, run):
        if run.time_since_start is None: return None
        return -run.time_since_start

    def run_started_order_by_rev_time_start(self, run):
        return run.time_start

    def add_work(self, work):
        """
        add a new work object
        """
        pass
    
    def add_run(self, work_idx, run):
        """
        add a new run for work object (indexed by work_idx)
        """
        pass
    
    def update_run(self, run):
        """
        reflect the fact that run has been updated
        """
        pass
    
    def commit(self):
        pass
    
    def __len__(self):
        return 0
    
    def list_such_runs(self, such):
        if 0: yield None

class work_db_none(work_db_base):
    """
    database that only counts the number of works.
    no jobs appear in html page. only used to see the overhead
    of other schemes.
    """
    def __init__(self, conf):
        work_db_base.__init__(self, conf)
        self.db_file = None
        self.n_works = 0
    def add_work(self, work):
        self.n_works = self.n_works + 1
    def __len__(self):
        return self.n_works
        
class work_db_naive_mem(work_db_base):
    """
    database that keeps every jobs on memory.
    it grows memory indefinitely, so should be avoided
    in large runs. not very useful any more because 
    we have a simply-better smart_mem.
    """
    def __init__(self, conf):
        work_db_base.__init__(self, conf)
        self.db_file = None
        self.works = []         # list of works
        self.work_runs = {}     # work idx -> list of run_idxs

    def add_work(self, work):
        """
        work : Work object
        """
        self.works.append(work)
        self.work_runs[work.work_idx] = []

    def add_run(self, work_idx, run):
        """
        work_idx : index of work the run is associated with
        run : run object
        """
        self.work_runs[work_idx + 0].append(run)

    def __len__(self):
        return len(self.works)

    def list_such_runs(self, such):
        """
        list self.limit most early 
        failed/longest/most recently started runs
        """
        if dbg>=2:
            Es("list_such_runs(%s):\n" % such)
        sort_key = self.table_spec_dict[such]
        if dbg>=2:
            Es(" sort_key=%s\n" % sort_key)
        work_runs = []
        for w in self.works:
            for r in self.work_runs[w.work_idx]:
                k = sort_key(r)
                if dbg>=2:
                    Es("  work=%s run=%s -> key=%s\n" % (w, r, k))
                if k is not None:
                    work_runs.append((k, w, r))
        n = len(work_runs)
        work_runs.sort()
        work_runs = work_runs[:self.limit]
        if dbg>=2:
            Es(" %d work_runs made %d elems\n" % (n, len(work_runs)))
        for _,w,r in work_runs:
            yield (w, r)

class work_db_text(work_db_base):
    """
    this is a class that keeps only jobs that may appear in
    html in memory and throw away other jobs. more specifically,
    it keeps the following on memory.
    - 100 longest jobs among finished jobs
    - 100 failed and oldest jobs among finished jobs
    - 100 newest jobs among finished jobs
    - jobs not finished yet (running and queued).

    queued jobs never appear in html page, but they are on 
    memory anyways, so we do not bother to avoid having them on
    memory.
    """
    def __init__(self, conf):
        work_db_base.__init__(self, conf)
        self.n_works = 0
        # works having still queued or running runs, plus
        # works having runs referenced in fin_*_runs
        # (finished but long/failed/recent)
        self.works = []      
        # for work in works, track its run idxs
        # work idx -> list of run_idxs
        self.work_runs = {}

        # keep runs we cannot write to disks yet
        # for each category
        for name,sort_key in self.table_spec:
            self.such_runs[name] = ([], sort_key)

        self.wp = self.open_work_db()

    def open_work_db(self):
        wp = open(os.path.join(self.conf.state_dir, "work.txt"))
        wp.write("\t".join(Run.db_fields + Work.db_fields))
        return wp

    def add_work(self, work):
        """
        work : Work object
        this is a new work, so we simply keep it on memory.
        """
        self.n_works = self.n_works + 1
        self.works.append(work)
        self.work_runs[work.work_idx] = []

    def add_run(self, work_idx, run):
        """
        work_idx : index of work the run is associated with
        run : run object.
        similar to add_work, this is a new run, so
        keep it on memory for now.
        """
        self.work_runs[work_idx].append(run)

    def __len__(self):
        return self.n_works

    def push_with_limit(self, h, x, limit):
        """
        h is a heap, holding upto LIMIT LARGEST items,
        with SMALLEST items FIRST. 
        this is a heap structure to keep a constant
        number of largest items.
        """
        if len(h) < limit:      # heap is small so we insert it
            heapq.heappush(h, x)
            return 1
        elif x > h[0]:          # heap is already the maximum length
            assert (len(h) == limit), len(h)
            heapq.heapreplace(h, x)
            return 1                    # insert it
        else:
            return 0

    def all_runs_finished(self, work):
        for run in self.work_runs[work.work_idx]:
            if not run.is_finished(): return 0
        return 1

    def mk_work_run_text_row(self, work, run):
        work_row = adapt_to_text(work, Work.fields)
        run_row = adapt_to_text(run, Run.fields)
        return "\t".join(work_row + run_row)

    def update_lists(self):
        """
        this is a preprocessing step before generating html page.
        we need to get 100 longest jobs among BOTH finished and RUNNING
        jobs.
        we keep only finished jobs in heaps and not runninjobs, 
        because running jobs change their age (e.g., time_since_start), 
        so their positions in heap will keep changing.
        thus, we compute them by merging the list of finished jobs
        and running ones.
        """
        new_works = []
        new_work_runs = {}
        # iterate over jobs that were running/queued when we saw them 
        # the last time, and move jobs that are now finished to
        # appropriate heaps.
        # as a result, some works will be dropped off works list.
        # then corresponding entries in work_runs need to be 
        # freed as well. we actually accomplish this gc by rebuilding
        # new work_runs structure that keep still-necessary entries.
        for w in self.works:
            # check if all runs are finished
            # yes -> keep only runs that are long/failed/recent
            # in appropriate list and discard others.
            # no -> it reenters works and work_runs 
            if self.all_runs_finished(w):
                for r in self.work_runs[w.work_idx]:
                    self.wp.write("%s\n" % self.mk_work_run_text_row(w, r))
                    # r should have finished.
                    # move it to longest/failed/recent heaps
                    # as appropriate
                    assert r.time_since_start is not None
                    assert r.time_start is not None
                    assert r.time_end is not None
                    assert (r.status != run_status.queued), r.status
                    assert (r.status != run_status.running), r.status
                    for name,(runs,sort_key) in self.such_runs.items():
                        k = sort_key(r)
                        if k is not None:
                            self.push_with_limit(runs, (k, w, r), self.limit)
            else:
                # w is still running or queued
                new_works.append(w)
                new_work_runs[w.work_idx] = self.work_runs[w.work_idx]
        # copy entries for finished works that are necessary
        for _,(runs,_) in self.such_runs.items():
            for k,w,r in runs:
                new_work_runs[w.work_idx] = self.work_runs[w.work_idx]
        self.works = new_works
        self.work_runs = new_work_runs
        self.wp.flush()

    def commit(self):
        self.update_lists()

    def list_such_runs(self, such):
        """
        such : "long", "failed", "recent"
        """
        # assume commit was just called
        runs,sort_key = self.such_runs[such]
        h = runs[:]             # make a copy of the heap
        for w in self.works:
            for r in self.work_runs[w.work_idx]:
                k = sort_key(r)
                if k is not None:
                    self.push_with_limit(h, (k, w, r), self.limit)
        results = []
        while len(h) > 0:
            _,w,r = heapq.heappop(h)
            results.insert(0, (w, r))
        for w,r in results:
            yield (w, r)


def mk_work_db(conf):
    return work_db_naive_mem(conf)

class time_series_data:
    def __init__(self, server, directory, file_prefix, 
                 section_title, graph_title, line_spec, line_titles):
        self.server = server
        self.directory = directory
        self.file_prefix = file_prefix
        self.section_title = section_title
        self.graph_title = graph_title
        self.line_spec = line_spec
        self.line_titles = line_titles
        self.last_t = 0.0
        self.last_x = (0.0,) * len(line_titles)
        dim = len(line_titles)
        self.template = (" %f" * dim) + "\n"
        self.wp = None

    def ensure_dat(self):
        if self.wp is None and self.directory != "":
            dat = os.path.join(self.directory, "%s.dat" % self.file_prefix)
            self.wp = open(dat, "wb")
            self.wp.write("0.0")
            self.wp.write(self.template % self.last_x)
        if self.wp:
            return 1
        else:
            return 0
        
    def refresh(self, t):
        if self.ensure_dat():
            self.wp.write("%f" % t)
            self.wp.write(self.template % self.last_x)
        self.last_t = t

    def add_x(self, t, i, x):
        new_x = self.last_x[:i] + (x,) + self.last_x[i+1:]
        if self.ensure_dat():
            self.wp.write("%f" % t)
            self.wp.write(self.template % self.last_x)
            self.wp.write("%f" % t)
            self.wp.write(self.template % new_x)
        self.last_t = t
        self.last_x = new_x

    def add_dx(self, t, i, dx):
        self.add_x(t, i, self.last_x[i] + dx)

    def sync(self):
        if self.ensure_dat():
            self.wp.flush()

    def close(self):
        if self.ensure_dat():
            self.wp.close()
        
    def run_gnuplot(self):
        # make sure the graph reflects current time
        dir = self.directory
        if dir == "": return 0
        self.refresh(self.server.cur_time())
        self.sync()
        pre = self.file_prefix
        dat = "%s.dat" % pre
        gpl = "%s.gpl" % pre
        err = "%s.err" % pre
        png = "%s.png" % pre
        tmp = "%s.tmp" % pre
        full_gpl = os.path.join(dir, gpl)
        full_err = os.path.join(dir, err)
        gp = open(full_gpl, "wb")
        gp.write("""set terminal png
set style fill solid
set ylabel "%s"
""" % self.graph_title)
        gp.write('plot ')
        for i in range(len(self.line_titles)):
            if i > 0: gp.write(', ')
            gp.write(('"%s" using 1:%d title "%s" with %s'
                      % (dat, i + 2, self.line_titles[i], self.line_spec)))
        gp.write('\n')
        gp.close()
        cmd = ("cd %s && gnuplot %s 2> %s > %s && mv %s %s" 
               % (dir, gpl, err, tmp, tmp, png))
        status = os.system(cmd)
        fp = open(full_err, "rb")
        msg = fp.read()
        fp.close()
        self.server.LOG("'%s' status = %s\n" % (cmd, status))
        if msg != "":
            self.server.LOG("stderr :\n%s" % msg)
        return status

class html_generator:
    def __init__(self, conf, server):
        self.conf = conf
        self.server = server

    def mk_td_elem(self, v):
        if v is None: return "-"
        t = type(v) 
        if t is types.IntType or t is types.LongType:
            return "%d" % v
        if t is types.FloatType:
            return "%.3f" % v
        v = str(v)
        v = string.replace(v, "<", "&lt;")
        v = string.replace(v, ">", "&gt;")
        v = string.replace(v, "\n", "<br>")
        return v

    def mk_header_row(self, columns):
        R = []
        for c in columns:
            R.append("<td>%s</td>" % self.mk_td_elem(c))
        return "".join(R)
        
    def mk_object_row(self, columns, obj, rowspan):
        if type(obj) is types.DictType:
            d = obj
        else:
            d = obj.__dict__
        R = []
        if dbg>=2:
            Es("generating columns of object %s\n" % d)
        for c in columns:
            if dbg>=2:
                Es(" column: %s:\n" % c)
            cls = None
            m = None
            # m = d.get("get_td_%s" % c)
            if hasattr(obj, ("get_td_%s" % c)):
                m = getattr(obj, ("get_td_%s" % c))
                if dbg>=2:
                    Es("  use method: %s:\n" % m)
                v = m()
                if type(v) is types.TupleType:
                    cls,v = v
                if type(v) is not types.StringType:
                    v = self.mk_td_elem(v)
            else:
                if dbg>=2:
                    Es("  no method, use field\n")
                v = self.mk_td_elem(d.get(c, "???"))
            if dbg>=2:
                Es(" -> cls=%s value=%s\n" % (cls, v))
            if cls is None:
                cls = ""
            else:
                cls = ' class="%s"' % cls
            if rowspan is None:
                R.append("<tr><td>%s</td><td%s>%s</td></tr>\n" % (c, cls, v))
            elif rowspan == 1:
                R.append("<td%s>%s</td>" % (cls, v))
            else:
                R.append("<td%s rowspan=%d>%s</td>" % (cls, rowspan, v))
        return "".join(R)

    def mk_basic_table(self):
        return self.mk_object_row(job_scheduler.db_fields, self.server, None)

    def mk_conf_table(self):
        return self.mk_object_row(jobsched_config.db_fields,
                                  self.server.conf, None)

    def mk_men_table(self):
        """
        generate worker state
        """
        men = self.server.men.items()
        men.sort()
        R = []
        R.append("<tr>%s</tr>\n" % self.mk_header_row(Man.db_fields + [ "cmd" ]))
        for name,man in men:
            rowspan = max(1, len(man.runs_running))
            m_row = self.mk_object_row(Man.db_fields, man, rowspan)
            for run in man.runs_running.values():
                R.append("<tr>%s<td>%s</td></tr>\n" % (m_row, self.mk_td_elem(run.work.cmd)))
                m_row = ""
            if m_row != "":
                R.append("<tr>%s<td>-</td></tr>\n" % m_row)
        return "".join(R)

    def mk_work_run_table(self, such):
        R = []
        w_header_row = self.mk_header_row(Work.db_fields)
        r_header_row = self.mk_header_row(Run.db_fields)
        R.append("<tr>%s%s</tr>\n" % (w_header_row, r_header_row))
        for w,r in self.server.works.list_such_runs(such):
            w_row = self.mk_object_row(Work.db_fields, w, 1)
            r_row = self.mk_object_row(Run.db_fields, r, 1)
            R.append("<tr>%s%s</tr>\n" % (w_row, r_row))
        return "".join(R)

    def expandpath(self, p):
        return os.path.expanduser(os.path.expandvars(p))

    def generate(self, finished):
        # FIXIT: need to generate how much time is spent on it
        for ts in self.server.ts.values(): 
            ts.run_gnuplot()
        self.server.works.commit()
        D = {}
        D["basic_info_table"] = self.mk_basic_table()
        D["conf_table"] = self.mk_conf_table()
        for such,_ in self.server.works.table_spec:
            D["%s_jobs_table" % such] = self.mk_work_run_table(such)
        D["workers_table"] = self.mk_men_table()
        html = os.path.join(self.conf.state_dir, "index.html")
        html_t = os.path.join(self.conf.state_dir, "index.html.tmp")
        template_html = self.expandpath(self.conf.template_html)
        wp = open(html_t, "wb")
        fp = open(template_html, "rb")
        template = fp.read()
        fp.close()
        # Es("%s\n" % D)
        wp.write(template % D)
        wp.flush()
        wp.close()
        os.rename(html_t, html)

class job_scheduler(gxpc.cmd_interpreter):
    def ensure_directory(self, dire):
        if dire == "": return 0
        try:
            os.mkdir(dire)
        except OSError,e:
            if e.args[0] == errno.EEXIST:
                pass
            else:
                raise
        if os.path.isdir(dire): return 0
        Es("error: could not make a state directory %s\n" % dire)
        return -1

    def open_LOG(self):
        """
        open log file for writing
        return -1 on failure, 0 on success
        """
        if self.conf.state_dir == "": return 0
        if self.conf.log_file == "": return 0
        log = os.path.join(self.conf.state_dir, self.conf.log_file)
        try:
            self.logfp = open(log, "wb")
        except Exception,e:
            Es("%s: %s : %s\n" % (self.prog, log, e.args,))
            return -1
        if self.logfp:
            self.LOG("started at %.3f\n" % time.time())
        return 0

    def close_LOG(self):
        """
        open log file
        """
        if self.logfp:
            self.LOG("closing log\n")
            self.logfp.close()
            self.logfp = None
            
    def LOG(self, s):
        """
        write s to log
        """
        logfp = self.logfp
        if logfp:
            t = "%s: %.3f: %s" % (self.prog, self.cur_time(), s)
            if dbg>=2: Es(t)
            logfp.write(t)
            logfp.flush()

    def check_time_limit(self):
        pass

    def check_interrupted(self):
        for i in range(self.interrupts_seen, self.interrupted):
            if i == 0:
                time.sleep(0.5)
                Es("\nwarning: *** GXP got 1st interrupt, waiting for running jobs to finish\n")
                Es("Hit Ctrl-C once more to send them signals (SIGINT)\n")
            elif i == 1:
                Es("\nwarning: *** GXP got 2nd interrupt, trying to kill running jobs with SIGINT\n")
                self.send_sigs_to_running_jobs()
            elif i == 2:
                Es("\nwarning: *** GXP got 3rd interrupt, GXP will finish, possibly with jobs on remote hosts running\n")
            else:
                assert (i < 3), i
        self.interrupts_seen = self.interrupted

    def get_loadavg(self):
        fp = os.popen("uptime")
        result = fp.read()
        fp.close()
        m = re.search("load average: ([^,]*),", result)
        if m is None: return -1
        try:
            return float(m.group(1))
        except ValueError,e:
            return -1

    def record_loadavg(self, cur_time, force):
        loadavg = self.ts["loadavg"]
        last_t = loadavg.last_t
        if force or last_t == 0.0 or cur_time > last_t + 10.0:
            loadavg.add_x(cur_time, 0, self.get_loadavg())

    def get_self_rss(self):
        fp = os.popen("ps h -o rss -p %d" % os.getpid())
        result = string.strip(fp.read())
        fp.close()
        try:
            result = float(result)
        except ValueError,e:
            return -1
        return result / 1024.0

    def record_rss(self, cur_time, force):
        rss = self.ts["rss"]
        last_t = rss.last_t
        if force or last_t == 0.0 or cur_time > last_t + 10.0:
            rss.add_x(cur_time, 0, self.get_self_rss())

    def get_mem(self):
        fp = os.popen("free")
        result = fp.read()
        fp.close()
        m = re.search("Mem:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)", 
                      result)
        if m is None: return (-1,-1,-1,-1,-1,-1)
        try:
            # make it MB
            return map((lambda s: float(s) / 1024.0), 
                       m.group(1,2,3,4,5,6))
        except ValueError,e:
            return (-1,-1,-1,-1,-1,-1)
        

    def record_mem(self, cur_time, force):
        mem = self.ts["mem"]
        last_t = mem.last_t
        if force or last_t == 0.0 or cur_time > last_t + 10.0:
            total,used,free,shared,buffers,cached = self.get_mem()
            mem.add_x(cur_time, 0, total)
            mem.add_x(cur_time, 1, used)
            mem.add_x(cur_time, 2, free)
            mem.add_x(cur_time, 3, shared)
            mem.add_x(cur_time, 4, buffers)
            mem.add_x(cur_time, 5, cached)

    def generate_html(self, cur_time, finished):
        if finished or cur_time > self.next_update_time:
            for run in self.runs_running.values():
                run.sync(cur_time)
            self.htmlg.generate(finished)
            return 1
        else:
            return 0

    def record_everything(self, force):
        t0 = self.cur_time()
        self.record_rss(t0, force)
        self.record_mem(t0, force)
        self.record_loadavg(t0, force)
        if self.generate_html(t0, force):
            dt = self.cur_time() - t0
            overhead = self.conf.gen_html_overhead
            if overhead <= 0.0:
                time_until_next = float("inf")
            else:
                time_until_next = dt / overhead
            self.next_update_time = self.next_update_time + time_until_next
            self.gen_html_time = dt


    def receive_works(self, wkg, ws):
        self.LOG("receive_works from %s\n" % ws)
        works = wkg.read_works(ws)
        n_received = len(works)
        self.LOG("received %d works from %s\n" % (n_received, ws))
        for w in works:
            self.works.add_work(w)
            w.make_run()
        if n_received > 0:
            self.ts["run_counts"].add_dx(self.cur_time(), 0, n_received)
        return n_received

    def finish_run(self, gupid, run):
        man = self.men[gupid]
        ct = self.cur_time()
        self.LOG("run[%d,%d] (%s) finished by %s\n" 
                 % (run.work_idx, run.run_idx, run.work.cmd, run.man_name))
        # not quite right when we handle io
        # FIXIT:
        rid = "run_%d_%d" % (run.work_idx, run.run_idx)
        del self.runs_running[rid]
        del man.runs_running[rid]
        self.ts["parallelism"].add_dx(ct, 0, -1)
        self.ts["run_counts"].add_dx(ct, 2, 1) # finished_runs += 1
        self.n_events = self.n_events + 1
        # return resource left for the man
        man.modify_resource(run.work.requirement, 1)
        # FIXIT: slow
        if man not in self.men_free and man.state == man_state.active:
            self.men_free.append(man)
        return run.finish(ct)

    def handle_event_io_run(self, gupid, tid, ev):
        if self.logfp:
            self.LOG("handle_event_io_run"
                     "(gupid=%s, tid=%s, ev.fd=%s, ev.kind=%s)\n" 
                     % (gupid, tid, ev.fd, ev.kind))
        run = self.runs_running[ev.rid]
        if ev.kind == ioman.ch_event.OK:
            eof = 0
        else:
            eof = 1
        run.add_io(ev.fd, ev.payload, eof)
        if run.is_finished(): 
            self.finish_run(gupid, run)
        self.works.update_run(run)
        
    def handle_event_die_run(self, gupid, tid, ev):
        if self.logfp:
            self.LOG("handle_event_die_run"
                     "(gupid=%s, tid=%s, ev.rid=%s ev.status=%s)\n" 
                     % (gupid, tid, ev.rid, ev.status))
        run = self.runs_running[ev.rid]
        worker_time_start = ev.time_start - self.time_start
        worker_time_end = ev.time_end - self.time_start
        run.record_die(ev.status, ev.rusage, 
                       worker_time_start, worker_time_end)
        if run.is_finished(): 
            self.finish_run(gupid, run)
        self.works.update_run(run)

    def ensure_man(self, gupid):
        """
        ensure we have a man of gupid
        """
        man = self.men.get(gupid)
        if man is None:
            man = self.mang.make_man(gupid)
            self.men[gupid] = man
        return man

    def check_completed(self, man, rid):
        if man.completed(rid):

            ### think twice if we should do it many times
            ### we may break capacity_left??
            ### also think twice how to maintain men_free

            man.finalize_capacity(self.conf, rid)
            assert man not in self.men_free
            self.men_free.append(man)
            if self.logfp:
                self.LOG("a man initialized\n%s\n" % man)
        else:
            if self.logfp:
                self.LOG("man %s not completed yet\n" % man.name)

    def handle_event_io_join(self, gupid, tid, ev):
        """
        received IO result of hello message
        """
        if self.logfp:
            self.LOG("handle_event_io_join"
                     "(gupid=%s,tid=%s,ev.fd=%s,ev.kind=%s)\n" 
                     % (gupid, tid, ev.fd, ev.kind))
        man = self.ensure_man(gupid)
        if ev.kind == ioman.ch_event.OK:
            eof = 0
        else:
            eof = 1
        man.record_io(ev.rid, ev.fd, ev.payload, eof)
        self.check_completed(man, ev.rid)

    def handle_event_die_join(self, gupid, tid, ev):
        """
        received a notification of hello message
        """
        if self.logfp:
            self.LOG("handle_event_die_join"
                     "(gupid=%s,tid=%s,ev.rid=%s,ev.status=%s)\n" 
                     % (gupid, tid, ev.rid, ev.status))
        man = self.ensure_man(gupid)
        man.record_die(ev.rid, ev.status)
        self.check_completed(man, ev.rid)

    def handle_event_io_leave(self, gupid, tid, ev):
        """
        received IO result of hello message
        """
        if self.logfp:
            self.LOG("handle_event_io_leave"
                     "(gupid=%s,tid=%s,ev.fd=%s,ev.kind=%s)\n" 
                     % (gupid, tid, ev.fd, ev.kind))
        # nothing to do here

    def handle_event_die_leave(self, gupid, tid, ev):
        """
        received a notification of hello message
        """
        if self.logfp:
            self.LOG("handle_event_die_leave"
                     "(gupid=%s,tid=%s,ev.rid=%s,ev.status=%s)\n" 
                     % (gupid, tid, ev.rid, ev.status))
        man = self.men.get(gupid)
        if man:
            if self.logfp:
                self.LOG("man %s leaving after current job (if any)\n" 
                         % gupid)
            man.state = man_state.leaving

    def handle_event_io(self, gupid, tid, ev):
        if self.logfp:
            self.LOG("handle_event_io"
                     "(gupid=%s,rid=%s,fd=%s,kind=%s,payload=%s)\n" 
                     % (gupid, ev.rid, ev.fd, ev.kind, ev.payload))
        rid = ev.rid
        if rid.startswith("run_"):
            self.handle_event_io_run(gupid, tid, ev)
        elif rid.startswith("join_"):
            self.handle_event_io_join(gupid, tid, ev)
        elif rid.startswith("leave_"):
            self.handle_event_io_leave(gupid, tid, ev)
        else:
            Es("rid = %s!!\n" % rid)
            bomb
            
    def handle_event_die(self, gupid, tid, ev):
        if self.logfp:
            self.LOG("handle_event_die(gupid=%s,rid=%s,status=%s)\n" 
                     % (gupid, ev.rid, ev.status))
        rid = ev.rid
        if rid.startswith("run_"):
            self.handle_event_die_run(gupid, tid, ev)
        elif rid.startswith("join"):
            self.handle_event_die_join(gupid, tid, ev)
        elif rid.startswith("leave"):
            self.handle_event_die_leave(gupid, tid, ev)
        else:
            Es("rid = %s!!\n" % rid)
            bomb
            
    def select_by_select(self, R, W, E, T):
        if T is None:
            return select.select(R, W, E)
        else:
            return select.select(R, W, E, T)

    def select_by_poll(self, R, W, E, T):
        d = {}
        for f in R:
            if type(f) is types.IntType:
                fd = f
            else:
                fd = f.fileno()
            d[fd] = select.POLLIN
        for f in W:
            if type(f) is types.IntType:
                fd = f
            else:
                fd = f.fileno()
            d[fd] = (d.get(fd, 0) | select.POLLOUT)
        for f in E:
            if type(f) is types.IntType:
                fd = f
            else:
                fd = f.fileno()
            d[fd] = (d.get(fd, 0) | select.POLLIN | select.POLLOUT)
        p = select.poll()
        for fd, mask in d.items():
            p.register(fd, mask)
        if T is None or T == float("inf"):
            poll_result = p.poll()
        else:
            poll_result = p.poll(int(T*1000))
        R0 = []
        W0 = []
        E0 = []
        d = dict(poll_result)
        for f in R:
            if type(f) is types.IntType:
                fd = f
            else:
                fd = f.fileno()
            if (d.get(fd, 0) & (select.POLLIN|select.POLLHUP)) != 0:
                R0.append(f)
        for f in W:
            if type(f) is types.IntType:
                fd = f
            else:
                fd = f.fileno()
            if (d.get(fd, 0) & (select.POLLOUT)) != 0:
                W0.append(f)
        for f in E:
            if type(f) is types.IntType:
                fd = f
            else:
                fd = f.fileno()
            if (d.get(fd, 0) & select.POLLERR) != 0:
                E0.append(f)
        return R0,W0,E0

    def select(self, R, W, E, T):
        return self.select_by_select(R, W, E, T)

    def process_incoming_events(self, timeout):
        """
        process incoming events.
        an event is either from gxp daemon notifying
        us of an IO or a process termination (the latter
        critically important), or from the work generator
        object sending work to us.
        """
        self.LOG("process_incoming_events: timeout=%f\n" % timeout)
        assert self.so
        wkg = self.wkg
        streams = wkg.streams.keys()
        server_socks = wkg.server_socks.keys()
        child_pipes = wkg.child_pipes.keys()
        R_ = []
       
        R_.extend(streams)
        R_.extend(server_socks)
        R_.extend(child_pipes)
        if self.so: R_.append(self.so)
        self.LOG("checking %d streams, %d server_socks, and %d child_pipes\n"
                 % (len(streams), len(server_socks), len(child_pipes)))
        R,_,_ = self.select(R_, [], [], timeout)
        n_received = 0
        for r in R:
            if r is self.so:
                # process_msg_from_deamon (in gxpc.py),
                # which eventually calls one of these
                # handle_event_xxx.  we overwrite
                # handle_event_die (see above)
                self.process_msg_from_daemon(self.so)
            elif r in wkg.streams:
                n_received = n_received + self.receive_works(wkg, r)
            elif r in wkg.server_socks:
                n_received = n_received + wkg.accept_connection(r)
            elif r in wkg.child_pipes:
                wkg.reap_child(r)
            else:
                assert 0
        return n_received

    def make_matches(self):
        if self.logfp:
            self.LOG("make_matches: %d runs todo %d men free\n" 
                     % (len(self.runs_todo), len(self.men_free)))
        matches_found = 0
        new_men_free = []
        # continue working as long as there is a free man 
        # and a ready task
        while len(self.runs_todo) > 0 and len(self.men_free) > 0:
            # get a man and a run from the head of the queues
            run = self.runs_todo[0]
            man = self.men_free[0]
            # see if this man can exec this run by checking 
            # the resource
            if self.logfp:
                self.LOG("make_matches: check if %s can exec %s\n" 
                         % (man, run))
            if man.state != man_state.active:
                if self.logfp:
                    self.LOG("make_matches: no, %s is not active any more\n" 
                             % man)
                self.men_free.pop(0)
                continue
            elif not man.has_affinity(run.work.affinity) \
                   or not man.has_resource(run.work.requirement):
                if self.logfp:
                    self.LOG("make_matches: no, %s does not have resource or affinity for %s\n" 
                             % (man, run))
                self.men_free.pop(0)
                # FIXIT: almost wrong.  even though this man may be able to
                # accommodate other jobs, we forget this man as long as
                # one job is running on it.
                if len(man.runs_running) == 0:
                    new_men_free.append(man)
                continue
            assert (man.state == man_state.active), man.state
            run.man_name = man.name  # assign man to run
            matches_found = matches_found + 1
            man.modify_resource(run.work.requirement, -1)
            if self.logfp:
                self.LOG("yes, %s will exec %s\n" % (man, run))
            if man not in self.matches:
                self.matches[man] = []
            self.matches[man].append(run)
            self.n_pending_matches = self.n_pending_matches + 1
            self.runs_todo.pop(0)
        self.men_free.extend(new_men_free)
        if self.logfp:
            self.LOG("%s : %d matches found\n" 
                     % (self.prog, matches_found))

    def mk_target_tree_rec(self, ptree, gupids):
        """
        make a subtree of ptree, containing gupids
        """
        if ptree is None: return None
        if ptree.name is None: return None
        child_trees = []
        for child in ptree.children.values():
            t = self.mk_target_tree_rec(child, gupids)
            if t is not None:
                child_trees.append(t)
        if ptree.name in gupids:
            del gupids[ptree.name]
            return gxpm.target_tree(ptree.name, ptree.hostname, 
                                    ptree.target_label,
                                    1, None, ptree.eenv, child_trees)
        elif len(child_trees) > 0:
            return gxpm.target_tree(ptree.name, ptree.hostname, 
                                    ptree.target_label,
                                    0, None, ptree.eenv, child_trees)
        else:
            return None


    def mk_target_tree_1(self, clauses):
        """
        gupids : dictionary having gupids as keys
        """
        gupids = {}
        for g in clauses.keys(): 
            gupids[g] = None
        t = self.mk_target_tree_rec(self.session.peer_tree, gupids)
        if len(gupids) == 0:
            return t
        else:
            return None

    def mk_target_tree(self, clauses):
        for i in range(2):
            t = self.mk_target_tree_1(clauses)
            if t: return t
            if self.logfp:
                self.LOG("%s : reloading session file %s\n" 
                         % (self.prog, self.session_file))
            self.session
            self.session = self.reload_session(self.session_file, 1)
        assert 0

    def send_clauses(self, tgt, tid, clauses, persist, keep_connection):
        """
        do action (act) on some nodes.
        """
        if self.opts.verbosity>=2:
            Es("gxpc: send clauses %s to daemon %s persist=%d keep_connection=%d\n"
               % (clauses, tgt, persist, keep_connection))
        gcmds = [ clauses ]
        assert tgt is not None
        assert tid is not None
        if tgt is None: return None
        if tid is None:
            tid = "t%s" % self.session.gen_random_id()
        self.ensure_connect()
        m = gxpm.down(tgt, tid, persist, keep_connection, gcmds)
        msg = gxpm.unparse(m)
        if self.asend(msg) == -1:
            Es("gxp_js.py: could not send msg to gxp daemon (he may be dead)\n")
            return None
        else:
            return tid

    def send_sigs_to_running_jobs(self):
        self.LOG("send_sigs:\n")
        ct = self.cur_time()
        n_dispatched = 0
        clauses = {}
        for run in self.runs_running.values():
            rid = "run_%d_%d" % (run.work_idx, run.run_idx)
            man_name = run.man_name
            self.LOG("send_sig to run[%d,%d] (%s) to %s\n" 
                     % (run.work_idx, run.run_idx, run.work.cmd, man_name))
            act = gxpm.action_sig(rid, "INT") # SIGINT
            if not clauses.has_key(man_name):
                clauses[man_name] = []
            clauses[man_name].append(act)
        if len(clauses) > 0:
            this_tgt = self.mk_target_tree(clauses)
            assert self.tid is not None
            x = self.send_clauses(this_tgt, self.tid, clauses, 0, 1)
            if x is None: return -1
        return 0

    def dispatch_runs(self):
        self.LOG("dispatch_runs:\n")
        ct = self.cur_time()
        n_dispatched = 0
        clauses = {}
        while len(self.matches) > 0:
            man,runs = self.matches.popitem()
            acts = []
            assert len(runs) > 0
            while len(runs) > 0:
                run = runs.pop(0)
                self.n_pending_matches = self.n_pending_matches - 1
                rid = "run_%d_%d" % (run.work_idx, run.run_idx)
                self.LOG("send run[%d,%d] (%s) to %s\n" 
                         % (run.work_idx, run.run_idx, run.work.cmd, man.name))
                act = gxpm.action_createproc(rid, run.work.dirs, run.work.envs,
                                             run.work.cmd, self.pipes, 
                                             []) # opts.rlimit
                acts.append(act)
                self.runs_running[rid] = run
                man.runs_running[rid] = run
                assert (run.time_start is None), run.time_start
                run.time_start = ct
                run.status = run_status.running
                n_dispatched = n_dispatched + 1
            assert len(acts) > 0
            clauses[man.name] = acts
        if len(clauses) > 0:
            self.n_events = self.n_events + n_dispatched
            self.ts["parallelism"].add_dx(ct, 0, n_dispatched)
            self.ts["run_counts"].add_dx(ct, 1, n_dispatched)
            this_tgt = self.mk_target_tree(clauses)
            assert self.tid is not None
            x = self.send_clauses(this_tgt, self.tid, clauses, 0, 1)
            if x is None: 
                self.LOG("dispatch_runs: could not send clauses\n")
                return -1
        self.LOG("dispatch_runs: %d runs dispatched\n" % n_dispatched)
        return 0

    def server_iterate(self):
        if self.logfp:
            self.LOG("server_iterate: %d runs running %d matches %d todo %d interrupted %d interrupts_seen wkg=%s\n"
                     % (len(self.runs_running), 
                        len(self.matches), len(self.runs_todo),
                        self.interrupted, self.interrupts_seen,
                        self.wkg))
        # check for unnoticed interrupts
        self.check_interrupted()
        # termination condition: case 1
        # no more jobs coming in fiture (wkg.closed()), 
        # no jobs running (len(self.runs_running) == 0),
        # no job-worker pairs previously matched and waiting to be dispatched
        # (len(self.matches) == 0), and
        # no jobs in the queue waiting for a matching worker.
        # this is true, successful termination
        if self.wkg.closed() \
                and len(self.runs_running) == 0 \
                and len(self.matches) == 0 \
                and len(self.runs_todo) == 0:
            if self.logfp:
                self.LOG("server_iterate: finished\n")
            return 0
        # termination condition: case 2
        # we got an interrupt (ctrl-c) and
        # no jobs running (len(self.runs_running) == 0)
        # or we got interrupts three times or more
        if self.interrupted > 2 or (self.interrupted > 0 and len(self.runs_running) == 0):
            if self.logfp:
                self.LOG("server_iterate: finished after %d interrupts\n"
                         % self.interrupted)
            return 0
        if self.so is None:
            msg = "warning: found socket to daemon broken, forced to quit (probably gxpd died)\n"
            Es(msg)
            if self.logfp:
                self.LOG(msg)
            return 0
        # self.check_time_limit()
        self.record_everything(0)
        self.make_matches()
        # calc timeout:
        # jobs to dispatch -> timeout needs to be short
        # (just check incoming events if any)
        # no jobs to dispatch -> save work. wait until
        # events come
        timeout = 0.001
        if len(self.matches) == 0:
            timeout = max(timeout, 
                          self.next_update_time - self.cur_time())
        # FIXIT: sufficient matches -> do dispatch
        if self.process_incoming_events(timeout) == 0 \
               or self.n_pending_matches > 1000:
            # we dispatch runs only when not interrupted
            if self.interrupted == 0 and self.dispatch_runs() == -1:
                return -1
        return 1
        
    def expandpath(self, p):
        return os.path.expanduser(os.path.expandvars(p))

    def gen_join_leave_rid(self, ctl):
        prefix = ctl
        if ctl is None: prefix = "join"
        x = self.join_leave_rid_idx
        self.join_leave_rid_idx = x + 1
        return "%s_%d" % (prefix, x)

    def send_initial_hello(self):
        """
        send a dummy job (do nothing) to know how many workers.
        we will be receiving event_die events from all workers.
        """
        opts = self.opts
        conf = self.conf
        if self.logfp and dbg>=2:
            self.LOG("sending initial hello to workers\n")
        self.events = None
        ex,tgt = self.session.select_exec_tree(opts)
        if ex == -1: return -1
        if self.logfp:
            self.LOG("opts = %s\ntarget of initial hello %d %s\n"
                     % (opts, ex, tgt))
        if tgt is not None:
            # self.LOG("%s: toplevel tree = %s\n" % (self.prog, tgt.show()))
            # dir=[] envs=None cmd="echo cpu 7 mem 5000000"
            pipes = []
            for fd in [ 1, 2 ]:
                pipes.append(("pipe", [ ("r", fd, "eof") ], [ ("w", fd) ]))
            cmd = self.expandpath(self.conf.worker_prof_cmd)
            rid = self.gen_join_leave_rid(conf.ctl)
            act = gxpm.action_createproc(rid, [], None, cmd, pipes, [])
            # persist=1, keep_connection=1
            tid = self.send_action(tgt, self.tid, act, 1, 1)
        return 0

    def mk_time_serieses(self, conf):
        time_series_spec = [
            ("parallelism", "Parallelism",  "Outstanding Jobs",         
             "boxes",             [ "" ]),
            ("loadavg",     "Load Average", "Last 1 min. Load Average", 
             "lines linewidth 3", [ "" ]),
            ("rss",         "RSS",          "Resident Set Size (MB)",   
             "lines linewidth 3", [ "" ]),
            ("mem",         "Memory",       "Memory Usage (MB)",  
             "lines linewidth 3", [ "total", "used", "free", "shared",
                                    "buffers", "cached" ]),
            ("run_counts",  "Run Counts",   "",                         
             "lines linewidth 3", [ "received", "sent", "finished" ]) ]
        TS = {}
        for name,section_title,graph_title,line_spec,line_titles in time_series_spec:
            TS[name] = time_series_data(self, conf.state_dir, name, 
                                        section_title, graph_title, line_spec, line_titles)
        return TS

    def gen_tid(self):
        tid = self.opts.tid
        if tid is None:
            tid = "tid-%d" % self.self_pid
        return tid

    def server_main_init(self, make_args):
        """
        real initialization
        """
        self.sys_argv = sys.argv
        self.cwd = os.getcwd()
        self.self_pid = os.getpid()
        self.final_status = None        # not known yet
        self.hostname = socket.gethostname()

        # task id shared by all runs
        self.tid = self.gen_tid()
        assert self.tid is not None
        self.join_leave_rid_idx = 0
        self.work_idx = 0
        self.wkg = mk_work_generator(self.conf, self, make_args)
        self.mang = man_generator(self.conf, self)
        self.mon = men_monitor(self.conf, self)
        self.works = mk_work_db(self.conf)
        self.runs_todo = [] # runs received, but not yet matched with man
        self.matches = {}   # matches[man] = [ run,run,.. ]
        self.n_pending_matches = 0
        self.runs_running = {}  # runs running (idx -> Run object)
        self.men = {}           # men (workers) gupid -> man object
        self.men_free = []      # list of free men

        self.ts = self.mk_time_serieses(self.conf)
        self.htmlg = html_generator(self.conf, self)
        self.gen_html_time = 0.0
        self.next_update_time = 0.0
        self.n_events = 0
        self.interrupted = 0
        self.interrupts_seen = 0
        pipes = []
        for fd in self.conf.job_output:
            pipes.append(("pipe", [ ("r", fd, "eof") ], [ ("w", fd) ]))
        self.pipes = pipes
        # send a dummy cmd to all workers to know how many are working
        if self.send_initial_hello() == -1: return -1
        return 0

    def server_main_with_log(self, make_args):
        if self.logfp:
            self.LOG("server_main_with_log: config\n%s\n" % self.conf)
        if self.server_main_init(make_args) == -1: 
            return cmd_interpreter.RET_NOT_RUN
        # if -a ctl=xxx is specified, return 
        if self.conf.ctl is not None:
            return 0
        while 1:
            try:
                x = self.server_iterate()
            except KeyboardInterrupt:
                # when interrupted, we continue to gracefully
                # wait for running jobs to finish
                self.interrupted = self.interrupted + 1
                continue
            # finished
            if x == 0: break
            # something wrong happened
            if x == -1: return 1
        # done. generate the last html
        self.final_status = self.wkg.determine_final_status()
        self.record_everything(1)
        exit_status,term_sig = self.final_status
        if exit_status is None: return 1
        return exit_status

    def cur_time(self):
        return time.time() - self.time_start
        
    def server_main(self, args, make_args):
        opts = jobsched_cmd_opts()
        if opts.parse(args) == -1:
            return gxpc.cmd_interpreter.RET_NOT_RUN
        self.conf = jobsched_config(opts)
        if self.conf.parse() == -1: 
            return gxpc.cmd_interpreter.RET_NOT_RUN
        if dbg>=1:
            Es("configuration:\n%s\n" % self.conf)
        # do minimum initialization to open the log file
        self.prog = sys.argv[0]
        self.time_start = time.time()
        self.logfp = None
        # create state dir and open the log file,
        # but only if -a ctl=xxxx is NOT specified
        if self.conf.ctl is None:
            if self.ensure_directory(self.conf.state_dir) == -1: 
                return gxpc.cmd_interpreter.RET_NOT_RUN
            if self.open_LOG() == -1: 
                return gxpc.cmd_interpreter.RET_NOT_RUN
        try:
            # real main with log opened
            return self.server_main_with_log(make_args)
        finally:
            self.close_LOG()

    def do_js_cmd(self, args):
        """
        args : whatever is given after 'js'
        """
        if self.init3() == -1: return cmd_interpreter.RET_NOT_RUN
        return self.server_main(args, None)
        
    def do_make2_cmd(self, args):
        """
        args : whatever is given after 'make2'
        """
        # FIXIT: parse args and give them to make
        if self.init3() == -1: return cmd_interpreter.RET_NOT_RUN
        # self.set_make_environ()
        make_args = []
        while args:
            a = args.pop(0)
            if a == "--": break
            make_args.append(a)
        # make_cmd = 'work_proc_sock2="make %s"' % (" ".join(make_args))
        return self.server_main(args, make_args)
        
    # db related stuff

    db_fields = [ "sys_argv", "cwd", "hostname",
                  "self_pid", "tid", "start_time", 
                  "current_time", 
                  "n_runs_todo",
                  "n_runs_running",
                  "n_works",
                  "n_free_men",
                  "final_status",
                  "gen_html_time", ]

    def get_td_final_status(self):
        if self.final_status is None:
            return "job_running","running"
        else:
            exit_status,term_sig = self.final_status
            if exit_status == 0:
                return "job_success",("exited 0")
            elif exit_status is not None:
                return "job_failed",("exited %d" % exit_status)
            elif term_sig is not None:
                return "job_killed",("killed %d" % term_sig)
            else:
                return "job_success","no jobs"

    def get_td_start_time(self):
        return time.strftime("%Y-%m-%d %H:%M:%S", 
                             time.localtime(self.time_start))

    def get_td_current_time(self):
        return self.cur_time()
        
    def get_td_n_runs_todo(self):
        return len(self.runs_todo)

    def get_td_n_runs_running(self):
        return len(self.runs_running)

    def get_td_n_works(self):
        return len(self.works)

    def get_td_n_free_men(self):
        return len(self.men_free)

if __name__ == "__main__":
    # dbg_jobsched_config()
    sys.exit(job_scheduler().main(sys.argv))
