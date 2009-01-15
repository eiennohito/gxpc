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

def import_safe_pickler():
    import cPickle,pickle
    try:
        cPickle.dumps(None)
        return cPickle
    except:
        return pickle

pickler = import_safe_pickler()

def unparse(m):
    return pickler.dumps(m)

def parse(msg):
    return pickler.loads(msg)

class exec_env:
    def __init__(self):
        self.cwd = None
        self.env = {}
    def show(self):
        return ("cwd=%s, env=%s" % (self.cwd, self.env))

class target_tree:
    def __init__(self, name, hostname, target_label,
                 eflag, exec_idx, eenv, children):
        self.name = name
        self.hostname = hostname
        self.target_label = target_label
        self.eflag = eflag
        self.exec_idx = exec_idx
        # self.cwd = cwd
        # self.env = {}
        self.eenv = eenv                # shared
        # allchildren <=> children is None
        self.children = children
        self.num_execs = None           # not known

    def count(self):
        if self.children is None: return None # unknown
        c = 1
        for ch in self.children:
            x = ch.count()
            if x is None: return None
            c = c + x
        return c

    def count_execs(self):
        if self.children is None: return None # unknown
        c = self.eflag
        for ch in self.children:
            x = ch.count_execs()
            if x is None: return None
            c = c + x
        return c

    def show(self):
        if self.children is None:
            cs = None
        else:
            cs = map(lambda c: c.show(), self.children)
        if self.eenv is None:
            eenv_show = "None"
        else:
            eenv_show = self.eenv.show()
        return ("target_tree(%s, %s, %s, %s, %s, %s, %s)" \
                % (self.name, self.hostname, self.target_label,
                   self.eflag, self.exec_idx,
                   eenv_show, cs))

    def set_eflag(self, flag):
        self.eflag = flag
        if self.children != None:
            for child in self.children:
                child.set_eflag(flag)

def merge_target_tree(tgt1, tgt2):
    eflag = tgt1.eflag or tgt2.eflag
    name_dictionary = {}
    children = tgt1.children + tgt2.children
    for child in children:
        if name_dictionary.has_key(child.name):
            name_dictionary[child.name] = merge_target_tree(name_dictionary[child.name], child)
        else:
            name_dictionary[child.name] = child
    return target_tree(tgt1.name, tgt1.hostname, tgt1.target_label,
                       eflag, None, tgt1.eenv, name_dictionary.values())


class xxx_synchronize_message:
    def __init__(self, peer_tree=None, exec_tree=None):
        self.peer_tree = peer_tree
        self.exec_tree = exec_tree

#
# actions
# 

class action:
    pass

class action_quit(action):
    pass

class action_ping(action):
    def __init__(self, level):
        self.level = level

class action_createproc(action):
    def __init__(self, rid, cwd, env, cmd, pipes):
        self.rid = rid
        self.cwd = cwd
        self.env = env
        self.cmd = cmd
        self.pipes = pipes

class action_createpeer(action):
    def __init__(self, rid, cwd, env, cmd, pipes):
        self.rid = rid
        self.cwd = cwd
        self.env = env
        self.cmd = cmd
        self.pipes = pipes

class action_feed(action):
    def __init__(self, rid, fd, payload):
        self.rid = rid
        self.fd = fd
        self.payload = payload
    
class action_close(action):
    def __init__(self, rid, fd):
        self.rid = rid
        self.fd = fd
    
class action_sig(action):
    def __init__(self, rid, sig):
        self.rid = rid
        self.sig = sig

class action_chdir(action):
    def __init__(self, to):
        self.to = to
    
class action_export(action):
    def __init__(self, var, val):
        self.var = var
        self.val = val

class action_trim(action):
    def __init__(self):
        pass
    
class action_set_max_buf_len(action):
    def __init__(self, max_buf_len):
        self.max_buf_len = max_buf_len
    
class action_prof_start(action):
    def __init__(self, file):
        self.file = file
    
class action_prof_stop(action):
    pass

class action_set_log_level(action):
    def __init__(self, level):
        self.level = level
    
class action_set_log_base_time(action):
    pass
    
class action_reclaim_task(action):
    def __init__(self, target_tids):
        self.target_tids = target_tids

# to synchronize gxpcs
class xxx_action_synchronize(action, xxx_synchronize_message):
    pass


#
# commands
#

#
# clause
#

class clause:
    def __init__ (self, on, actions):
        self.on = on
        self.actions = actions

#
#
#

keep_connection_never = 0
keep_connection_until_fin = 1
keep_connection_forever = 2

class down:
    def __init__(self, target, tid, persist, keep_connection, gcmds):
        self.target = target
        self.tid = tid
        self.persist = persist          # 1 if the task sholud persist
        self.keep_connection = keep_connection # 0, 1, 2
        self.gcmds = gcmds              # list of list of clauses

#
# event
#

class event:
    pass
    
class event_info(event):
    def __init__(self, status, msg):
        self.status = status
        self.msg = msg

class event_info_pong(event_info):
    def __init__(self, status, msg,
                 targetlabel, peername, hostname,
                 parent, children, children_in_progress):
        event_info.__init__(self, status, msg)
        self.targetlabel = targetlabel
        self.peername = peername
        self.hostname = hostname
        self.parent = parent
        self.children = children
        self.children_in_progress = children_in_progress

class event_io(event):
    """
    an event indicating a process or a child gxp says something.
    src : proc or peer
    kind : OK, EOF, ERROR, TIMEOUT
    rid : relative process ID within a task
    fd : file descriptor (channel name)
    <event name=io>
    <src>proc</src><kind>OK</kind><rid>...</rid>
    <pid>...</pid><fd>...</fd>
    <payload>...</payload>
    </event>
    """
    def __init__(self, src, kind, rid, pid, fd, payload, err_msg):
        self.src = src
        self.kind = kind
        self.rid = rid
        self.pid = pid
        self.fd = fd
        self.payload = payload
        self.err_msg = err_msg
        
class event_die(event):
    """
    an event indicating a process is dead.
    <event name=die>
     <src>proc</src><rid>...</rid><pid>...</pid><status>...</status>
    </event>
    """
    def __init__(self, src, rid, pid, status):
        self.src = src
        self.rid = rid
        self.pid = pid
        self.status = status

class event_peerstatus(event):
        
    """
    an event indicating a peer status (NG/OK) becomes available
    <event name=peerstatus>
     <peername>...</peername><status>OK</status>
     <parentname>...</parentname>
     <rid>rid</rid>
    </event>
    """
    def __init__(self, peername, target_label, hostname, status, parent_name, rid):
        self.peername = peername
        self.target_label = target_label
        self.hostname = hostname
        self.status = status
        self.parent_name = parent_name
        self.rid = rid

class event_fin(event):
    """
    <event name=fin><weight>3</weight></event>
    """
    def __init__(self, weight):
        self.weight = weight

class event_nopeersinprogress(event):
    """
    <event name=nopeersinprogress></event>
    """
    pass

# to synchronize gxpcs
class event_invalidate_view(event):
    def __init__(self):  # peer_tree, exec_tree
        pass

#
#
#

class up:
    def __init__(self, gupid, tid, event):
        self.gupid = gupid
        self.tid = tid
        self.event = event

class syn:
    def __init__(self, gupid, tid, event):
        self.gupid = gupid
        self.tid = tid
        self.event = event

