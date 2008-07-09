import fcntl,os,re,socket,string,sys

# include '..' into PYTHONPATH
try:
    import ioman
except ImportError,e:
    sys.stderr.write(("warning: %s, perhaps you invoked this as a standalone "
                      "program. include gxp3 directory in your PYTHONPATH\n"
                      % (e.args,)))
    ioman = None

class ifconfig:
    """
    ifconfig().get_my_addrs() will return a list of
    IP addresses of this host
    """
    def __init__(self):
        self.v4ip_pat_str = "(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})"
        self.v4ip_p = re.compile(self.v4ip_pat_str)
        self.ifconfig_list = [ "LANG=C /sbin/ifconfig -a 2> /dev/null" ]

    def parse_ip_addr(self, addr):
        """
        '12.34.56.78' --> (12,34,56,78)
        """
        m = re.match(self.v4ip_pat_str, addr)
        if m is None: return None
        A,B,C,D = m.group(1,2,3,4)
        try:
            return string.atoi(A),string.atoi(B),string.atoi(C),string.atoi(D)
        except ValueError,e:
            return None

    def is_global_ip_addr(self, addr):
        a,b,c,d = self.parse_ip_addr(addr)
        if a == 192 and b == 168: return 0
        if a == 172 and 16 <= b < 32: return 0
        if a == 10: return 0
        if a == 127: return 0
        return 1

    def is_local_ip_addr(self, addr):
        if addr == "127.0.0.1":
            return 1
        else:
            return 0

    def is_private_ip_addr(self, addr):
        a,b,c,d = self.parse_ip_addr(addr)
        if a == 192 and b == 168: return 1
        if a == 172 and 16 <= b < 32: return 1
        if a == 10: return 1
        return 0

    def is_global_ip_addr(self, addr):
        if self.is_local_ip_addr(addr): return 0
        if self.is_private_ip_addr(addr): return 0
        return 1

    def in_same_private_subnet(self, P, Q):
        """
        true when (1) both P and Q are private, and (2) they belong
        to the same subnets.
        """
        if self.is_private_ip_addr(P) == 0: return 0
        if self.is_private_ip_addr(Q) == 0: return 0
        a,b,c,d = self.parse_ip_addr(P)
        A,B,C,D = self.parse_ip_addr(Q)
        if a == 192 and b == 168     and A == 192 and B == 168: return 1
        if a == 172 and 16 <= b < 32 and A == 172 and 16 <= B < 32: return 1
        if a == 10 and A == 10: return 1
        return 0

    def guess_connectable(self, P, Q):
        """
        P : address, Q : endpoint name.
        (e.g.,
        P = '133.11.238.3',
        Q = ('tcp', (('157.82.246.104', '192.168.1.254'), 59098))
        )
        """
        # consider * -> private is allowed
        # Qas is like ('157.82.246.104', '192.168.1.254')
        proto, (Qas, Qp) = Q            
        Qa = Qas[0]
        if self.is_global_ip_addr(Qa): return 1
        # consider global -> private is blocked
        if self.is_global_ip_addr(P): return 0
        if self.in_same_private_subnet(P, Qa): return 1
        return 0

    def get_addr_of_if_by_proc(self, ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            result = fcntl.ioctl(s.fileno(), 0x8915, #SIOCGIFADDR
                                 (ifname+'\0'*32)[:32])
        except IOError:
            s.close()
            return None
        s.close()
        return socket.inet_ntoa(result[20:24])

    def get_my_addrs_by_proc_net_dev(self):
        dev = "/proc/net/dev"
        if not os.path.exists(dev): return []
        fp = open(dev, "rb")
        # skip header(2 lines)
        line = fp.readline()
        line = fp.readline()
        addrs = []
        while 1:
            line = fp.readline()
            if line == "": break
            # line looks like:
            # "  eth0:323457502 5053463    0    0    0     0   ..."
            ifname_rest = string.split(string.strip(line), ":", 1)
            if len(ifname_rest) == 2:
                addr = self.get_addr_of_if_by_proc(ifname_rest[0])
                if addr is not None:
                    addrs.append(addr)
        fp.close()
        return addrs

    def get_my_addrs_by_ifconfig(self):
        # an ad-hoc treatment of the case where this program
        # is invoked without gxp3 directory in PYTHONPATH.
        # this should not happen when this module is called
        # by gxp and accompanying programs. 
        if ioman is None: return []
        patterns = [ re.compile("inet addr:([\d|\.]+)"),
                     re.compile("inet ([\d|\.]+)") ]
        addrs = []
        for c in self.ifconfig_list:
            r = os.popen(c)
            # blocking=1
            pr = ioman.primitive_channel_fd(r.fileno(), 1)
            # ifconfig_out = r.read()
            ifconfig_outs = []
            while 1:
                l,err,msg = pr.read(1000)
                if l <= 0: break
                ifconfig_outs.append(msg)
            ifconfig_out = string.join(ifconfig_outs, "")
            for p in patterns:
                found = p.findall(ifconfig_out)
                if len(found) > 0: break
            for addr in found:
                if self.parse_ip_addr(addr) is not None:
                    addrs.append(addr)
            # ??? we got:
            # "close failed: [Errno 9] Bad file descriptor"
            # r.close()
            # pr.close()
            if len(addrs) > 0: return addrs
        return []

    def get_my_addrs_by_lookup(self):
        hostname = socket.gethostname()
        try:
            can_name,aliases,ip_addrs = socket.gethostbyname_ex(hostname)
        except socket.error,e:
            return []
        return ip_addrs
        
    def apply_filter(self, ip, addr_filters):
        """
        apply address filters to ip address or hostname (ip).
        rule:
        (1) apply the first matching filter and ignore the rest
        (2) if none matches, ip is discarded
        (3) as an exception, if no filters are specified, take it
        """
        if len(addr_filters) == 0: return 1 # rule (3)
        if addr_filters is not None:
            for s,addr_filter in addr_filters:
                if re.match(addr_filter, ip):
                    return s            # rule (1)
        return 0                        # rule (2)

    def sort_addrs(self, ip_addrs, addr_filters):
        """
        1. remove duplicates.
        2. remove loopback addrs (127.0.0.1).
        3. sort IP addresses.
           global addrs first
        """
        a = {}
        for ip in ip_addrs:
            if self.apply_filter(ip, addr_filters):
                a[ip] = 1
        to_sort = []
        a_keys = a.keys()
        a_keys.sort()
        for ip in a_keys:
            if self.parse_ip_addr(ip) is None:
                to_sort.append((0, len(to_sort), ip))
            elif self.is_local_ip_addr(ip):
                # exclude '127.0.0.1'
                continue
            elif self.is_private_ip_addr(ip):
                # put addrs like '192.168.XX.XX' at the end
                to_sort.append((2, len(to_sort), ip))
            else:
                # put global ip addrs first
                to_sort.append((1, len(to_sort), ip))
        to_sort.sort()
        sorted_ip_addrs = []
        for _,_,ip in to_sort:
            sorted_ip_addrs.append(ip)
        return sorted_ip_addrs

    def compile_addr_filters(self, addr_filter_strings):
        addr_filters = []
        for addr_filter_string in addr_filter_strings:
            if addr_filter_string[0:1] == "-":
                addr_filters.append((0, re.compile(addr_filter_string[1:])))
            elif addr_filter_string[0:1] == "+":
                addr_filters.append((1, re.compile(addr_filter_string[1:])))
            else:
                addr_filters.append((1, re.compile(addr_filter_string)))
        return addr_filters

    def get_my_addrs_aux(self):
        """
        get my ip address the clients are told to connect to.

        addr_filters is a list of strings, each of which is the form

           +REGEXP
           -REGEXP
           REGEXP   (equivalent to +REGEXP)
        """
        # look at /proc/net/dev on Linux
        A = self.get_my_addrs_by_proc_net_dev()
        if len(A) > 0: return A
        # use ifconfig command
        A = self.get_my_addrs_by_ifconfig()
        if len(A) > 0: return A
        # second by looking up my hostname
        A = self.get_my_addrs_by_lookup()
        if len(A) > 0: return A
        return []

    def get_my_addrs(self, addr_filter_strings):
        addrs = self.get_my_addrs_aux()
        addr_filters = self.compile_addr_filters(addr_filter_strings)
        return self.sort_addrs(addrs, addr_filters)

    def get_my_addrs_and_hosts(self, addr_filter_strings):
        addrs = self.get_my_addrs_aux()
        hosts = [ socket.gethostname() ]
        addr_filters = self.compile_addr_filters(addr_filter_strings)
        return self.sort_addrs(addrs + hosts, addr_filters)

the_ifconfig_obj = ifconfig()

def get_my_addrs(addr_filters=None):
    if addr_filters is None: addr_filters = []
    return the_ifconfig_obj.get_my_addrs(addr_filters)

def get_my_addrs_and_hosts(addr_filters=None):
    if addr_filters is None: addr_filters = []
    return the_ifconfig_obj.get_my_addrs_and_hosts(addr_filters)

if __name__ == "__main__":
    sys.stdout.write("%s\n" % string.join(get_my_addrs_and_hosts(sys.argv[1:]), " "))
    
