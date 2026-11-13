#!/usr/bin/env python3
"""Supervise a bounded, read-only startup check in its own process session.

Parent touches only private internal state and owned process metadata. Protected
settings and pool operations belong exclusively to its new-session worker.
"""
import ctypes
import fcntl
import errno
import json
import math
import os
from pathlib import Path
import pwd
import re
import signal
import stat
import subprocess
import sys
import time
import threading
import uuid

ACCOUNT_HOME = Path(pwd.getpwuid(os.getuid()).pw_dir)
STATE_DIR = ACCOUNT_HOME / 'Library/Application Support/home-server/startup-check'
LOCK_NAME='startup.lock'
SUPERVISOR_LOCK_NAME='supervisor.lock'
STATE_NAME='state.json'
PHASE_NAME='phase.json'
TIMEOUT_SECONDS=25
CLEANUP_SECONDS=4
ENV={'HOME':str(ACCOUNT_HOME),'PATH':'/usr/bin:/bin:/usr/sbin:/sbin','LC_ALL':'C','PYTHONDONTWRITEBYTECODE':'1'}
_PHASE_DIR=None
_RUN_ID=None
_PHASES={'opening_internal_root','opening_docker_settings','checking_pool_helper','checking_external_disk','checking_shared_directories','ready'}

class Refusal(Exception):pass


def canonical_boot_uuid(value):
    if (type(value) is not str or re.fullmatch(
            r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',value) is None
            or value.lower()=='00000000-0000-0000-0000-000000000000'):
        raise Refusal('boot_identity_unconfirmed')
    return value.lower()


def valid_boot_uuid(value):
    try:return canonical_boot_uuid(value)==value
    except Refusal:return False


def open_directory(path):
    path=Path(path)
    if not path.is_absolute() or '..' in path.parts:raise Refusal('unsafe_state_path')
    fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        for part in path.parts[1:]:
            child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
            os.close(fd);fd=child
        return fd
    except BaseException:os.close(fd);raise


def private_state(path):
    parent=open_directory(Path(path).parent)
    try:
        try:os.mkdir(Path(path).name,0o700,dir_fd=parent)
        except FileExistsError:pass
        fd=os.open(Path(path).name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
    finally:os.close(parent)
    st=os.fstat(fd)
    if st.st_uid!=os.getuid() or st.st_mode&0o077:
        os.close(fd);raise Refusal('state_directory_not_private')
    return fd


def regular(fd,limit=4096):
    st=os.fstat(fd)
    if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.getuid() or st.st_nlink!=1 or st.st_mode&0o077 or st.st_size>limit:
        raise Refusal('unsafe_private_state')
    return st


def read_state(directory):
    try:fd=os.open(STATE_NAME,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
    except FileNotFoundError:return None
    try:regular(fd);return json.loads(os.read(fd,4097))
    finally:os.close(fd)


def publish(directory,name,value):
    raw=(json.dumps(value,separators=(',',':'),allow_nan=False)+'\n').encode()
    if len(raw)>4096:raise Refusal('state_size_bound')
    try:
        old=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
    except FileNotFoundError:pass
    else:
        try:regular(old)
        finally:os.close(old)
    # One fixed temporary slot per fixed receipt; no retry archive grows.
    # state.json has one serialized parent writer; phase.json one locked worker.
    temporary='.'+name+'.tmp'
    try:stale=os.open(temporary,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
    except FileNotFoundError:pass
    else:
        try:regular(stale)
        finally:os.close(stale)
        os.unlink(temporary,dir_fd=directory)
    fd=None;created=False
    try:
        fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory);created=True
        view=memoryview(raw)
        while view:
            amount=os.write(fd,view)
            if amount<=0:raise Refusal('state_write_failed')
            view=view[amount:]
        os.fsync(fd);os.close(fd);fd=None
        os.replace(temporary,name,src_dir_fd=directory,dst_dir_fd=directory)
        os.fsync(directory)
    finally:
        if fd is not None:os.close(fd)
        if created:
            try:os.unlink(temporary,dir_fd=directory)
            except FileNotFoundError:pass


def lock_state(directory,name=LOCK_NAME):
    if name not in (LOCK_NAME,SUPERVISOR_LOCK_NAME):raise Refusal('unexpected_lock_name')
    fd=os.open(name,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600,dir_fd=directory)
    try:
        regular(fd,0);fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        return fd
    except BaseException:os.close(fd);raise


def worker_phase(phase):
    if phase not in _PHASES or _PHASE_DIR is None:raise Refusal('invalid_worker_phase')
    publish(_PHASE_DIR,PHASE_NAME,{'run':_RUN_ID,'phase':phase,'at_epoch':time.time()})


def worker_lock_fd():
    return int(os.environ['STARTUP_CHECK_LOCK_FD'])


def shared_clock():
    # Explicit shared boot clock: Python3.9 macOS time.monotonic is process-local.
    return time.clock_gettime(time.CLOCK_MONOTONIC_RAW)


def end_worker_group(pid):
    if os.getpid()!=pid:return
    if os.getsid(0)==pid and os.getpgrp()==pid:
        os.killpg(pid,signal.SIGKILL)
    else:
        # Never signal another numeric group if this process changed identity.
        os.kill(pid,signal.SIGKILL)


def worker_group_drained(run,lock,deadline):
    pid=os.getpid();remaining=min(.5,deadline-shared_clock())
    if remaining<=0:raise Refusal('worker_deadline')
    owner=OwnedSession(run,lock)
    rows=owner.process_table(time.monotonic()+remaining,group_id=pid)
    if rows!=[(os.getuid(),pid,pid)]:raise Refusal('worker_group_not_drained')


def guard_worker_lifetime(parent,pid,deadline):
    """Independent thread: only this still-live session leader may end its group.

    Kernel-uninterruptible tasks cannot be promised physical termination; the
    inherited lock and parent's reconciliation remain the completion authority.
    """
    while os.getppid()==parent and shared_clock()<deadline:
        time.sleep(min(.05,max(0,deadline-shared_clock())))
    end_worker_group(pid)


def worker_entry(check_callable):
    """Internal worker only: inherited descriptors + same new session required."""
    global _PHASE_DIR,_RUN_ID
    authenticated=False;pid=os.getpid()
    try:
        run=os.environ['STARTUP_CHECK_RUN_ID']
        lock=int(os.environ['STARTUP_CHECK_LOCK_FD']);gate=int(os.environ['STARTUP_CHECK_GATE_FD'])
        directory=int(os.environ['STARTUP_CHECK_STATE_FD']);parent=int(os.environ['STARTUP_CHECK_PARENT_PID'])
        deadline=float(os.environ['STARTUP_CHECK_DEADLINE'])
        if not math.isfinite(deadline) or not 0<deadline-shared_clock()<=30:
            raise Refusal('invalid_worker_deadline')
        if not re.fullmatch(r'[a-f0-9]{32}',run) or min(lock,gate,directory)<3 or len({lock,gate,directory})!=3:
            raise Refusal('invalid_internal_worker')
        if os.getppid()!=parent or os.getsid(0)!=os.getpid() or os.getpgrp()!=os.getpid():
            raise Refusal('worker_session_identity_changed')
        ds=os.fstat(directory)
        if not stat.S_ISDIR(ds.st_mode) or ds.st_uid!=os.getuid() or ds.st_mode&0o077:raise Refusal('invalid_worker_state')
        ls=regular(lock,0);expected=os.stat(LOCK_NAME,dir_fd=directory,follow_symlinks=False)
        if (ls.st_dev,ls.st_ino)!=(expected.st_dev,expected.st_ino):raise Refusal('worker_lock_identity_changed')
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if not stat.S_ISFIFO(os.fstat(gate).st_mode):raise Refusal('invalid_worker_start_gate')
        _PHASE_DIR,_RUN_ID=directory,run
        authenticated=True
        threading.Thread(target=guard_worker_lifetime,args=(parent,os.getpid(),deadline),daemon=True).start()
        if os.read(gate,1)!=b'G':raise Refusal('parent_start_not_confirmed')
        os.close(gate)
        check_callable()
        worker_group_drained(run,lock,deadline)
        worker_phase('ready')
        return 0
    except BaseException:
        # Keep the leader/timer alive through failure cleanup. Pool-helper
        # timeout can leave descendants after its direct subprocess is reaped.
        if authenticated:end_worker_group(pid)
        return 1


class UsageV2(ctypes.Structure):
    _fields_=[('uuid',ctypes.c_uint8*16)]+[(name,ctypes.c_uint64) for name in (
        'user','system','wakeups','interrupts','pageins','wired','resident','physical','start','exit',
        'child_user','child_system','child_wakeups','child_interrupts','child_pageins','child_elapsed','read_bytes','write_bytes')]


class BSDShortInfo(ctypes.Structure):
    # Darwin SDK sys/proc_info.h, PROC_PIDT_SHORTBSDINFO (13).
    _fields_=[(key,ctypes.c_uint32) for key in ('pid','ppid','pgid','status')]+[
        ('comm',ctypes.c_char*16)]+[(key,ctypes.c_uint32) for key in (
        'flags','uid','gid','ruid','rgid','svuid','svgid','reserved')]


class SigInfo(ctypes.Structure):
    # Darwin SDK sys/signal.h siginfo_t; all fields retained for correct ABI size.
    _fields_=[('signo',ctypes.c_int),('error',ctypes.c_int),('code',ctypes.c_int),
        ('pid',ctypes.c_int),('uid',ctypes.c_uint),('status',ctypes.c_int),
        ('address',ctypes.c_void_p),('value',ctypes.c_void_p),('band',ctypes.c_long),
        ('padding',ctypes.c_ulong*7)]


class OwnedSession:
    """Fresh nonce + birth + UID session/group checks before any owned-group signal."""
    def __init__(self,run,lock_fd):
        self.marker=('STARTUP_CHECK_RUN_ID='+run).encode();self.lock_fd=lock_fd
        self.libproc=ctypes.CDLL('/usr/lib/libproc.dylib',use_errno=True)
        self.libproc.proc_pid_rusage.argtypes=[ctypes.c_int,ctypes.c_int,ctypes.c_void_p]
        self.libproc.proc_pid_rusage.restype=ctypes.c_int
        self.libproc.proc_listpids.argtypes=[ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p,ctypes.c_int]
        self.libproc.proc_listpids.restype=ctypes.c_int
        self.libproc.proc_pidinfo.argtypes=[ctypes.c_int,ctypes.c_int,ctypes.c_uint64,ctypes.c_void_p,ctypes.c_int]
        self.libproc.proc_pidinfo.restype=ctypes.c_int
        self.libc=ctypes.CDLL('/usr/lib/libSystem.B.dylib',use_errno=True)
        self.libc.sysctl.argtypes=[ctypes.POINTER(ctypes.c_int),ctypes.c_uint,ctypes.c_void_p,ctypes.POINTER(ctypes.c_size_t),ctypes.c_void_p,ctypes.c_size_t]
        self.libc.sysctl.restype=ctypes.c_int
        self.libc.sysctlbyname.argtypes=[ctypes.c_char_p,ctypes.c_void_p,ctypes.POINTER(ctypes.c_size_t),ctypes.c_void_p,ctypes.c_size_t]
        self.libc.sysctlbyname.restype=ctypes.c_int
        self.libc.waitid.argtypes=[ctypes.c_uint,ctypes.c_uint,ctypes.POINTER(SigInfo),ctypes.c_int]
        self.libc.waitid.restype=ctypes.c_int
        self.metadata_unknown=False;self.signalled=False;self.residual_work=False

    def birth(self,pid):
        value=UsageV2()
        if self.libproc.proc_pid_rusage(pid,2,ctypes.byref(value)):raise Refusal('owned_birth_unconfirmed')
        return value.start

    def exit_code(self,child):
        # Never reap before the final group signal: the waitable leader pins its
        # PID/PGID, preventing reuse between an ownership check and killpg.
        info=SigInfo();pid=child['process'].pid
        if self.libc.waitid(1,pid,ctypes.byref(info),1|4|32):
            raise Refusal('owned_exit_state_unconfirmed')
        if info.pid==0:return None
        if info.pid!=pid:raise Refusal('owned_exit_identity_changed')
        child['exit_observed']=True
        return info.status if info.code==1 else -info.status

    def tagged(self,pid):
        mib,size=(ctypes.c_int*3)(1,49,pid),ctypes.c_size_t()
        if self.libc.sysctl(mib,3,None,ctypes.byref(size),None,0) or not 4<size.value<=1024*1024:return False
        data=ctypes.create_string_buffer(size.value)
        if self.libc.sysctl(mib,3,data,ctypes.byref(size),None,0):return False
        raw=data.raw[:size.value];argc=ctypes.c_int.from_buffer_copy(raw[:4]).value
        if not 0<argc<=10000:return False
        cursor=raw.find(b'\0',4)
        if cursor<0:return False
        while cursor<len(raw) and raw[cursor]==0:cursor+=1
        for _ in range(argc):
            cursor=raw.find(b'\0',cursor)
            if cursor<0:return False
            cursor+=1
        return self.marker in raw[cursor:].split(b'\0')

    def process_table(self,deadline,group_id=None):
        # Kernel metadata only, no auxiliary process that could outlive cleanup.
        # A full/truncated buffer or any non-disappearance error fails closed.
        if time.monotonic()>=deadline:raise Refusal('metadata_deadline')
        if group_id is not None and (type(group_id) is not int or group_id<=1):
            raise Refusal('invalid_process_group')
        pids=(ctypes.c_int*32768)();capacity=ctypes.sizeof(pids)
        length=self.libproc.proc_listpids(1 if group_id is None else 2,group_id or 0,pids,capacity)
        if not 0<length<capacity or length%ctypes.sizeof(ctypes.c_int):
            raise Refusal('owned_group_metadata_unconfirmed')
        rows=[]
        for pid in pids[:length//ctypes.sizeof(ctypes.c_int)]:
            if time.monotonic()>=deadline:raise Refusal('metadata_deadline')
            if pid<=0:continue
            info=BSDShortInfo();ctypes.set_errno(0)
            size=self.libproc.proc_pidinfo(pid,13,0,ctypes.byref(info),ctypes.sizeof(info))
            if size!=ctypes.sizeof(info):
                if size==0 and ctypes.get_errno()==errno.ESRCH:continue
                raise Refusal('owned_group_metadata_unconfirmed')
            if info.pid!=pid:raise Refusal('owned_group_metadata_unconfirmed')
            rows.append((info.uid,pid,info.pgid))
        if time.monotonic()>=deadline:raise Refusal('metadata_deadline')
        return rows

    def members(self,child,deadline):
        members=[]
        for uid,pid,pgid in self.process_table(deadline):
            if time.monotonic()>=deadline:raise Refusal('cleanup_deadline')
            if pgid!=child['process'].pid:continue
            if pid==child['process'].pid and child.get('exit_observed') is True:continue
            try:
                birth=self.birth(pid)
                if (uid!=os.getuid() or os.getsid(pid)!=child['process'].pid or birth<child['start']
                        or (pid==child['process'].pid and birth!=child['start'])
                        or (pid in child['known'] and child['known'][pid]!=birth) or not self.tagged(pid)):
                    raise Refusal('owned_group_identity_unconfirmed')
            except ProcessLookupError:continue
            except (Refusal,PermissionError):
                # The pinned direct child may have exited since exit_code()
                # observed it running. Its zombie no longer exposes a nonce.
                # WNOWAIT proves only this exact unreaped child's exit.
                if pid==child['process'].pid and self.exit_code(child) is not None:continue
                try:os.kill(pid,0)
                except ProcessLookupError:continue
                raise
            child['known'][pid]=birth;members.append(pid)
        return members

    def boot_identity(self):
        # Boot-session UUID is immutable within a boot; kern.boottime is not
        # (calendar correction can change it). One fixed-size kernel read only.
        value=ctypes.create_string_buffer(37);size=ctypes.c_size_t(37)
        rc=self.libc.sysctlbyname(b'kern.bootsessionuuid',ctypes.byref(value),ctypes.byref(size),None,0)
        raw=value.raw
        if rc!=0 or size.value!=37 or raw[-1:]!=b'\0' or b'\0' in raw[:-1]:
            raise Refusal('boot_identity_unconfirmed')
        try:return canonical_boot_uuid(raw[:-1].decode('ascii',errors='strict'))
        except (UnicodeDecodeError,Refusal):raise Refusal('boot_identity_unconfirmed') from None

    def session_absent(self,session_id,deadline):
        # Metadata only: never signal any PID during prior-run reconciliation.
        for _uid,pid,pgid in self.process_table(deadline):
            if time.monotonic()>=deadline:raise Refusal('reconciliation_deadline')
            if pgid==session_id:return False
            try:
                if os.getsid(pid)==session_id:return False
            except ProcessLookupError:continue
        return True

    def prior_completed(self,prior,lock_identity,boot,deadline):
        if (prior.get('protocol_version')!=1 or prior.get('worker_lock_identity')!=lock_identity
                or prior.get('guarded_start_protocol') is not True
                or not re.fullmatch(r'[a-f0-9]{32}',prior.get('run',''))
                or not valid_boot_uuid(prior.get('boot_identity')) or not valid_boot_uuid(boot)):
            return False,'prior_protocol_or_lock_unconfirmed'
        if prior['boot_identity']!=boot:return True,'previous_boot_ended'
        pid=prior.get('child_pid')
        if pid is None:
            # Before persisted PID registration, the guarded worker cannot pass
            # its inherited pipe gate. Acquiring the SAME worker flock proves
            # the registered pre-GO process/descriptor lifetime has ended.
            if prior.get('start_gate_released') is False and prior.get('phase') in ('starting','quarantined'):
                return True,'pre_go_registration_ended_and_inherited_lock_released'
            return False,'prior_registration_unconfirmed'
        if (type(pid) is not int or pid<=1 or prior.get('child_sid')!=pid or prior.get('child_pgid')!=pid
                or type(prior.get('child_birth')) is not int or prior['child_birth']<=0):
            return False,'prior_child_provenance_unconfirmed'
        try:
            try:current_birth=self.birth(pid)
            except Refusal:
                try:os.kill(pid,0)
                except ProcessLookupError:current_birth=None
                else:return False,'prior_birth_lookup_unconfirmed'
            if current_birth==prior['child_birth']:return False,'prior_exact_worker_still_present'
            if self.group_exists(pid) or not self.session_absent(pid,deadline):
                return False,'prior_group_or_session_still_present'
        except BaseException:return False,'prior_physical_state_unknown'
        return True,'prior_worker_birth_ended_group_and_session_absent'

    @staticmethod
    def group_exists(pid):
        try:os.killpg(pid,0);return True
        except ProcessLookupError:return False

    def cleanup(self,child,deadline):
        def finish_if_empty():
            # Exit metadata may disappear before the pinned child is waitable.
            # Spend only the original cleanup deadline observing that transition.
            while self.exit_code(child) is None:
                remaining=deadline-time.monotonic()
                if remaining<=0:raise Refusal('cleanup_deadline')
                time.sleep(min(.025,remaining))
            if self.members(child,deadline):return False
            # No signal is allowed after this bounded reap. A surviving group
            # or an unknown metadata state becomes quarantine, never escalation.
            remaining=min(.1,deadline-time.monotonic())
            if remaining<=0:raise Refusal('cleanup_deadline')
            child['process'].wait(timeout=remaining)
            return not self.group_exists(child['process'].pid) and not self.metadata_unknown
        try:
            for sig,grace in ((signal.SIGTERM,.5),(signal.SIGKILL,1.0)):
                self.exit_code(child)
                members=self.members(child,deadline)
                if not members:return finish_if_empty()
                self.residual_work=True
                # Fresh identity checks plus the unreaped leader pin this group.
                try:
                    os.killpg(child['process'].pid,sig);self.signalled=True
                except (ProcessLookupError,PermissionError):
                    # Darwin can report EPERM for a zombie-only group. Never
                    # infer absence: re-observe the owned child and fresh census.
                    return finish_if_empty()
                until=min(deadline,time.monotonic()+grace)
                while time.monotonic()<until:
                    self.exit_code(child)
                    if not self.members(child,deadline):return finish_if_empty()
                    time.sleep(.025)
            return False
        except BaseException:return False


def supervise(worker_command,*,state_dir=STATE_DIR,timeout_seconds=TIMEOUT_SECONDS,cleanup_seconds=CLEANUP_SECONDS,ownership_factory=OwnedSession):
    """Explicit argv/callable seam for isolated tests; production CLI supplies only itself."""
    began=time.monotonic();shared_began=shared_clock();run=uuid.uuid4().hex
    result={'run':run,'phase':'not_started','ready':False,'physical_cleanup_confirmed':False,'reason':'not_started'}
    directory=lock=serialization=gate_read=gate_write=None;child=None;owner=None
    cancelled=threading.Event();prior_handlers={}
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):
            prior_handlers[signum]=signal.signal(signum,lambda *_:cancelled.set())
    def save(phase,**fields):
        result.update(fields,phase=phase,at_epoch=time.time())
        publish(directory,STATE_NAME,result)
    try:
        if not .1<=timeout_seconds<=30 or not .1<=cleanup_seconds<=5:raise Refusal('invalid_bounds')
        directory=private_state(state_dir)
        try:serialization=lock_state(directory,SUPERVISOR_LOCK_NAME)
        except BlockingIOError:
            result.update(phase='busy',reason='prior_supervisor_lock_held');return result
        try:lock=lock_state(directory)
        except BlockingIOError:
            result.update(phase='busy',reason='prior_worker_lock_held');return result
        prior=read_state(directory)
        owner=ownership_factory(run,lock)
        boot=owner.boot_identity()
        ls=os.fstat(lock);lock_identity=[ls.st_dev,ls.st_ino]
        reconciliation=None
        if prior is not None and (prior.get('phase')!='complete' or prior.get('physical_cleanup_confirmed') is not True):
            try:ended,why=owner.prior_completed(prior,lock_identity,boot,time.monotonic()+1.5)
            except BaseException:ended,why=False,'prior_physical_state_unknown'
            if not ended:
                # Preserve the original owner/provenance; never replace it with
                # this attempt's nonce or erase the last useful child identity.
                result=dict(prior)
                result.update(ready=False,physical_cleanup_confirmed=False)
                save('quarantined',reason=why);return result
            reconciliation={'prior_run':prior['run'],'reason':why}
        result.update(protocol_version=1,worker_lock_identity=lock_identity,boot_identity=boot,
                      guarded_start_protocol=True,start_gate_released=False,
                      worker_deadline_monotonic=shared_began+timeout_seconds,worker_deadline_clock='CLOCK_MONOTONIC_RAW')
        if reconciliation:result['reconciled_prior']=reconciliation
        save('starting',reason='check_running')
        if cancelled.is_set():raise Refusal('cancelled')
        gate_read,gate_write=os.pipe()
        argv=worker_command() if callable(worker_command) else list(worker_command)
        env=dict(ENV,STARTUP_CHECK_RUN_ID=run,STARTUP_CHECK_LOCK_FD=str(lock),STARTUP_CHECK_GATE_FD=str(gate_read),
                 STARTUP_CHECK_STATE_FD=str(directory),STARTUP_CHECK_PARENT_PID=str(os.getpid()),
                 STARTUP_CHECK_DEADLINE=str(shared_began+timeout_seconds))
        process=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
            env=env,start_new_session=True,pass_fds=(lock,gate_read,directory))
        child={'process':process,'start':None,'known':{}}
        child['start']=owner.birth(process.pid);child['known'][process.pid]=child['start']
        if os.getsid(process.pid)!=process.pid or os.getpgid(process.pid)!=process.pid:raise Refusal('owned_session_unconfirmed')
        save('running',child_pid=process.pid,child_birth=child['start'],child_sid=process.pid,child_pgid=process.pid,start_gate_released=True)
        os.close(gate_read);gate_read=None
        if cancelled.is_set():raise Refusal('cancelled')
        os.write(gate_write,b'G');os.close(gate_write);gate_write=None
        end=began+timeout_seconds
        while owner.exit_code(child) is None and time.monotonic()<end and not cancelled.is_set():time.sleep(min(.05,max(0,end-time.monotonic())))
        code=owner.exit_code(child)
        expired=time.monotonic()>=end
        result['ready']=code==0 and not cancelled.is_set() and not expired
        result['reason']='cancelled' if cancelled.is_set() else 'whole_check_deadline' if expired else 'ready' if code==0 else 'check_failed'
    except BaseException:
        result.update(ready=False,reason='cancelled' if cancelled.is_set() else 'supervisor_or_check_unconfirmed')
    finally:
        for fd in (gate_read,gate_write):
            if fd is not None:os.close(fd)
        if child is not None:
            try:save('cleanup')
            except BaseException:result['ready']=False
            confirmed=owner is not None and child['start'] is not None and owner.cleanup(child,time.monotonic()+cleanup_seconds)
            if confirmed:
                try:child['process'].wait(timeout=.1)
                except BaseException:confirmed=False
            # Never LOCK_UN: inherited open-file-description locks must survive
            # parent death or an escaped/unreapable descendant. A new FD must win
            # the lock after closing our copy before physical completion is claimed.
            if lock is not None:os.close(lock);lock=None
            if confirmed:
                try:
                    lock=lock_state(directory)
                    latest=os.fstat(lock)
                    if [latest.st_dev,latest.st_ino]!=result.get('worker_lock_identity'):confirmed=False
                except BaseException:confirmed=False
            result['cleanup_signalled']=bool(getattr(owner,'signalled',False))
            result['residual_work_observed']=bool(getattr(owner,'residual_work',False))
            if result['ready'] and (result['cleanup_signalled'] or result['residual_work_observed']):
                result.update(ready=False,reason='residual_work_required_cleanup')
            result['physical_cleanup_confirmed']=bool(confirmed)
            if not confirmed:result.update(ready=False,reason='physical_cleanup_unconfirmed')
            if cancelled.is_set():result.update(ready=False,reason='cancelled')
            try:save('complete' if confirmed else 'quarantined')
            except BaseException:result.update(phase='quarantined',ready=False,reason='quarantine_write_unconfirmed')
        elif directory is not None and lock is not None and result['phase']=='starting':
            # No child was created; failed metadata/spawn preparation can retry.
            result['physical_cleanup_confirmed']=True
            try:save('complete')
            except BaseException:result.update(phase='quarantined',ready=False)
        # Recheck cancellation after final publication while still serialized.
        if cancelled.is_set():
            result.update(ready=False,reason='cancelled')
            if directory is not None and serialization is not None and result.get('phase') in ('complete','quarantined'):
                try:publish(directory,STATE_NAME,result)
                except BaseException:result.update(phase='quarantined',ready=False,reason='quarantine_write_unconfirmed')
        if lock is not None:os.close(lock)
        if directory is not None:os.close(directory)
        if serialization is not None:os.close(serialization)
        for signum,handler in prior_handlers.items():signal.signal(signum,handler)
        result['elapsed_seconds']=round(time.monotonic()-began,4)
        if cancelled.is_set():result.update(ready=False,reason='cancelled')
    return result
