"""Real private temp files and injected I/O faults; no production state or services."""
import contextlib
import copy
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import persistence_recovery as persistence
from coordinator import APIError, Coordinator, StateFile, dispatch, exclusive_state_lock, maintain
from test_coordinator import Clock, Health

EMPTY = {"version": 1, "requests": {}, "queue": [], "active": None}


@contextlib.contextmanager
def fixture(**config):
    with tempfile.TemporaryDirectory() as folder:
        parent = Path(folder) / 'state-dir'
        parent.mkdir(mode=0o700)
        path = parent / 'state.json'
        StateFile(path).save(copy.deepcopy(EMPTY))
        with exclusive_state_lock(path) as ownership:
            store, clock = StateFile(path, ownership), Clock()
            engine = Coordinator(store, Health(), {"healthy_seconds": 0, **config},
                                 clock=clock, monotonic_clock=clock)
            yield engine, store, clock, path


def submit(engine, name='request-a', **kwargs):
    body = dict(request_id=name, job='fixture', peak_mib=512,
                wait_timeout_s=1000, run_timeout_s=100)
    body.update(kwargs)
    return engine.submit(body)


@contextlib.contextmanager
def fault(stage):
    original = persistence.os.fsync
    count = 0
    def sync(fd):
        nonlocal count
        count += 1
        if count == (1 if stage == 'before_replace' else 2):
            raise OSError('synthetic storage unavailable')
        return original(fd)
    with patch.object(persistence.os, 'fsync', side_effect=sync):
        yield


class PersistenceRecoveryTests(unittest.TestCase):
    def test_queue_grant_release_before_and_after_replace_recover_exact_snapshot(self):
        for operation in ('queue', 'grant', 'release'):
            for stage in ('before_replace', 'after_replace'):
                with self.subTest(operation=operation, stage=stage), fixture() as (e,s,c,p):
                    if operation != 'queue': submit(e)
                    if operation == 'release':
                        lease = e.get('request-a')['lease_id']
                    previous = p.read_bytes()
                    with fault(stage), self.assertRaises(OSError):
                        if operation == 'queue': submit(e)
                        elif operation == 'grant': e.get('request-a')
                        else: e.finish('request-a', {'lease_id': lease, 'stopped': True})
                    frozen = e.failed_snapshot
                    self.assertTrue(e.failed)
                    self.assertEqual(frozen, persistence.encode(e.state))
                    if stage == 'before_replace': self.assertEqual(p.read_bytes(), previous)
                    else: self.assertEqual(p.read_bytes(), frozen)
                    c.advance(30)
                    self.assertTrue(e.retry_persistence())
                    self.assertFalse(e.failed)
                    self.assertEqual(p.read_bytes(), frozen)
                    self.assertEqual(persistence.encode(e.state), frozen)
                    if operation == 'grant': self.assertEqual(e.state['active'], 'request-a')
                    else: self.assertIsNone(e.state['active'])
                    if operation == 'release': self.assertEqual(e.state['requests']['request-a']['state'], 'released')
                    self.assertIsNone(e.healthy_since)
                    self.assertFalse(e.warning_window.ready)

    def test_repeated_io_failure_is_rate_limited_then_recovers(self):
        with fixture() as (e,s,c,p):
            with fault('before_replace'), self.assertRaises(OSError): submit(e)
            frozen = e.failed_snapshot
            with patch.object(s, 'retry_frozen', wraps=s.retry_frozen) as retry:
                for _ in range(10): self.assertFalse(e.retry_persistence())
                retry.assert_not_called()
                c.advance(30)
                with fault('before_replace'): self.assertFalse(e.retry_persistence())
                self.assertTrue(e.failed)
                self.assertEqual(e.failed_snapshot, frozen)
                self.assertFalse(e.retry_persistence())
                self.assertEqual(retry.call_count, 1)
                c.advance(30)
                self.assertTrue(e.retry_persistence())
                self.assertEqual(retry.call_count, 2)

    def test_visible_unsynced_candidate_survives_later_failed_rename(self):
        with fixture() as (e,s,c,p):
            with fault('after_replace'), self.assertRaises(OSError): submit(e)
            frozen=e.failed_snapshot
            first_inode=p.stat().st_ino
            c.advance(30)
            with patch.object(persistence.os,'replace',side_effect=OSError('rename failed before effect')):
                self.assertFalse(e.retry_persistence())
            self.assertEqual(p.stat().st_ino,first_inode)
            self.assertEqual(p.read_bytes(),frozen)
            self.assertTrue(e.failed)
            c.advance(30)
            self.assertTrue(e.retry_persistence())
            self.assertEqual(p.read_bytes(),frozen)
            self.assertIsNone(e.state['active'])

    def test_previous_owned_inode_is_not_successful_new_write_proof(self):
        with fixture() as (e,s,c,p):
            original=p.read_bytes()
            backup=p.with_name('external-prior-inode')
            replace, sync = persistence.os.replace, persistence.os.fsync
            sync_count=0
            def displaced(src,dst):
                replace(p,backup)
                replace(src,dst)
            def restore_after_directory_sync(fd):
                nonlocal sync_count
                sync_count+=1
                sync(fd)
                if sync_count==2:replace(backup,p)
            with patch.object(persistence.os,'replace',side_effect=displaced), patch.object(persistence.os,'fsync',side_effect=restore_after_directory_sync):
                with self.assertRaises(persistence.PersistenceConflict):submit(e)
            self.assertEqual(p.read_bytes(),original)
            self.assertTrue(e.failed)
            self.assertEqual(e.recovery_blocked,'state_ownership_conflict')

    def test_no_api_mutates_failed_snapshot_or_refreshes_queue_presence(self):
        with fixture() as (e,s,c,p):
            with fault('before_replace'), self.assertRaises(OSError): submit(e)
            frozen, presence = e.failed_snapshot, dict(e.queue_last_seen)
            c.advance(30)
            lock = threading.Lock()
            calls = [('GET','/v1/status',{}), ('GET','/v1/requests/request-a',{}),
                     ('POST','/v1/requests/request-a/cancel',{'stopped':True}),
                     ('POST','/v1/requests/request-a/queued-heartbeat',{}),
                     ('POST','/v1/requests/request-a/heartbeat',{'lease_id':'fixture'}),
                     ('POST','/v1/requests',dict(request_id='request-b',job='fixture',peak_mib=512,run_timeout_s=100))]
            def call(args):
                with lock:
                    with self.assertRaises(APIError) as raised: dispatch(e,*args)
                    self.assertEqual(raised.exception.status,503)
            with ThreadPoolExecutor(max_workers=6) as pool: list(pool.map(call,calls*3))
            self.assertEqual(persistence.encode(e.state),frozen)
            self.assertEqual(e.queue_last_seen,presence)
            self.assertEqual(e.recovery_attempts,0)  # API traffic cannot trigger retries.

    def test_maintenance_alone_retries_without_claim_or_health_reuse(self):
        with fixture() as (e,s,c,p):
            with fault('before_replace'), self.assertRaises(OSError): submit(e)
            c.advance(30)
            class OnePass:
                def __init__(self): self.done=False
                def is_set(self): return self.done
                def wait(self,seconds): self.done=True
            maintain(e,threading.Lock(),OnePass())
            self.assertFalse(e.failed)
            self.assertIsNone(e.state['active'])
            self.assertIsNone(e.healthy_since)
            self.assertEqual(e.state['requests']['request-a']['state'],'queued')

    def test_expired_failed_grant_remains_orphan_after_recovery(self):
        with fixture() as (e,s,c,p):
            submit(e,run_timeout_s=10)
            with fault('after_replace'), self.assertRaises(OSError): e.get('request-a')
            identity=copy.deepcopy(e.state['requests']['request-a'])
            c.advance(30);self.assertTrue(e.retry_persistence())
            result=e.get('request-a')
            self.assertEqual(result['state'],'blocked_orphan')
            self.assertEqual(result['lease_id'],identity['lease_id'])
            self.assertEqual(result['run_deadline'],identity['run_deadline'])
            self.assertEqual(e.state['active'],'request-a')
            submit(e,'request-b');self.assertEqual(e.get('request-b')['state'],'queued')

    def test_queue_deadline_and_owner_absence_are_not_extended(self):
        for wait in (20,1000):
            with self.subTest(wait=wait), fixture() as (e,s,c,p):
                with fault('before_replace'), self.assertRaises(OSError): submit(e,wait_timeout_s=wait)
                original=copy.deepcopy(e.state['requests']['request-a'])
                c.advance(31);self.assertTrue(e.retry_persistence());e.tick()
                result=e.state['requests']['request-a']
                self.assertEqual(result['state'],'expired')
                self.assertEqual(result['created_at'],original['created_at'])
                self.assertEqual(result['queue_deadline'],original['queue_deadline'])
                self.assertIsNone(e.state['active'])

    def test_release_recovery_requires_fresh_health_before_replacement_grant(self):
        with fixture() as (e,s,c,p):
            submit(e);lease=e.get('request-a')['lease_id'];submit(e,'request-b')
            e.config['healthy_seconds']=60
            with fault('before_replace'), self.assertRaises(OSError):
                e.finish('request-a',{'lease_id':lease,'stopped':True})
            c.advance(30);self.assertTrue(e.retry_persistence())
            self.assertEqual(e.get('request-b')['state'],'queued')
            for _ in range(11): c.advance(5);self.assertEqual(e.get('request-b')['state'],'queued')
            c.advance(5);self.assertEqual(e.get('request-b')['state'],'granted')

    def test_foreign_replacement_corruption_unsafe_mode_and_lock_loss_stay_blocked(self):
        for change in ('foreign_bytes','same_bytes_new_inode','corrupt','unsafe_mode','lock_replaced','lock_deleted','parent_replaced','state_deleted'):
            with self.subTest(change=change), fixture() as (e,s,c,p):
                with fault('before_replace'), self.assertRaises(OSError): submit(e)
                if change in ('foreign_bytes','same_bytes_new_inode','corrupt'):
                    other=p.with_name('foreign')
                    other.write_bytes(p.read_bytes() if change=='same_bytes_new_inode' else b'broken' if change=='corrupt' else b'{}')
                    other.chmod(0o600);os.replace(other,p)
                elif change=='unsafe_mode': p.chmod(0o644)
                elif change.startswith('lock_'):
                    lock=p.with_name(p.name+'.lock');lock.unlink()
                    if change=='lock_replaced': lock.touch(mode=0o600)
                elif change=='parent_replaced':
                    p.parent.rename(p.parent.with_name('original'));p.parent.mkdir(mode=0o700)
                else:p.unlink()
                before=p.read_bytes() if p.exists() else None
                c.advance(30);self.assertFalse(e.retry_persistence())
                self.assertTrue(e.failed)
                self.assertEqual(p.read_bytes() if p.exists() else None,before)
                self.assertIsNotNone(e.recovery_blocked)

    def test_frozen_memory_changed_or_invalid_is_never_written(self):
        for invalid in (False,True):
            with self.subTest(invalid=invalid),fixture() as (e,s,c,p):
                with fault('before_replace'), self.assertRaises(OSError): submit(e)
                prior=p.read_bytes()
                if invalid:e.state['active']='missing-owner'
                else:e.state['requests']['request-a']['reason']='unexpected mutation'
                c.advance(30);self.assertFalse(e.retry_persistence())
                self.assertIsNotNone(e.recovery_blocked);self.assertEqual(p.read_bytes(),prior)

    def test_monotonic_unknown_or_backward_rearms_bounded_retry(self):
        with fixture() as (e,s,c,p):
            with fault('before_replace'), self.assertRaises(OSError): submit(e)
            c.now=float('nan');self.assertFalse(e.retry_persistence())
            c.now=500;self.assertFalse(e.retry_persistence())
            c.advance(29);self.assertFalse(e.retry_persistence())
            c.advance(1);self.assertTrue(e.retry_persistence())

    def test_unlocked_store_cannot_recover_failed_state(self):
        with fixture() as (e,s,c,p):
            with fault('before_replace'), self.assertRaises(OSError): submit(e)
            s.ownership=None;c.advance(30)
            self.assertFalse(e.retry_persistence());self.assertTrue(e.failed)
            self.assertIsNotNone(e.recovery_blocked)

    def test_startup_never_initializes_missing_or_corrupt_state(self):
        for content in (None,b'{broken'):
            with self.subTest(content=content),fixture() as (e,s,c,p):
                p.unlink()
                if content is not None:p.write_bytes(content);p.chmod(0o600)
                with self.assertRaises((FileNotFoundError,ValueError)):
                    Coordinator(StateFile(p,s.ownership),Health())
                self.assertEqual(p.read_bytes() if p.exists() else None,content)

if __name__=='__main__': unittest.main()
