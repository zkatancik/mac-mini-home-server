"""Mocked process results exercise actual inspection/poll/state; no encode/API."""
import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock
import test_media_normalizer as fixtures
m = fixtures.media_normalizer

@contextlib.contextmanager
def result_fixture(root, code, *, wrapped=True, stderr=b'fixture diagnostic'):
    root=Path(root).resolve();client,token=root/'client.py',root/'token'
    client.write_text('# fixture only');token.write_text('fixture only')
    def create_process(*_args, **_kwargs):
        process=mock.Mock(returncode=code);process.stdout,process.stderr=io.BytesIO(b''),io.BytesIO(stderr)
        process.poll.return_value=code
        return process
    env={'MEDIA_NORMALIZER_MEMORY_COORDINATOR_URL':'http://fixture.invalid' if wrapped else '',
         'MEDIA_NORMALIZER_MEMORY_CLIENT_FILE':str(client),'MEDIA_NORMALIZER_MEMORY_TOKEN_FILE':str(token)}
    with mock.patch.dict(os.environ,env),mock.patch.object(m.subprocess,'Popen',side_effect=create_process) as launch:yield launch

class TransientAdmissionTests(unittest.TestCase):
    def poll_fixture(self,root,code,expected,*,wrapped=True):
        root=Path(root).resolve();path=root/'fixture.mkv';path.write_bytes(b'unchanged fixture media')
        before=path.stat();config=fixtures.settings(root);state=m.empty_state()
        item=m.ManagedMedia('sonarr',1,2,path,'2001-01-01T00:00:00Z')
        probe=fixtures.media_probe(fixtures.video_stream(codec='av1'))
        with result_fixture(root,code,wrapped=wrapped,stderr=b'admission wait deadline reached'), \
             mock.patch.object(m,'probe_media',return_value=probe),mock.patch.object(m,'log'), \
             mock.patch.object(m,'create_rollback') as rollback,mock.patch.object(m,'replace_atomically') as replace:
            status,failed=m.process_poll_item(item,config,state,{},dry_run=False,source='fixture')
        self.assertEqual(status,expected)
        record=m.load_state(config.state_path)['files'][str(path)];self.assertEqual(record['status'],expected)
        self.assertEqual(path.read_bytes(),b'unchanged fixture media')
        self.assertEqual((path.stat().st_dev,path.stat().st_ino,path.stat().st_size),(before.st_dev,before.st_ino,before.st_size))
        rollback.assert_not_called();replace.assert_not_called()
        priority,unseen=m.select_poll_work([item],state,{'sonarr':'2001-06-01T00:00:00Z'},m.parse_datetime('2001-11-05T00:00:00Z'))
        if expected in ('capacity_deferred','interrupted'):
            self.assertTrue(failed);self.assertEqual(priority,[item]);self.assertEqual(unseen,[])
            self.assertNotEqual(record.get('decision'),'report_only_decode_failure')
            self.assertNotIn('decode_samples',record.get('metrics',{}))
        else:
            self.assertFalse(failed);self.assertEqual(priority,[]);self.assertEqual(unseen,[])
            self.assertEqual(record['decision'],'report_only_decode_failure')
        return record
    def test_admission_expiry_persists_retryable_same_identity_through_poll(self):
        with tempfile.TemporaryDirectory() as root:self.poll_fixture(root,75,'capacity_deferred')
    def test_lease_loss_cancellation_and_signal_exit_stay_retryable(self):
        for code in (124,130,137,143,-15):
            with self.subTest(code=code),tempfile.TemporaryDirectory() as root:self.poll_fixture(root,code,'interrupted')
    def test_genuine_decode_error_remains_nonretryable(self):
        with tempfile.TemporaryDirectory() as root:self.poll_fixture(root,1,'report_only_decode_failure')
    def test_unwrapped_reserved_exit_codes_are_still_decoder_results(self):
        for code in (75,124,130):
            with self.subTest(code=code),tempfile.TemporaryDirectory() as root:self.poll_fixture(root,code,'report_only_decode_failure',wrapped=False)
    def test_transient_wrapped_exit_raises_regardless_of_check(self):
        for code,status in ((75,'capacity_deferred'),(124,'interrupted'),(130,'interrupted')):
            for check in (False,True):
                with self.subTest(code=code,check=check),tempfile.TemporaryDirectory() as root:
                    with result_fixture(root,code),self.assertRaises(m.TransientMediaProcessError) as caught:m.run_process(['ffmpeg','-f','null','-'],timeout=1,check=check)
                    self.assertEqual(caught.exception.status,status)
    def test_all_serial_samples_share_private_receipt_without_creating_parent(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root).resolve()
            receipt=root/'.config/home-server-media-normalizer/memory-admission.json'
            with mock.patch.object(m.Path,'home',return_value=root),result_fixture(root,0) as launch:
                samples=m.sample_decode(root/'fixture.mkv',fixtures.media_probe(),'ffmpeg')
            self.assertEqual(len(samples),3)
            self.assertEqual(launch.call_count,3)
            for call in launch.call_args_list:
                args=call.args[0];self.assertEqual(args[args.index('--receipt-file')+1],str(receipt))
                self.assertLess(args.index('--receipt-file'),args.index('--'))
                self.assertIn('--native',args)
            self.assertFalse(receipt.parent.exists())
    def test_unsafe_existing_receipt_parent_is_never_modified_by_normalizer(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root).resolve();parent=root/'.config/home-server-media-normalizer';parent.mkdir(parents=True,mode=0o755);parent.chmod(0o755)
            marker=parent/'existing';marker.write_bytes(b'preserve')
            with mock.patch.object(m.Path,'home',return_value=root),result_fixture(root,75):
                with self.assertRaises(m.TransientMediaProcessError):m.run_process(['ffmpeg','-f','null','-'],timeout=1,check=False)
            self.assertEqual(parent.stat().st_mode&0o777,0o755);self.assertEqual(marker.read_bytes(),b'preserve')
    def test_unwrapped_command_does_not_claim_a_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            with result_fixture(root,0,wrapped=False) as launch:m.run_process(['ffmpeg','-f','null','-'],timeout=1)
            self.assertNotIn('--receipt-file',launch.call_args.args[0])
    def test_non_ffmpeg_nonzero_result_is_unchanged(self):
        result=subprocess.CompletedProcess(['lsof'],75,'','fixture')
        with mock.patch.object(m.subprocess,'run',return_value=result):self.assertIs(m.run_process(['lsof'],check=False),result)
    def test_dry_run_transient_does_not_write_state(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root).resolve();path=root/'fixture.mkv';path.write_bytes(b'original');config=fixtures.settings(root);state=m.empty_state()
            with result_fixture(root,75),mock.patch.object(m,'probe_media',return_value=fixtures.media_probe(fixtures.video_stream(codec='av1'))):
                with self.assertRaises(m.TransientMediaProcessError):m.normalize_one(path,config,state,dry_run=True)
            self.assertFalse(config.state_path.exists());self.assertEqual(state['files'],{})
    def test_direct_inspection_interrupt_is_saved_before_propagation(self):
        for stop in (KeyboardInterrupt,SystemExit):
            with self.subTest(stop=stop),tempfile.TemporaryDirectory() as root:
                root=Path(root).resolve();path=root/'fixture.mkv';path.write_bytes(b'original');config=fixtures.settings(root);state=m.empty_state()
                with mock.patch.object(m,'inspect_path',side_effect=stop):
                    with self.assertRaises(stop):m.normalize_one(path,config,state,dry_run=False)
                record=m.load_state(config.state_path)['files'][str(path)]
                self.assertEqual(record['status'],'interrupted');self.assertEqual(record['file_identity'],m.current_file_identity(path));self.assertEqual(path.read_bytes(),b'original')
    def test_encode_stage_transient_keeps_retryable_status_and_original(self):
        for status in ('capacity_deferred','interrupted'):
            with self.subTest(status=status),tempfile.TemporaryDirectory() as root:
                root=Path(root).resolve();path=root/'fixture.mkv';path.write_bytes(b'original');config=fixtures.settings(root);state=m.empty_state()
                report={'decision':'normalize','signatures':['incomplete_hvcc'],'frame_rate':'24000/1001','file_identity':m.current_file_identity(path)}
                with mock.patch.object(m,'inspect_path',return_value=report),mock.patch.object(m,'probe_media',return_value=fixtures.media_probe()), \
                     mock.patch.object(m,'require_stable'),mock.patch.object(m,'require_inactive'),mock.patch.object(m,'require_free_space'),mock.patch.object(m,'log'), \
                     mock.patch.object(m,'run_process',side_effect=m.TransientMediaProcessError('fixture',status=status)):
                    with self.assertRaises(m.TransientMediaProcessError):m.normalize_one(path,config,state,dry_run=False)
                self.assertEqual(m.load_state(config.state_path)['files'][str(path)]['status'],status);self.assertEqual(path.read_bytes(),b'original');self.assertEqual(list(root.glob('.*.media-normalizer.*')),[])

if __name__=='__main__':unittest.main(verbosity=2)
