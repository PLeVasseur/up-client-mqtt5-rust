#!/usr/bin/env python3
# Copyright (c) 2026 Contributors to the Eclipse Foundation
# SPDX-License-Identifier: Apache-2.0
"""Unit controls for build configuration; these do not test MQTT interoperability."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class DetectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.directory = Path(cls.temporary.name)
        cls.binary = cls.directory / 'detect'
        subprocess.run(['rustc', '+' + TOOLCHAIN, '--edition=2021', '-Dwarnings',
                        str(ROOT / 'build.rs'), '-o', str(cls.binary)], check=True)
        # Only this configuration unit test uses a controlled CLI response.
        # Actual broker acceptance separately requires the real Docker daemon.
        docker = cls.directory / 'docker'
        docker.write_text('#!/bin/sh\ntest "$*" = "image inspect eclipse-mosquitto:2.0" || exit 9\nexit "$DETECTION_STATUS"\n')
        docker.chmod(0o755)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def run_detection(self, status, required=None):
        env = os.environ.copy()
        env.pop('UP_MQTT5_REQUIRE_BROKER', None)
        env.update(PATH=str(self.directory), DETECTION_STATUS=str(status))
        if required is not None:
            env['UP_MQTT5_REQUIRE_BROKER'] = required
        return subprocess.run([str(self.binary)], env=env, capture_output=True, text=True, timeout=5)

    def test_required_missing_image_fails_instead_of_omitting_tests(self):
        result = self.run_detection(1, '1')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('broker acceptance requires Docker', result.stderr)
        self.assertNotIn('rustc-cfg=docker_available', result.stdout)

    def test_present_image_enables_tests_and_records_rerun_inputs(self):
        result = self.run_detection(0, '1')
        self.assertEqual(result.returncode, 0)
        self.assertIn('cargo::rustc-cfg=docker_available', result.stdout)
        self.assertIn('cargo::rerun-if-env-changed=DOCKER_HOST', result.stdout)
        self.assertIn('cargo::rerun-if-env-changed=UP_MQTT5_REQUIRE_BROKER', result.stdout)

    def test_optional_missing_image_does_not_claim_broker_availability(self):
        for required in [None, '0']:
            with self.subTest(required=required):
                result = self.run_detection(1, required)
                self.assertEqual(result.returncode, 0)
                self.assertNotIn('rustc-cfg=docker_available', result.stdout)
                self.assertIn('prepared eclipse-mosquitto:2.0 image', result.stdout)

    def test_invalid_required_value_fails(self):
        result = self.run_detection(0, 'yes')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('must be 0 or 1', result.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--toolchain', default='1.95.0')
    args = parser.parse_args()
    TOOLCHAIN = args.toolchain
    ROOT = Path(__file__).resolve().parents[1]
    unittest.main(argv=[__file__], verbosity=2)
