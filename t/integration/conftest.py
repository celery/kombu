from __future__ import absolute_import, unicode_literals

import os
import subprocess


def pytest_sessionfinish(session, exitstatus):
    tox_env_dir = os.environ.get('TOX_WORK_DIR')
    if exitstatus and tox_env_dir:
        subprocess.call(["bash", "./rabbitmq_logs.sh"])
