#!/usr/bin/env python

import asyncio
import asyncssh
import concurrent.futures
import sys

import multiprocessing_on_dill

import config, biothings
biothings.config_for_app(config)

concurrent.futures.process.multiprocessing = multiprocessing_on_dill
from functools import partial
from collections import OrderedDict


import logging

logging.info("Hub DB backend: %s" % config.HUB_DB_BACKEND)
logging.info("Hub database: %s" % config.DATA_HUB_DB_DATABASE)

from biothings.utils.manager import JobManager
loop = asyncio.get_event_loop()
process_queue = concurrent.futures.ProcessPoolExecutor(max_workers=config.HUB_MAX_WORKERS)
thread_queue = concurrent.futures.ThreadPoolExecutor()
loop.set_default_executor(process_queue)
job_manager = JobManager(loop,
                         max_memory_usage=config.HUB_MAX_MEM_USAGE)

from hub.dataload import __sources_dict__ as dataload_sources

import biothings.hub.dataload.dumper as dumper
import biothings.hub.dataload.uploader as uploader

# Check the sources dictionary
print(dataload_sources)

# will check every 10 seconds for sources to upload
upload_manager = uploader.UploaderManager(poll_schedule = '* * * * * */10', job_manager=job_manager)
upload_manager.register_sources(dataload_sources)
upload_manager.poll('upload', lambda doc: upload_manager.upload_src(doc["_id"]))

dmanager = dumper.DumperManager(job_manager=job_manager)
dmanager.register_sources(dataload_sources)
dmanager.schedule_all()

import biothings.hub.databuild.builder as builder
from hub.databuild.builder import InteractionDataBuilder
mappers = [
]
pbuilder = partial(InteractionDataBuilder, mappers=mappers)
bmanager = builder.BuilderManager(
        job_manager=job_manager,
        builder_class=pbuilder,
        poll_schedule="* * * * * */10")
bmanager.configure()
bmanager.poll("build", lambda conf: bmanager.merge(conf["_id"]))

from biothings.utils.hub import schedule, pending, done

COMMANDS = OrderedDict()
# dump commands
COMMANDS["dump"] = dmanager.dump_src
COMMANDS["dump_all"] = dmanager.dump_all
# # upload commands
COMMANDS["upload"] = upload_manager.upload_src
COMMANDS["upload_all"] = upload_manager.upload_all

# indexing j
# COMMANDS["index"] = partial(index_manager.index,"default")

# admin/advanced
EXTRA_NS = {
        "dm": dmanager,
        "um": upload_manager,
        "bm": bmanager,
        "merge": bmanager.merge,
        # "im" : index_manager,
        "jm" : job_manager,
        "loop" : loop,
        "pqueue" : process_queue,
        "tqueue" : thread_queue,
        "g": globals(),
        "sch" : partial(schedule,loop),
        "top" : job_manager.top,
        "pending" : pending,
        "done" : done,
        }

from biothings.utils.hub import start_server
from biothings.utils.hub import HubShell
shell = HubShell()
shell.set_commands(COMMANDS,EXTRA_NS)
server = start_server(loop,"my hub",passwords=config.HUB_PASSWD, port=config.HUB_SSH_PORT, shell=shell)

try:
    loop.run_until_complete(server)
except (OSError, asyncssh.Error) as exc:
    sys.exit('Error starting server: ' + str(exc))

loop.run_forever()
