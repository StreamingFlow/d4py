# Copyright (c) The University of Edinburgh 2014-2015
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import time


class Timer:
    def __init__(self, verbose: bool = False) -> None:
        self.verbose = verbose

    def __enter__(self) -> Timer:
        self.start = time.time()
        return self

    def __exit__(self, *args: list[str]) -> None:
        self.end = time.time()
        self.secs = self.end - self.start


class MonitoringWrapper:
    def __init__(self, baseObject: object) -> None:
        self.__class__ = type(
            baseObject.__class__.__name__,
            (self.__class__, baseObject.__class__),
            {},
        )
        self.__dict__ = baseObject.__dict__
        self.baseObject = baseObject


class ReadTimingWrapper(MonitoringWrapper):
    def __init__(self, baseObject: object) -> None:
        MonitoringWrapper.__init__(self, baseObject)
        self.readtime: float | None = None
        self.readrate = []

    def _read(self):
        now = time.time()
        if self.readtime:
            self.readrate.append(now - self.readtime)
        self.readtime = now
        return self.baseObject._read()

    def _terminate(self):
        self.log(
            f"Average read rate : {sum(self.readrate) / float(len(self.readrate))}",
        )
        self.baseObject._terminate()


class ProcessTimingPE(MonitoringWrapper):
    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self.times_total = 0
        self.times_count = 0

    def process(self, inputs):
        with Timer() as t:
            result = self.baseObject.process(inputs)
        self.times_total += t.secs
        self.times_count += 1
        return result

    def _postprocess(self):
        self.log(f"Average processing time: {self.times_total / self.times_count}")
