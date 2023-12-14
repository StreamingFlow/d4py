# Copyright (c) The University of Edinburgh 2014
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

import pickle
import traceback
from importlib import import_module

import storm
from output_writer import OutputWriter


class SourceWrapper(storm.Spout):
    def initialize(self, conf):
        try:
            self.counter = 0
            self.modname = conf["dispel4py.module"]
            self.scriptname = conf["dispel4py.script"]

            scriptconfig = (
                pickle.loads(str(conf["dispel4py.config"]))
                if "dispel4py.config" in conf
                else {}
            )

            storm.log("Dispel4Py ------> loading script %s" % self.scriptname)
            mod = import_module(self.modname)
            self.script = getattr(mod, self.scriptname)()
            for key, value in scriptconfig.iteritems():
                storm.log(
                    f"Dispel4Py ------> {self.scriptname}: setting attribute {key}",
                )
                setattr(self.script, key, value)
            storm.log("Dispel4Py ------> loaded script %s" % self.scriptname)

            # attach an output writer to each output connection
            for outputname, output in self.script.outputconnections.iteritems():
                output["writer"] = OutputWriter(self.scriptname, outputname)

            # pre-processing if required
            self.script.preprocess()
            storm.log(
                f"Dispel4Py ------> {self.scriptname}: preprocess() completed.",
            )
        except Exception as e:
            storm.log(
                f"Dispel4Py ------> {self.scriptname}: Exception {e},"
                f"Traceback: {traceback.format_exc()}",
            )
            raise

    def next_tuple(self):
        try:
            input_tuple = None
            try:
                input_tuple = self.script._static_input.pop(0)
            except AttributeError:
                # there is no static input
                if self.counter >= self.script._num_iterations:
                    return
            except IndexError:
                # static input is empty - no more processing
                return
            storm.log(
                f"Dispel4Py ------> {self.scriptname}: input {input_tuple}",
            )
            outputs = self.script.process(input_tuple)
            if outputs is None:
                return
            for streamname, output in outputs.iteritems():
                result = output if isinstance(output, list) else [output]
                storm.emit(result, stream=streamname, id=self.counter)
                storm.log(
                    "Dispel4Py ------> {}: emitted tuple {} to stream {}".format(
                        self.script.id, result, streamname,
                    ),
                )
                self.counter += 1
        except Exception as e:
            # logging the error but it should be passed to client somehow
            storm.log(
                f"Dispel4Py ------> {self.scriptname}: Exception {e},"
                f"Traceback: {traceback.format_exc()}",
            )


if __name__ == "__main__":
    SourceWrapper().run()
