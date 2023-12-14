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
from output_writer import OutputWriter, decode_types, encode_types


class SimpleWrapper(storm.BasicBolt):
    def initialize(self, conf):
        try:
            self.modname = conf["dispel4py.module"]
            self.scriptname = conf["dispel4py.script"]
            self.inputmapping = conf["dispel4py.inputmapping"]
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

            self.boltId = (
                f"{self.scriptname} ({self.script.boltId})"
                if hasattr(self.script, "boltId")
                else self.scriptname
            )
            # pre-processing if required
            self.script.preprocess()
            storm.log(f"Dispel4Py ------> {self.boltId}: preprocess() completed.")
        except:
            storm.log(
                f"Dispel4Py ------> {self.scriptname}: {traceback.format_exc()}",
            )
            raise

    def process(self, tup):
        # storm.log("Dispel4Py ------> %s: Received block."
        #    % (self.script.id, ))
        try:
            inputname = self.inputmapping[tup.component][tup.stream]
            storm.log(
                "Dispel4Py ------> {}: Received block at input '{}'".format(
                    self.script.id,
                    inputname,
                ),
            )
            # inputs = tup.values
            inputs = decode_types(tup.values)
            outputs = self.script.process({inputname: inputs})
            # storm.log("Dispel4Py ------> %s: Processing complete."\
            #    % self.scriptname)

            if outputs is None:
                return
            for streamname, output in outputs.iteritems():
                result = output if isinstance(output, list) else [output]
                try:
                    storm.emit(result, stream=streamname)
                    storm.log(
                        "Dispel4Py ------> {}: Emitted to stream {}: {}".format(
                            self.script.id, streamname, str(result)[:200],
                        ),
                    )
                except TypeError:
                    # encode manually
                    encoded = encode_types(result)
                    storm.emit(encoded, stream=streamname)
                    storm.log(
                        "Dispel4Py ------> {}: Emitted to stream {}".format(
                            self.script.id, streamname,
                        ),
                    )
                # except:
                #     storm.log("%s: %s"
                #               % (self.script.id, traceback.format_exc()))
        except Exception as e:
            storm.log(
                f"Dispel4Py ------> {self.script.id}: Exceptio {e},"
                f"Traceback: {traceback.format_exc()}",
            )


if __name__ == "__main__":
    SimpleWrapper().run()
