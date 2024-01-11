# Copyright (c) 
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

import os

from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    print("installing dispel4py")
    with open(os.path.join(os.path.dirname(__file__), fname)) as file:
        return file.read()

with open('requirements.txt', 'r') as f:
    install_requires = list()
    dependency_links = list()
    for line in f:
        re = line.strip()
        if re:
            if re.startswith('git+') or re.startswith('svn+') or re.startswith('hg+'):
                dependency_links.append(re)
            else:
                install_requires.append(re)


setup(
  name="stream_d4py",
    version="2.8",
    description="dispel4py is a free and open-source Python library for describing abstract stream-based workflows for distributed data-intensive applications.",
    license="Apache 2",
    author='Rosa Filgueira and Amy Krauser',
    author_email='rosa.filgueira.vicente@gmail.com',
    keywords="updated dispel4py dispel workflows processing elements data intensive",
    url='https://github.com/StreamingFlow/d4py/',
    long_description=read("README.md"),
    long_description_content_type='text/markdown',
    packages=["dispel4py", "dispel4py.new", "dispel4py.examples", "dispel4py.examples.graph_testing"],
    install_requires=install_requires,
    python_requires=">=3.10",
    entry_points={"console_scripts": ["dispel4py = dispel4py.new.processor:main"]},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
)
