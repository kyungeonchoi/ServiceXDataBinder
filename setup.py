import setuptools
import codecs
import os.path


with open("README.md") as fh:
    long_description = fh.read()

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")

setuptools.setup(name="servicex_databinder",
                 version=get_version("servicex_databinder/__init__.py"),
                 packages=setuptools.find_packages(exclude=['tests']),
                 description="ServiceX data management using a configuration file",
                 long_description=long_description,
                 long_description_content_type='text/markdown',
                 author="KyungEon Choi (UT Austin)",
                 author_email="kyungeonchoi@utexas.edu",
                 url="https://github.com/kyungeonchoi/ServiceXDataBinder",
                 license="BSD 3-clause",
                 python_requires='>=3.7',
                 install_requires=[
                    "servicex>=2.5",
                    "tcut-to-qastle>=0.7",                    
                    "nest-asyncio>=1.5.1",
                    "tqdm>=4.60.0",
                    "pyarrow>=3.0.0",
                    "backoff>=1.11.1",
                    "func_adl_servicex>=1.1"
                    ],
                 classifiers=[
                    "Development Status :: 3 - Alpha",
                    "Intended Audience :: Developers",
                    "Intended Audience :: Information Technology",
                    "Intended Audience :: Science/Research",
                    "Programming Language :: Python",
                    "Topic :: Scientific/Engineering :: Physics",
                    "Topic :: Software Development",
                    "Topic :: Utilities",
                    ],
                 platforms="Any",)
