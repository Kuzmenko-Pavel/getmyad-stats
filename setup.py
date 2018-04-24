import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.md')) as f:
    CHANGES = f.read()

requires = [
    "Pillow==2.9.0",
    "amqplib==1.0.2",
    "celery==3.1.7",
    "pymongo==3.6.1",
    "pymssql==2.1.3",
    "xlwt==0.7.5",
    "python-memcached==1.57",
    'requests',
    'datetime'
]

setup(name='yottos-getmyad-stats',
      version='0.0',
      description='yottos-getmyad-stats',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
          "Programming Language :: Python",
      ],
      author='Pavel Kuzmenko',
      author_email='kyzmenko.pavel@gmail.com',
      url='',
      keywords='celery',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=requires,
      )
