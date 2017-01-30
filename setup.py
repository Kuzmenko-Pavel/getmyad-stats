import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.txt')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.txt')) as f:
    CHANGES = f.read()

requires = [
    "Pillow==2.9.0",
    # Pygments==1.5
    # SOAPpy==0.12.0
    # amqp==1.0.13
    "amqplib==1.0.2",
    # anyjson==0.3.3
    # argparse==1.2.1
    # billiard==2.7.3.34
    "celery==3.0.25",
    # chardet==2.0.1
    # fpconst==0.7.2
    # html5lib==0.999
    # pylibmc==1.5.0
    "pymongo==3.2.2",
    "pymssql==2.0.1",
    # python-dateutil==2.5.3
    # python-memcached==1.57
    # pytz==2013.9
    # reportbug==6.4.4
    # simplejson==3.8.0
    # six==1.10.0
    # urllib3==1.9.1
    # wsgiref==0.1.2
    "xlwt==0.7.5"
]

setup(name='yottos-getmyad-stats',
      version='0.0',
      description='yottos-getmyad-stats',
      long_description=README.md + '\n\n' + CHANGES.md,
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
