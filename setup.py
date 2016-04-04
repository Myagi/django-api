from setuptools import setup
from pip.req import parse_requirements

install_reqs = parse_requirements('./requirements.txt', session=False)
reqs = [str(ir.req) for ir in install_reqs]

setup(name='django-simple-api',
      version='0.1',
      description='Builds on django-rest-framework to make developing a powerful Django API dead simple.',
      url='https://github.com/Myagi/django-simple-api',
      author='Alex McLeod',
      author_email='alex.mcleod@myagi.com',
      license='MIT',
      packages=['simple_api'],
      install_requires=reqs,
      zip_safe=False)
