from setuptools import setup

setup(name='runner',
      version='0.0.0',
      description='HPC python wrapper',
      url='https://github.com/dfguan/runner',
      author='Dengfeng Guan',
      author_email='dfguan9@gmail.com',
      license='MIT',
      packages=['runner'],
      package_dir={'runner':'runner'},
      package_data={'runner': ['sys.config']},
      zip_safe=False)
