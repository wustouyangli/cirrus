from setuptools import setup, find_packages

setup(
    name='cirrus',
    version='1.0.0',
    description='rpc tool for distributed server',
    author='Ouyang Li',
    author_email='wust_ouyangli@outlook.com',
    include_package_data=True,
    packages=find_packages(),
    install_requires=[
        'thrift>=0.10',
        'kazoo>=2.0',
        'netifaces>=0.10',
        'gevent>=1.3',
        'setproctitle>=1.1',
        'psutil>=5.6'
    ],
    scripts=[
        'cirrus_zk_watcher.py'
    ],
    zip_safe=False
)