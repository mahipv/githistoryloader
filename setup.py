from setuptools import setup

setup(
    name='tsgitloader',
    version='0.1.0',
    author='Mahi',
    author_email='your@email.com',
    description='A simple tool for loading Git repository history.',
    url='https://github.com/mahipv/githistoryloader',
    py_modules=['tsgitloader', 'toolchainutils'],
    install_requires=[
        'GitPython',
        'langchain',
        'llama_index',
        'pandas',
        'psycopg2',
        'requests',
        'timescale_vector'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
