import setuptools

setuptools.setup(name='bop',
                 packages=[
                   'bop',
                   'bop.bop',
                 ],
                 install_requires=open('requirements.txt').readlines())
