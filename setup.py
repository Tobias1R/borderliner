from setuptools import setup, find_packages
import os
import subprocess
# Return the git revision as a string
def git_version():
    def _minimal_ext_cmd(cmd):
        # construct minimal environment
        env = {}
        for k in ['SYSTEMROOT', 'PATH']:
            v = os.environ.get(k)
            if v is not None:
                env[k] = v
        # LANGUAGE is used on win32
        env['LANGUAGE'] = 'C'
        env['LANG'] = 'C'
        env['LC_ALL'] = 'C'
        out = subprocess.Popen(cmd, stdout = subprocess.PIPE, env=env).communicate()[0]
        return out

    try:
        out = _minimal_ext_cmd(['git', 'rev-parse', 'HEAD'])
        GIT_REVISION = out.strip().decode('ascii')
    except OSError:
        GIT_REVISION = "Unknown"

    return GIT_REVISION



with open('requirements.txt') as f:
    install_requires = [pkg for pkg in f.read().strip().split('\n') if not pkg.startswith('borderliner==')]


setup(
    name='borderliner',
    version='0.1.1',
    description='The ultimate data pipeline framework',
    author='Tobias Rocha',
    author_email='tobias_rocha@yahoo.com',
    packages=find_packages(),
    install_requires=install_requires,
    package_data={
        'borderliner': ['admin/templates/*', 'admin/scripts/*']
    },
)
version = git_version()
print(f'installed {version}')