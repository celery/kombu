from paver.easy import *            # noqa
from paver import doctools          # noqa
from paver.setuputils import setup  # noqa

options(
        sphinx=Bunch(builddir=".build"),
)


def sphinx_builddir(options):
    return path("docs") / options.sphinx.builddir / "html"


@task
def clean_docs(options):
    sphinx_builddir(options).rmtree()


@task
@needs("clean_docs", "paver.doctools.html")
def html(options):
    destdir = path("Documentation")
    destdir.rmtree()
    builtdocs = sphinx_builddir(options)
    builtdocs.move(destdir)


@task
@needs("paver.doctools.html")
def qhtml(options):
    destdir = path("Documentation")
    builtdocs = sphinx_builddir(options)
    sh("rsync -az %s/ %s" % (builtdocs, destdir))


@task
@needs("clean_docs", "paver.doctools.html")
def ghdocs(options):
    builtdocs = sphinx_builddir(options)
    sh("git checkout gh-pages && \
            cp -r %s/* .    && \
            git commit . -m 'Rendered documentation for Github Pages.' && \
            git push origin gh-pages && \
            git checkout master" % builtdocs)


@task
@needs("clean_docs", "paver.doctools.html")
def upload_pypi_docs(options):
    builtdocs = path("docs") / options.builddir / "html"
    sh("python setup.py upload_sphinx --upload-dir='%s'" % (builtdocs))


@task
@needs("upload_pypi_docs", "ghdocs")
def upload_docs(options):
    pass


@task
def autodoc(options):
    sh("contrib/release/doc4allmods kombu")


@task
def verifyindex(options):
    sh("contrib/release/verify-reference-index.sh")


@task
def clean_readme(options):
    path("README").unlink()
    path("README.rst").unlink()


@task
@needs("clean_readme")
def readme(options):
    sh("python contrib/release/sphinx-to-rst.py docs/templates/readme.txt \
            > README.rst")
    sh("ln -sf README.rst README")


@task
@cmdopts([
    ("custom=", "C", "custom version"),
])
def bump(options):
    s = "-- '%s'" % (options.custom, ) \
            if getattr(options, "custom") else ""
    sh("contrib/release/bump_version.py \
            kombu/__init__.py README.rst %s" % (s, ))


@task
@cmdopts([
    ("coverage", "c", "Enable coverage"),
    ("quick", "q", "Quick test"),
    ("verbose", "V", "Make more noise"),
])
def test(options):
    cmd = "nosetests"
    if getattr(options, "coverage", False):
        cmd += " --with-coverage3"
    if getattr(options, "quick", False):
        cmd = "QUICKTEST=1 SKIP_RLIMITS=1 %s" % cmd
    if getattr(options, "verbose", False):
        cmd += " --verbosity=2"
    sh(cmd)


@task
@cmdopts([
    ("noerror", "E", "Ignore errors"),
])
def flake8(options):
    noerror = getattr(options, "noerror", False)
    sh("""flake8 kombu""", ignore_error=noerror)


@task
@cmdopts([
    ("noerror", "E", "Ignore errors"),
])
def flakeplus(options):
    noerror = getattr(options, "noerror", False)
    sh("python contrib/release/flakeplus.py kombu",
       ignore_error=noerror)


@task
@cmdopts([
    ("noerror", "E", "Ignore errors"),
])
def flakes(options):
    flake8(options)
    flakeplus(options)


@task
@cmdopts([
    ("noerror", "E", "Ignore errors"),
])
def pep8(options):
    noerror = getattr(options, "noerror", False)
    return sh("""find kombu -name "*.py" | xargs pep8 | perl -nle'\
            print; $a=1 if $_}{exit($a)'""", ignore_error=noerror)


@task
def removepyc(options):
    sh("find . -name '*.pyc' | xargs rm")


@task
@needs("removepyc")
def gitclean(options):
    sh("git clean -xdn")


@task
@needs("removepyc")
def gitcleanforce(options):
    sh("git clean -xdf")


@task
@needs("flakes", "autodoc", "verifyindex", "test", "gitclean")
def releaseok(options):
    pass


@task
@needs("releaseok", "removepyc", "upload_docs")
def release(options):
    pass
