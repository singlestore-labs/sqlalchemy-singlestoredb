# Release process

## Bump the package version and build documentation

Run the following command:
```
resources/bump_version.py < major | minor | patch >

```

This will bump the version number in `setup.cfg` and `sqlalchemy_singlestoredb/__init__.py`
using semantic versioning rules: minor bump for new features, patch bump for
bug fixes. It will generate a list of changes since the last version and
ask for confirmation of the release notes in `docs/src/whatsnew.rst`.
It will then build the documentation using the unified build script.
You will need `sphinx` and `sphinx_rtd_theme` installed for this step.
The documentation build can optionally use Docker to start a SingleStoreDB
server for running documentation examples.


## Commit and push the changes

After verifying the release summary in the documentation, commit the changes.
All modified files should be staged by `bump_version.py` already.
```
git commit -m "Prepare for vX.X.X release" && git push

```

## Final testing

The coverage tests will be triggered by the push, but you should run the full set of
[Coverage tests](https://github.com/singlestore-labs/sqlalchemy-singlestoredb/actions/workflows/coverage.yml)
and then run the Smoke tests which verify the code works at all of the advertised Python versions:
[Smoke test](https://github.com/singlestore-labs/sqlalchemy-singlestoredb/actions/workflows/smoke-test.yml).


## Create the release on Github

To create the release, run:
```
resources/create_release.py

```

This will generate a tag and start the release process in the
[Publish packages](https://github.com/singlestore-labs/sqlalchemy-singlestoredb/actions/workflows/publish.yml)
Github workflow.
