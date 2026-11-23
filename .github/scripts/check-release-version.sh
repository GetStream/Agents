#!/usr/bin/env bash
# Fails unless the Python packages built in <dist-dir> all carry the version of the release tag.
#
# The version is read from the nearest git tag (hatch-vcs). When the tag is not visible, the
# build does not fail: it falls back to 0.0.0 (`fallback_version` in each pyproject.toml), or to
# a dev version from another tag. PyPI never accepts a version twice, so this runs before upload.
#
# Usage: check-release-version.sh <tag> [<dist-dir>]   (tag: vX.Y.Z; dist-dir: dist)
# Writes `version=X.Y.Z` to $GITHUB_OUTPUT when it is set.
set -euo pipefail

tag=${1:?usage: check-release-version.sh <tag> [<dist-dir>]}
dist=${2:-dist}

fail() {
  echo "::error::$*"
  exit 1
}

[[ $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "release tag '$tag' is not vX.Y.Z"
want=${tag#v}

got=$(cd sdks/python && uvx -q --from 'hatch==1.18.2' hatch version)
[[ $got == "$want" ]] || fail "hatch version prints '$got' for release tag '$tag', expected '$want'"

shopt -s nullglob
files=("$dist"/*.whl "$dist"/*.tar.gz)
(( ${#files[@]} > 0 )) || fail "no wheel or sdist in $dist"
for f in "${files[@]}"; do
  name=$(basename "$f")
  case $name in
    # A wheel is {name}-{version}-{python}-{abi}-{platform}.whl, and name has no '-'.
    *.whl) version=$(cut -d- -f2 <<<"$name") ;;
    # An sdist is {name}-{version}.tar.gz.
    *) stem=${name%.tar.gz}; version=${stem##*-} ;;
  esac
  [[ $version == "$want" ]] || fail "$name has version '$version', expected '$want'"
done

echo "${#files[@]} files in $dist, all at version $want"
if [[ -n ${GITHUB_OUTPUT:-} ]]; then
  echo "version=$want" >> "$GITHUB_OUTPUT"
fi
