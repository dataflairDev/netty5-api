#!/usr/bin/env bash
# Publishes netty-connection to Maven Central (Central Portal) and tags the release.
#
#   ./release.sh <version> [--dry-run]
#
# --dry-run runs every check and the full build, publishes into build/release-dry-run/m2 (not ~/.m2) and
# verifies the signatures; it changes nothing in git and uploads nothing.
#
# Credentials are read by Gradle from ~/.gradle/gradle.properties or ORG_GRADLE_PROJECT_* environment
# variables. This script only checks that they exist and never prints them.

set -euo pipefail

die() { printf 'release: error: %s\n' "$*" >&2; exit 1; }
info() { printf 'release: %s\n' "$*"; }
warn() { printf 'release: warning: %s\n' "$*" >&2; }

usage() { printf 'usage: %s <version> [--dry-run]\n' "$0" >&2; exit 2; }

VERSION=""
DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    -h|--help) usage ;;
    -*) printf 'release: unknown option %s\n' "$arg" >&2; usage ;;
    *) [ -z "$VERSION" ] || usage; VERSION="$arg" ;;
  esac
done
[ -n "$VERSION" ] || usage

[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] \
  || die "'$VERSION' is not a version like 2.0.0"
[[ "$VERSION" != *SNAPSHOT* ]] || die "snapshot versions are not released through this script"

TAG="v$VERSION"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

# ---------------------------------------------------------------------------------------------- git checks

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "$ROOT is not a git work tree"

dirty=""
while IFS= read -r line; do
  [ -n "$line" ] || continue
  path="${line:3}"
  case "$path" in
    .idea|.idea/*|\".idea/*) warn "ignoring IDE change: $line" ;;
    *) dirty+="  $line"$'\n' ;;
  esac
done < <(git status --porcelain --untracked-files=all)
[ -z "$dirty" ] || die "work tree is not clean, commit or stash first:"$'\n'"$dirty"

branch="$(git symbolic-ref --short -q HEAD || true)"
[ "$branch" = "main" ] || die "releases are made from 'main', current branch is '${branch:-detached HEAD}'"

info "fetching origin"
git fetch --quiet origin main --tags || die "git fetch origin failed"
local_sha="$(git rev-parse HEAD)"
remote_sha="$(git rev-parse origin/main)"
if [ "$local_sha" != "$remote_sha" ]; then
  ahead="$(git rev-list --count origin/main..HEAD)"
  behind="$(git rev-list --count HEAD..origin/main)"
  die "main is not in sync with origin/main ($ahead ahead, $behind behind)"
fi

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  die "tag $TAG already exists locally"
fi
if git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1; then
  die "tag $TAG already exists on origin"
fi

# -------------------------------------------------------------------------------------- credential checks

USER_PROPS="${GRADLE_USER_HOME:-$HOME/.gradle}/gradle.properties"

# true if Gradle property $1 is set (non-empty) via ORG_GRADLE_PROJECT_$1 or the user gradle.properties
has_prop() {
  local name="$1" value
  value="$(printenv "ORG_GRADLE_PROJECT_$name" 2>/dev/null || true)"
  [ -n "$value" ] && return 0
  [ -f "$USER_PROPS" ] || return 1
  local escaped="${name//./\\.}"
  grep -Eq "^[[:space:]]*${escaped}[[:space:]]*[=:][[:space:]]*[^[:space:]]" "$USER_PROPS"
}

missing=()
has_prop mavenCentralUsername || missing+=("mavenCentralUsername")
has_prop mavenCentralPassword || missing+=("mavenCentralPassword")
if has_prop signingInMemoryKey; then
  info "signing: in-memory key"
elif has_prop signing.keyId && has_prop signing.password && has_prop signing.secretKeyRingFile; then
  info "signing: key ring file"
else
  missing+=("signingInMemoryKey (or signing.keyId + signing.password + signing.secretKeyRingFile)")
fi
if [ "${#missing[@]}" -gt 0 ]; then
  printf 'release: error: missing Gradle properties (set them in %s or as ORG_GRADLE_PROJECT_<name>):\n' "$USER_PROPS" >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

# --------------------------------------------------------------------------------------------- version

current_version="$(sed -n 's/^VERSION_NAME=\([^[:space:]]*\).*/\1/p' gradle.properties | head -n 1)"
[ -n "$current_version" ] || die "VERSION_NAME not found in gradle.properties"

GRADLE=(./gradlew --no-configuration-cache "-PVERSION_NAME=$VERSION")
start_sha="$local_sha"
version_committed=false
published=false

rollback() {
  local status=$?
  if [ "$status" -ne 0 ] && [ "$version_committed" = true ] && [ "$published" = false ]; then
    warn "rolling back the version commit"
    git reset --quiet --soft "$start_sha" && git checkout --quiet "$start_sha" -- gradle.properties \
      || warn "rollback failed, run: git reset --soft $start_sha && git checkout $start_sha -- gradle.properties"
  fi
  exit "$status"
}
trap rollback EXIT

if [ "$current_version" != "$VERSION" ]; then
  if [ "$DRY_RUN" = true ]; then
    info "gradle.properties has VERSION_NAME=$current_version, building $VERSION without changing it (dry run)"
  else
    info "setting VERSION_NAME=$VERSION in gradle.properties"
    sed -i.bak "s/^VERSION_NAME=.*/VERSION_NAME=$VERSION/" gradle.properties && rm -f gradle.properties.bak
    git commit --quiet -m "release $VERSION" -- gradle.properties || die "could not commit the version change"
    version_committed=true
  fi
fi

# ------------------------------------------------------------------------------------------------ build

info "building and testing $VERSION"
"${GRADLE[@]}" clean build || die "build failed"

if [ "$DRY_RUN" = true ]; then
  DRY_REPO="$ROOT/build/release-dry-run/m2"
  info "dry run: publishing into $DRY_REPO and signing"
  "${GRADLE[@]}" "-Dmaven.repo.local=$DRY_REPO" publishToMavenLocal signMavenPublication \
    || die "local publication or signing failed"

  group="$(sed -n 's/^GROUP=\([^[:space:]]*\).*/\1/p' gradle.properties | head -n 1)"
  dir="$DRY_REPO/${group//.//}/netty-connection/$VERSION"
  base="netty-connection-$VERSION"
  for f in "$base.jar" "$base-sources.jar" "$base-javadoc.jar" "$base.pom" "$base.module"; do
    [ -s "$dir/$f" ] || die "missing $dir/$f"
    [ -s "$dir/$f.asc" ] || die "missing signature $dir/$f.asc"
  done
  info "artifacts and signatures in $dir:"
  (cd "$dir" && ls -1)
  info "dry run finished: nothing was uploaded, tagged or pushed"
  exit 0
fi

info "publishing $VERSION to Maven Central"
"${GRADLE[@]}" publishAndReleaseToMavenCentral || die "publishing to Maven Central failed"
published=true

# ------------------------------------------------------------------------------------------- tag + push

git tag -a "$TAG" -m "release $VERSION" || die "$VERSION is published, but creating tag $TAG failed"
if [ "$version_committed" = true ]; then
  git push --atomic origin "HEAD:refs/heads/main" "refs/tags/$TAG" \
    || die "$VERSION is published, but pushing main and $TAG failed; push them by hand"
else
  git push origin "refs/tags/$TAG" || die "$VERSION is published, but pushing $TAG failed; push it by hand"
fi

info "released $VERSION and pushed $TAG"
