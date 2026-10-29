#!/usr/bin/env bash
# Prints the section "## <version>" of CHANGELOG.md, without its heading, up to the next "## " heading.
#
#   .github/scripts/release-notes.sh <version> [changelog]
#
# Exits non-zero when the section does not exist or is empty.

set -euo pipefail

VERSION="${1:?usage: release-notes.sh <version> [changelog]}"
CHANGELOG="${2:-CHANGELOG.md}"

notes="$(awk -v heading="## ${VERSION}" '
  { sub(/\r$/, "") }
  /^## / { if (found) exit; if ($0 == heading) { found = 1; next } }
  found { print }
' "$CHANGELOG")"

# drop leading and trailing blank lines
notes="$(printf '%s\n' "$notes" | sed -e '/./,$!d' | sed -e ':a' -e '/^\n*$/{$d;N;ba' -e '}')"

if [ -z "$notes" ]; then
  printf 'release-notes: no section "## %s" in %s\n' "$VERSION" "$CHANGELOG" >&2
  exit 1
fi
printf '%s\n' "$notes"
