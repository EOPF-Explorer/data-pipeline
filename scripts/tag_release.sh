#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' >&2
Usage: tag_release.sh [-f|--force] <tag> [message]

  <tag>     Annotated tag to create (semantic version, e.g. v1.2.0)
  [message] Optional tag message (defaults to "Release <tag>")

Options:
  -f, --force   Move an existing tag locally and on origin (force-with-lease)
  -h, --help    Show this help and exit
EOF
}

die() {
  printf 'Error: %s\n' "$1" >&2
  exit 1
}

FORCE=0
TAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--force)
      FORCE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      die "unknown option: $1"
      ;;
    *)
      TAG="$1"
      shift
      break
      ;;
  esac
done

[[ -n "$TAG" ]] || { usage; exit 1; }

MESSAGE=${*:-"Release ${TAG}"}

[[ "$TAG" =~ ^v[0-9]+(\.[0-9]+)*$ ]] || die "tag must follow semantic style (e.g. v1.2.0)."

if [[ -n $(git status --porcelain) ]]; then
  die "working tree has uncommitted changes."
fi

CURRENT_BRANCH=$(git symbolic-ref --short HEAD)
[[ "$CURRENT_BRANCH" == "main" ]] || die "releases must be created from main (current: $CURRENT_BRANCH)."

REMOTE=origin
git config --get remote."$REMOTE".url >/dev/null 2>&1 || die "remote '$REMOTE' is not configured."

git fetch "$REMOTE" main --tags --quiet >/dev/null 2>&1 || die "failed to fetch $REMOTE/main."
[[ $(git rev-parse HEAD) == $(git rev-parse "$REMOTE/main") ]] || die "local main is out of sync with $REMOTE/main."

if git rev-parse "$TAG" >/dev/null 2>&1 && [[ $FORCE -eq 0 ]]; then
  die "tag '$TAG' already exists locally. Use --force to move it."
fi

if git ls-remote --tags "$REMOTE" "$TAG" | grep -q "refs/tags/$TAG$" && [[ $FORCE -eq 0 ]]; then
  die "tag '$TAG' already exists on $REMOTE. Use --force to replace it."
fi

if [[ $FORCE -eq 1 ]]; then
  git tag -fa "$TAG" -m "$MESSAGE"
  git push "$REMOTE" "$TAG" --force-with-lease
  echo "Moved tag '$TAG' to $(git rev-parse --short HEAD) and pushed with force-with-lease."
else
  git tag -a "$TAG" -m "$MESSAGE"
  git push "$REMOTE" "$TAG"
  echo "Created tag '$TAG' at $(git rev-parse --short HEAD) and pushed to $REMOTE."
fi
