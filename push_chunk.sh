#!/usr/bin/env bash
# Push data/ to GitHub in small incremental commits.
# Usage:
#   ./git_push_data.sh                    # all regions
#   ./git_push_data.sh --region sannio    # single region
#   ./git_push_data.sh --dry-run

set -euo pipefail

DATA_DIR="./data"
BATCH_SIZE=300
DRY_RUN=0
FILTER_REGION=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --region) FILTER_REGION="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

commit_and_push() {
  local msg="$1"; shift
  [[ $# -eq 0 ]] && return
  git add -- "$@" 2>/dev/null || true
  if git diff --cached --quiet; then
    echo "  (nothing new) $msg"
    git reset HEAD -- "$@" 2>/dev/null || true
    return
  fi
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "  [dry-run] $msg ($# files)"
    git reset HEAD -- "$@" 2>/dev/null || true
    return
  fi
  git commit -m "$msg"
  git push origin main
  echo "  ✓ $msg"
}

push_batches() {
  local prefix="$1"; shift
  local files=("$@")
  local total=${#files[@]}
  [[ $total -eq 0 ]] && return
  local i=0
  while [[ $i -lt $total ]]; do
    local batch=("${files[@]:$i:$BATCH_SIZE}")
    local end=$((i + ${#batch[@]}))
    commit_and_push "$prefix [$((i+1))-${end}/${total}]" "${batch[@]}"
    i=$end
  done
}

collect_files() {
  # macOS-compatible find -> array
  local pattern="$1"
  local dir="$2"
  local exclude="${3:-}"
  local files=()
  while IFS= read -r f; do
    files+=("$f")
  done < <(find "$dir" -name "$pattern" -not -name "._*" ${exclude:+-not -name "$exclude"} | sort)
  echo "${files[@]:-}"
}

for region_dir in "$DATA_DIR"/*/; do
  [[ -d "$region_dir" ]] || continue
  region=$(basename "$region_dir")
  [[ -n "$FILTER_REGION" && "$region" != "$FILTER_REGION" ]] && continue
  echo "── region: $region"

  meta=()
  [[ -f "${region_dir}grid.geojson" ]]  && meta+=("${region_dir}grid.geojson")
  [[ -f "${region_dir}metadata.json" ]] && meta+=("${region_dir}metadata.json")
  [[ ${#meta[@]} -gt 0 ]] && commit_and_push "data($region): grid + metadata" "${meta[@]}"

  for var_dir in "$region_dir"*/; do
    [[ -d "$var_dir" ]] || continue
    var=$(basename "$var_dir")
    echo "  var: $var"

    # full CSVs
    fulls=()
    while IFS= read -r f; do
      [[ -n "$f" ]] && fulls+=("$f")
    done < <(find "$var_dir" -name "*_full.csv" -not -name "._*" | sort)
    [[ ${#fulls[@]} -gt 0 ]] && push_batches "data($region/$var): full series" "${fulls[@]}"

    # chunks by year
    for year in $(seq 2019 2030); do
      chunks=()
      while IFS= read -r f; do
        [[ -n "$f" ]] && chunks+=("$f")
      done < <(find "$var_dir" -name "${var}_${year}-*.csv" -not -name "._*" -not -name "*_full.csv" | sort)
      [[ ${#chunks[@]} -gt 0 ]] && push_batches "data($region/$var/$year): chunks" "${chunks[@]}"
    done
  done
done

echo "Done."