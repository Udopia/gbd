#!/usr/bin/env bash
#
# Integration test for the gbd <-> gbdc interaction.
#
# Verifies that, once gbd-tools and gbdc are installed, the configuration system
# and the external-tool-driven `gbd init` / `gbd transform` work end to end
# (i.e. gbd correctly invokes the gbdc command-line tools as subprocesses).
#
# Run with -h to see the available options. Exit code 0 iff all checks pass.

set -uo pipefail

GBD_CMD="gbd"
GBDC_CMD="gbdc"
INSTALL=""
GBD_SRC=""
GBDC_SRC=""
KEEP=0

usage() {
    cat <<'EOF'
Integration test for the gbd <-> gbdc interaction.

Usage: integration_test.sh [options]

Options:
  -i MODE   install into a throwaway venv first: 'pypi' or 'local'
  -g PATH   gbd source directory   (with -i local)
  -c PATH   gbdc source directory  (with -i local)
  -G PATH   gbd executable to test    (default: gbd on PATH)
  -C PATH   gbdc executable to test   (default: gbdc on PATH)
  -k        keep the temporary work directory
  -h        show this help and exit

Examples:
  integration_test.sh                       # use gbd/gbdc on PATH
  integration_test.sh -i pypi               # fresh venv from PyPI
  integration_test.sh -i local -g ../gbd -c ../gbdc
  integration_test.sh -G ./gbd -C ../gbdc/build-cli/gbdc -k
EOF
}

while getopts ":i:g:c:G:C:kh" opt; do
    case "$opt" in
        i) INSTALL="$OPTARG" ;;
        g) GBD_SRC="$OPTARG" ;;
        c) GBDC_SRC="$OPTARG" ;;
        G) GBD_CMD="$OPTARG" ;;
        C) GBDC_CMD="$OPTARG" ;;
        k) KEEP=1 ;;
        h) usage; exit 0 ;;
        :) echo "error: option -$OPTARG requires an argument" >&2; usage; exit 2 ;;
        \?) echo "error: unknown option -$OPTARG" >&2; usage; exit 2 ;;
    esac
done

WORKDIR="$(mktemp -d)"
trap '[[ "$KEEP" == "1" ]] || rm -rf "$WORKDIR"' EXIT

pass=0
fail=0
say()  { printf '\n== %s ==\n' "$1"; }
ok()   { printf '  ok:   %s\n' "$1"; pass=$((pass + 1)); }
bad()  { printf '  FAIL: %s\n' "$1"; fail=$((fail + 1)); }
check_eq() { if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1 (got '$2', want '$3')"; fi; }
check_ge() { if [[ "$2" -ge "$3" ]]; then ok "$1"; else bad "$1 (got '$2', want >= '$3')"; fi; }
have() { command -v "$1" >/dev/null 2>&1; }
nlines() { awk 'END { print NR + 0 }'; }
# count output rows whose 2nd column (the resolved value) matches a regex
count_values() { awk -v re="$1" '$2 ~ re { c++ } END { print c + 0 }'; }

N=5  # number of sample instances

# ---------------------------------------------------------------------------
# Optional install into a throwaway venv
# ---------------------------------------------------------------------------
if [[ -n "$INSTALL" ]]; then
    say "install (-i $INSTALL)"
    python3 -m venv "$WORKDIR/venv"
    # shellcheck disable=SC1091
    source "$WORKDIR/venv/bin/activate"
    pip install -q --upgrade pip
    case "$INSTALL" in
        pypi)  pip install -q 'gbd-tools[gbdc]' || { echo "pip install failed"; exit 2; } ;;
        local) if [[ -z "$GBD_SRC" || -z "$GBDC_SRC" ]]; then
                   echo "error: -i local requires -g <gbd src> and -c <gbdc src>" >&2; exit 2
               fi
               pip install -q "$GBDC_SRC" "$GBD_SRC" || { echo "pip install failed"; exit 2; } ;;
        *)     echo "error: unknown install mode '$INSTALL' (expected 'pypi' or 'local')" >&2; exit 2 ;;
    esac
    GBD_CMD=gbd
    GBDC_CMD=gbdc
    ok "installed gbd-tools and gbdc"
fi

# ---------------------------------------------------------------------------
# Preflight: both tools present and gbdc responds to the contract
# ---------------------------------------------------------------------------
say "preflight"
have "$GBD_CMD"  || { echo "gbd not found ('$GBD_CMD')";  exit 2; }
have "$GBDC_CMD" || { echo "gbdc not found ('$GBDC_CMD')"; exit 2; }
echo "  gbd:  $(command -v "$GBD_CMD")"
echo "  gbdc: $(command -v "$GBDC_CMD")"
if "$GBDC_CMD" base --feature-names --gbd >/dev/null 2>&1; then
    ok "gbdc responds to the --feature-names contract"
else
    bad "gbdc base --feature-names --gbd failed"
fi

# ---------------------------------------------------------------------------
# Sample instances (distinct content -> distinct hashes)
# ---------------------------------------------------------------------------
say "generate $N sample instances"
CNFDIR="$WORKDIR/cnf"
mkdir -p "$CNFDIR"
for idx in $(seq 1 "$N"); do
    vars=$((idx + 3))
    {
        echo "c gbd integration test instance $idx"
        echo "p cnf $vars $vars"
        for ((i = 1; i <= vars; i++)); do
            echo "$i -$(((i % vars) + 1)) 0"
        done
    } > "$CNFDIR/inst_$idx.cnf"
done
check_eq "sample instances written" "$(ls "$CNFDIR"/*.cnf | nlines)" "$N"

DB="$WORKDIR/cnf_test.db"      # cnf-context database (name prefix -> context)
KDB="$WORKDIR/kis_test.db"     # kis-context database

# ---------------------------------------------------------------------------
# init local: register hashes and paths
# ---------------------------------------------------------------------------
say "init local"
echo y | "$GBD_CMD" -d "$DB" init local "$CNFDIR" >/dev/null 2>&1
n=$("$GBD_CMD" -d "$DB" get < /dev/null 2>/dev/null | nlines)
check_eq "all instances registered in local table" "$n" "$N"

# ---------------------------------------------------------------------------
# Extraction: base features (gbd runs 'gbdc base' per instance)
# ---------------------------------------------------------------------------
say "extract base features"
"$GBD_CMD" -d "$DB" init base < /dev/null >/dev/null 2>&1
n=$("$GBD_CMD" -d "$DB" get -r clauses < /dev/null 2>/dev/null | count_values '^[0-9]+$')
check_eq "clauses extracted for all instances" "$n" "$N"
n=$("$GBD_CMD" -d "$DB" get -r variables < /dev/null 2>/dev/null | count_values '^[0-9]+$')
check_eq "variables extracted for all instances" "$n" "$N"

# ---------------------------------------------------------------------------
# Extraction: gate features (exercises the bundled SAT solver)
# ---------------------------------------------------------------------------
say "extract gate features (SAT solver)"
gfeat=$("$GBDC_CMD" gate --feature-names --gbd 2>/dev/null | awk 'NR==1 { print $1 }')
"$GBD_CMD" -d "$DB" init gate < /dev/null >/dev/null 2>&1
n=$("$GBD_CMD" -d "$DB" get -r "$gfeat" < /dev/null 2>/dev/null | count_values '^-?[0-9.]+$')
check_ge "gate feature '$gfeat' extracted" "$n" "$N"

# ---------------------------------------------------------------------------
# Extraction: isohash (string-valued feature)
# ---------------------------------------------------------------------------
say "extract isohash"
"$GBD_CMD" -d "$DB" init isohash < /dev/null >/dev/null 2>&1
n=$("$GBD_CMD" -d "$DB" get -r isohash < /dev/null 2>/dev/null | count_values '^[0-9a-f]+$')
check_eq "isohash set for all instances" "$n" "$N"

# ---------------------------------------------------------------------------
# Parallel extraction (-j2): checksani (yes/no flags)
# ---------------------------------------------------------------------------
say "parallel extraction (-j2 checksani)"
"$GBD_CMD" -d "$DB" init -j2 checksani < /dev/null >/dev/null 2>&1
n=$("$GBD_CMD" -d "$DB" get -r no_empty_clause < /dev/null 2>/dev/null | count_values '^(yes|no)$')
check_eq "checksani (parallel) set for all instances" "$n" "$N"

# ---------------------------------------------------------------------------
# Transformation: cnf2kis (produces new instances + derived features + link)
# ---------------------------------------------------------------------------
say "transform cnf2kis"
echo y | "$GBD_CMD" -d "$KDB" info >/dev/null 2>&1
"$GBD_CMD" -d "$DB:$KDB" transform --source cnf --target kis_test cnf2kis < /dev/null >/dev/null 2>&1
check_eq "kis instances produced on disk" "$(ls "$CNFDIR"/*.kis 2>/dev/null | nlines)" "$N"
# query the kis database on its own so the primary context is 'kis'
n=$("$GBD_CMD" -d "$KDB" get -r k < /dev/null 2>/dev/null | count_values '^[0-9]+$')
check_eq "derived 'k' feature set for produced instances" "$n" "$N"
n=$("$GBD_CMD" -d "$KDB" get -r to_cnf < /dev/null 2>/dev/null | count_values '^[0-9a-f]{32}$')
check_eq "to_cnf back-link set for produced instances" "$n" "$N"

# ---------------------------------------------------------------------------
# Config file (via -d) + compressed transform (xz)
# ---------------------------------------------------------------------------
say "config-file transform with xz compression"
CONFIG="$WORKDIR/gbd.toml"
cat > "$CONFIG" <<EOF
[databases]
paths = ["$DB", "$KDB"]

[transformers.kisxz]
tool = "gbdc cnf2kis"
source = ["cnf"]
target = ["kis"]
compress = "xz"
description = "cnf2kis with xz-compressed output"
EOF
rm -f "$CNFDIR"/*.kis "$CNFDIR"/*.kis.xz
"$GBD_CMD" -d "$CONFIG" transform --source cnf --target kis_test kisxz < /dev/null >/dev/null 2>&1
xzc=$(ls "$CNFDIR"/*.kis.xz 2>/dev/null | nlines)
check_eq "xz-compressed kis instances produced" "$xzc" "$N"
if have xz && [[ "$xzc" -gt 0 ]]; then
    if xz -t "$(ls "$CNFDIR"/*.kis.xz | head -1)" 2>/dev/null; then
        ok "produced xz output is a valid archive"
    else
        bad "produced xz output failed integrity check"
    fi
fi

# ---------------------------------------------------------------------------
# Configuration precedence: GBD env config supplies the databases
# ---------------------------------------------------------------------------
say "GBD environment config file"
if GBD="$CONFIG" "$GBD_CMD" info < /dev/null 2>/dev/null | grep -q "cnf_test"; then
    ok "GBD env var config file resolved (databases loaded)"
else
    bad "GBD env var config file not resolved"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
say "summary"
printf '  passed: %d, failed: %d\n' "$pass" "$fail"
if [[ "$fail" -eq 0 ]]; then
    echo "ALL INTEGRATION TESTS PASSED"
    exit 0
else
    echo "SOME INTEGRATION TESTS FAILED"
    exit 1
fi
