#!/usr/bin/env bash
set -uo pipefail

# Data Accuracy Comparison Script
# Compares API responses between two ETL instances

LOCAL_BASE="${LOCAL_BASE:-http://localhost:8080}"
INTERNAL_BASE="${INTERNAL_BASE:-https://etl-internal.nolus.network}"
REPORT_FILE="docs/data-accuracy-report.md"
TMPDIR_BASE=$(mktemp -d)

trap 'rm -rf "$TMPDIR_BASE"' EXIT

# Counters
TOTAL=0
MATCH=0
STRUCT_MATCH=0
DIFF=0
ERROR=0

# Colors
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

mkdir -p "$(dirname "$REPORT_FILE")"
DETAILS=""

fetch() {
    local url="$1" out="$2"
    curl -s -o "$out" -w "%{http_code}" --connect-timeout 10 --max-time 60 "$url" 2>/dev/null || echo "000"
}

normalize_json() {
    local file="$1"
    # Try jq first; if it fails (non-JSON), just cat the raw content
    jq -S '.' "$file" 2>/dev/null || cat "$file"
}

compare_endpoint() {
    local path="$1"
    local mode="${2:-exact}"  # exact, structure, skip_values
    local label="$path"

    TOTAL=$((TOTAL + 1))

    local lf="$TMPDIR_BASE/local_${TOTAL}.raw"
    local rf="$TMPDIR_BASE/remote_${TOTAL}.raw"

    local lcode rcode
    lcode=$(fetch "${LOCAL_BASE}${path}" "$lf")
    rcode=$(fetch "${INTERNAL_BASE}${path}" "$rf")

    if [[ "$lcode" != "200" ]] || [[ "$rcode" != "200" ]]; then
        ERROR=$((ERROR + 1))
        printf "${RED}ERROR${NC}  %-55s src=%s ref=%s\n" "$label" "$lcode" "$rcode"
        DETAILS+=$'\n'"### $label"$'\n'"**ERROR**: source=$lcode, reference=$rcode"$'\n'
        return
    fi

    local ln="$TMPDIR_BASE/local_n_${TOTAL}.json"
    local rn="$TMPDIR_BASE/remote_n_${TOTAL}.json"
    normalize_json "$lf" > "$ln"
    normalize_json "$rf" > "$rn"

    # --- skip_values mode: just confirm both responded OK ---
    if [[ "$mode" == "skip_values" ]]; then
        STRUCT_MATCH=$((STRUCT_MATCH + 1))
        printf "${CYAN}SKIP${NC}   %-55s both 200 (values expected to differ)\n" "$label"
        DETAILS+=$'\n'"### $label"$'\n'"**SKIPPED VALUE CHECK**: Both returned 200. Values expected to differ."$'\n'
        return
    fi

    # --- structure mode: compare JSON keys only ---
    if [[ "$mode" == "structure" ]]; then
        local lkeys rkeys
        lkeys=$(jq -r 'if type == "array" then .[0] // {} else . end | keys[]' "$ln" 2>/dev/null | sort || true)
        rkeys=$(jq -r 'if type == "array" then .[0] // {} else . end | keys[]' "$rn" 2>/dev/null | sort || true)
        if [[ "$lkeys" == "$rkeys" ]]; then
            STRUCT_MATCH=$((STRUCT_MATCH + 1))
            printf "${CYAN}STRUCT${NC} %-55s keys match\n" "$label"
            DETAILS+=$'\n'"### $label"$'\n'"**STRUCTURE MATCH**: Keys identical."$'\n'
        else
            DIFF=$((DIFF + 1))
            printf "${RED}DIFF${NC}   %-55s structure mismatch\n" "$label"
            DETAILS+=$'\n'"### $label"$'\n'"**STRUCTURE DIFF**:"$'\n'"Source keys: $lkeys"$'\n'"Reference keys: $rkeys"$'\n'
        fi
        return
    fi

    # --- exact mode ---
    if diff -q "$ln" "$rn" > /dev/null 2>&1; then
        MATCH=$((MATCH + 1))
        printf "${GREEN}MATCH${NC}  %-55s identical\n" "$label"
        DETAILS+=$'\n'"### $label"$'\n'"**MATCH**: Responses identical."$'\n'
        return
    fi

    # Responses differ — classify the difference
    local ltype rtype
    ltype=$(jq -r 'type' "$ln" 2>/dev/null || echo "raw")
    rtype=$(jq -r 'type' "$rn" 2>/dev/null || echo "raw")

    # --- Array responses ---
    if [[ "$ltype" == "array" ]] && [[ "$rtype" == "array" ]]; then
        local llen rlen
        llen=$(jq 'length' "$ln")
        rlen=$(jq 'length' "$rn")

        if [[ "$llen" == "0" ]] && [[ "$rlen" != "0" ]]; then
            DIFF=$((DIFF + 1))
            printf "${RED}DIFF${NC}   %-55s src=[] ref=%s items\n" "$label" "$rlen"
            DETAILS+=$'\n'"### $label"$'\n'"**DIFF**: Source returned empty, reference has $rlen items."$'\n'
            return
        fi

        if [[ "$llen" == "$rlen" ]]; then
            STRUCT_MATCH=$((STRUCT_MATCH + 1))
            printf "${CYAN}COUNT${NC}  %-55s both %s items\n" "$label" "$llen"
            DETAILS+=$'\n'"### $label"$'\n'"**COUNT MATCH**: Both have $llen items, minor value differences."$'\n'
            return
        fi

        local count_diff=$((llen - rlen))
        if [[ $count_diff -lt 0 ]]; then count_diff=$((-count_diff)); fi
        DIFF=$((DIFF + 1))
        printf "${YELLOW}DIFF${NC}   %-55s src=%s ref=%s items (delta=%s)\n" "$label" "$llen" "$rlen" "$count_diff"
        DETAILS+=$'\n'"### $label"$'\n'"**DIFF**: Source has $llen items, reference has $rlen items (delta=$count_diff)."$'\n'
        return
    fi

    # --- Object responses ---
    if [[ "$ltype" == "object" ]] && [[ "$rtype" == "object" ]]; then
        local field_diffs=""
        local all_keys
        all_keys=$(jq -r 'keys[]' "$rn" 2>/dev/null || true)
        while IFS= read -r key; do
            [[ -z "$key" ]] && continue
            local lv rv
            lv=$(jq -r --arg k "$key" '.[$k] // "null"' "$ln" 2>/dev/null || echo "null")
            rv=$(jq -r --arg k "$key" '.[$k] // "null"' "$rn" 2>/dev/null || echo "null")
            if [[ "$lv" != "$rv" ]]; then
                if [[ "$lv" =~ ^-?[0-9]+\.?[0-9]*$ ]] && [[ "$rv" =~ ^-?[0-9]+\.?[0-9]*$ ]] && [[ "$rv" != "0" ]] && [[ "$rv" != "0.0" ]]; then
                    local pct
                    pct=$(echo "scale=2; ($lv - $rv) / $rv * 100" | bc 2>/dev/null || echo "N/A")
                    field_diffs+="  $key: src=$lv ref=$rv (${pct}%)"$'\n'
                else
                    field_diffs+="  $key: src=$lv ref=$rv"$'\n'
                fi
            fi
        done <<< "$all_keys"

        if [[ -z "$field_diffs" ]]; then
            MATCH=$((MATCH + 1))
            printf "${GREEN}MATCH${NC}  %-55s identical\n" "$label"
            DETAILS+=$'\n'"### $label"$'\n'"**MATCH**: Responses identical."$'\n'
        else
            local ndiffs
            ndiffs=$(echo -n "$field_diffs" | grep -c '.' || echo "0")
            DIFF=$((DIFF + 1))
            printf "${YELLOW}DIFF${NC}   %-55s %s fields differ\n" "$label" "$ndiffs"
            DETAILS+=$'\n'"### $label"$'\n'"**DIFF**: $ndiffs fields differ:"$'\n''```'$'\n'"$field_diffs"'```'$'\n'
        fi
        return
    fi

    # --- Fallback (raw/number/string) ---
    local raw_diff
    raw_diff=$(diff "$ln" "$rn" 2>/dev/null | head -30 || true)
    if [[ -z "$raw_diff" ]]; then
        MATCH=$((MATCH + 1))
        printf "${GREEN}MATCH${NC}  %-55s identical\n" "$label"
        DETAILS+=$'\n'"### $label"$'\n'"**MATCH**: Responses identical."$'\n'
    else
        DIFF=$((DIFF + 1))
        printf "${YELLOW}DIFF${NC}   %-55s values differ\n" "$label"
        DETAILS+=$'\n'"### $label"$'\n'"**DIFF**:"$'\n''```'$'\n'"$raw_diff"$'\n''```'$'\n'
    fi
}

# =============================================
# Run comparisons
# =============================================

echo ""
echo "=================================================="
echo " Data Accuracy Comparison"
echo " Source:    $LOCAL_BASE"
echo " Reference: $INTERNAL_BASE"
echo "=================================================="
echo ""

echo "--- Configuration ---"
compare_endpoint "/api/version" "structure"
compare_endpoint "/api/blocks" "skip_values"
compare_endpoint "/api/protocols" "exact"
compare_endpoint "/api/protocols/active" "exact"
compare_endpoint "/api/currencies" "exact"
compare_endpoint "/api/currencies/active" "exact"

echo ""
echo "--- Protocol Analytics ---"
compare_endpoint "/api/total-value-locked"
compare_endpoint "/api/total-tx-value"
compare_endpoint "/api/revenue"
compare_endpoint "/api/borrowed"
compare_endpoint "/api/supplied-funds"
compare_endpoint "/api/open-position-value"
compare_endpoint "/api/open-interest"
compare_endpoint "/api/unrealized-pnl"
compare_endpoint "/api/realized-pnl-stats"
compare_endpoint "/api/distributed"
compare_endpoint "/api/buyback-total"
compare_endpoint "/api/incentives-pool"

echo ""
echo "--- Token Breakdowns ---"
compare_endpoint "/api/leased-assets"
compare_endpoint "/api/loans-by-token"
compare_endpoint "/api/open-positions-by-token"
compare_endpoint "/api/position-buckets"
compare_endpoint "/api/pools"
compare_endpoint "/api/current-lenders"
compare_endpoint "/api/lease-value-stats"
compare_endpoint "/api/loans-granted"

echo ""
echo "--- Time Series ---"
compare_endpoint "/api/monthly-active-wallets"
compare_endpoint "/api/daily-positions"
compare_endpoint "/api/leases-monthly"
compare_endpoint "/api/supplied-borrowed-history"
compare_endpoint "/api/utilization-level"
compare_endpoint "/api/revenue-series"

echo ""
echo "--- Historical Lists (period=all) ---"
compare_endpoint "/api/liquidations?period=all"
compare_endpoint "/api/historically-liquidated?period=all"
compare_endpoint "/api/historically-repaid?period=all"
compare_endpoint "/api/historically-opened?period=all"
compare_endpoint "/api/historical-lenders?period=all"
compare_endpoint "/api/interest-repayments?period=all"
compare_endpoint "/api/buyback?period=all"
compare_endpoint "/api/realized-pnl-wallet?period=all"
compare_endpoint "/api/positions"

# =============================================
# Summary
# =============================================

echo ""
echo "=================================================="
echo " Results"
echo "=================================================="
printf "${GREEN}MATCH${NC}:          %d\n" "$MATCH"
printf "${CYAN}STRUCT/COUNT${NC}:   %d\n" "$STRUCT_MATCH"
printf "${YELLOW}DIFF${NC}:           %d\n" "$DIFF"
printf "${RED}ERROR${NC}:          %d\n" "$ERROR"
echo "---"
printf "TOTAL:           %d\n" "$TOTAL"
echo ""

cat > "$REPORT_FILE" <<EOF
# Data Accuracy Report

**Date**: $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Source**: $LOCAL_BASE
**Reference**: $INTERNAL_BASE

---

## Summary

| Status | Count |
|--------|-------|
| MATCH (identical) | $MATCH |
| STRUCT/COUNT (shape matches) | $STRUCT_MATCH |
| DIFF (values differ) | $DIFF |
| ERROR (HTTP failures) | $ERROR |
| **TOTAL** | **$TOTAL** |

---

## Detailed Results
$DETAILS
EOF

echo "Report saved to $REPORT_FILE"
