#!/usr/bin/env bash
set -uo pipefail

# Deep Record-Level Comparison Script
# Compares individual records between two ETL instances

SRC="${SRC:-https://etl-internal.nolus.network}"
REF="${REF:-https://etl.nolus.network}"
TMPDIR=$(mktemp -d)
REPORT="docs/deep-comparison-report.md"

trap 'rm -rf "$TMPDIR"' EXIT
mkdir -p "$(dirname "$REPORT")"

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

REPORT_BODY=""
TOTAL_ENDPOINTS=0
TOTAL_PERFECT=0
TOTAL_ISSUES=0

deep_compare() {
    local path="$1"
    local sort_key="$2"
    local label="${3:-$path}"
    local ignore_fields="${4:-}"  # comma-separated fields to ignore (e.g. time-sensitive)

    TOTAL_ENDPOINTS=$((TOTAL_ENDPOINTS + 1))
    printf "${CYAN}Fetching${NC} %-55s" "$label"

    local sf="$TMPDIR/src.json"
    local rf="$TMPDIR/ref.json"

    local sc rc
    sc=$(curl -s -o "$sf" -w "%{http_code}" --connect-timeout 15 --max-time 120 "${SRC}${path}" 2>/dev/null || echo "000")
    rc=$(curl -s -o "$rf" -w "%{http_code}" --connect-timeout 15 --max-time 120 "${REF}${path}" 2>/dev/null || echo "000")

    if [[ "$sc" != "200" ]] || [[ "$rc" != "200" ]]; then
        printf "\r${RED}ERROR${NC}  %-55s src=%s ref=%s\n" "$label" "$sc" "$rc"
        REPORT_BODY+=$'\n'"### $label"$'\n'"**ERROR**: src=$sc ref=$rc"$'\n'
        TOTAL_ISSUES=$((TOTAL_ISSUES + 1))
        return
    fi

    # Build jq filter to remove ignored fields
    local del_filter="."
    if [[ -n "$ignore_fields" ]]; then
        IFS=',' read -ra fields <<< "$ignore_fields"
        for f in "${fields[@]}"; do
            del_filter+=" | del(.\"$f\")"
        done
    fi

    # Sort both arrays by the sort key and normalize
    local sn="$TMPDIR/src_sorted.json"
    local rn="$TMPDIR/ref_sorted.json"

    jq -S --arg key "$sort_key" "[.[] | $del_filter] | sort_by(.[(\$key)])" "$sf" > "$sn" 2>/dev/null
    jq -S --arg key "$sort_key" "[.[] | $del_filter] | sort_by(.[(\$key)])" "$rf" > "$rn" 2>/dev/null

    local slen rlen
    slen=$(jq 'length' "$sn")
    rlen=$(jq 'length' "$rn")

    if [[ "$slen" != "$rlen" ]]; then
        printf "\r${YELLOW}COUNT${NC}  %-55s src=%s ref=%s\n" "$label" "$slen" "$rlen"
        REPORT_BODY+=$'\n'"### $label"$'\n'"**COUNT DIFF**: src=$slen ref=$rlen"$'\n'

        # Find records in one but not the other by sort key
        local only_src only_ref
        only_src=$(comm -23 <(jq -r --arg k "$sort_key" '.[] | .[$k]' "$sn" | sort) <(jq -r --arg k "$sort_key" '.[] | .[$k]' "$rn" | sort) | head -10)
        only_ref=$(comm -13 <(jq -r --arg k "$sort_key" '.[] | .[$k]' "$sn" | sort) <(jq -r --arg k "$sort_key" '.[] | .[$k]' "$rn" | sort) | head -10)

        if [[ -n "$only_src" ]]; then
            REPORT_BODY+="Only in source (first 10):"$'\n''```'$'\n'"$only_src"$'\n''```'$'\n'
        fi
        if [[ -n "$only_ref" ]]; then
            REPORT_BODY+="Only in reference (first 10):"$'\n''```'$'\n'"$only_ref"$'\n''```'$'\n'
        fi
        TOTAL_ISSUES=$((TOTAL_ISSUES + 1))
        return
    fi

    # Record-by-record diff
    local diff_output
    diff_output=$(diff "$sn" "$rn" 2>/dev/null || true)

    if [[ -z "$diff_output" ]]; then
        printf "\r${GREEN}MATCH${NC}  %-55s %s records identical\n" "$label" "$slen"
        REPORT_BODY+=$'\n'"### $label"$'\n'"**PERFECT MATCH**: All $slen records identical."$'\n'
        TOTAL_PERFECT=$((TOTAL_PERFECT + 1))
        return
    fi

    # Count how many records differ
    local diff_lines changed_records
    diff_lines=$(echo "$diff_output" | grep -c '^[<>]' || echo "0")
    # Approximate: each changed record produces ~2 lines per changed field
    # More precise: count unique record indices that differ
    changed_records=$(echo "$diff_output" | grep -c '^[0-9]' || echo "0")

    # Sample a few differing records for the report
    # Find first differing index
    local first_diff_idx=""
    local i=0
    while [[ $i -lt $slen ]] && [[ $i -lt 5000 ]]; do
        local sr rr
        sr=$(jq -c ".[$i]" "$sn")
        rr=$(jq -c ".[$i]" "$rn")
        if [[ "$sr" != "$rr" ]]; then
            first_diff_idx=$i
            break
        fi
        i=$((i + 1))
    done

    if [[ -z "$first_diff_idx" ]]; then
        # Diffs are beyond index 5000, likely minor
        printf "\r${YELLOW}DIFF${NC}   %-55s %s records, diffs after idx 5000\n" "$label" "$slen"
        REPORT_BODY+=$'\n'"### $label"$'\n'"**MINOR DIFF**: $slen records, differences found beyond index 5000 (likely trailing data)."$'\n'
        TOTAL_ISSUES=$((TOTAL_ISSUES + 1))
        return
    fi

    # Show sample diff
    local sample_src sample_ref
    sample_src=$(jq ".[$first_diff_idx]" "$sn")
    sample_ref=$(jq ".[$first_diff_idx]" "$rn")

    printf "\r${YELLOW}DIFF${NC}   %-55s %s records, ~%s diff blocks\n" "$label" "$slen" "$changed_records"
    REPORT_BODY+=$'\n'"### $label"$'\n'"**DIFF**: $slen records, ~$changed_records diff blocks, $diff_lines changed lines."$'\n'
    REPORT_BODY+=$'\n'"First differing record (index $first_diff_idx):"$'\n'
    REPORT_BODY+=$'\n'"**Source:**"$'\n''```json'$'\n'"$sample_src"$'\n''```'$'\n'
    REPORT_BODY+=$'\n'"**Reference:**"$'\n''```json'$'\n'"$sample_ref"$'\n''```'$'\n'

    TOTAL_ISSUES=$((TOTAL_ISSUES + 1))
}

echo ""
echo "=================================================="
echo " Deep Record-Level Comparison"
echo " Source:    $SRC"
echo " Reference: $REF"
echo "=================================================="
echo ""

# --- Critical endpoints ---

echo "--- Positions (746 records) ---"
deep_compare "/api/positions" "contract_id" "positions"

echo ""
echo "--- Liquidations (58,417 records) ---"
deep_compare "/api/liquidations?period=all" "contract_id" "liquidations?period=all"

echo ""
echo "--- Historically Opened (21,264 records) ---"
deep_compare "/api/historically-opened?period=all" "contract_id" "historically-opened?period=all"

echo ""
echo "--- Historically Repaid (21,264 records) ---"
deep_compare "/api/historically-repaid?period=all" "contract_id" "historically-repaid?period=all"

echo ""
echo "--- Historically Liquidated (21,264 records) ---"
deep_compare "/api/historically-liquidated?period=all" "contract_id" "historically-liquidated?period=all"

echo ""
echo "--- Historical Lenders (18,915 records) ---"
deep_compare "/api/historical-lenders?period=all" "address" "historical-lenders?period=all"

echo ""
echo "--- Buyback (all) ---"
deep_compare "/api/buyback?period=all" "timestamp" "buyback?period=all"

echo ""
echo "--- Realized PnL Wallet (20,518 records) ---"
deep_compare "/api/realized-pnl-wallet?period=all" "address" "realized-pnl-wallet?period=all"

echo ""
echo "--- Pools ---"
deep_compare "/api/pools" "protocol" "pools"

echo ""
echo "--- Current Lenders (582 records) ---"
deep_compare "/api/current-lenders" "address" "current-lenders"

echo ""
echo "--- Leased Assets ---"
deep_compare "/api/leased-assets" "ticker" "leased-assets"

echo ""
echo "--- Loans By Token ---"
deep_compare "/api/loans-by-token" "ticker" "loans-by-token"

echo ""
echo "--- Open Positions By Token ---"
deep_compare "/api/open-positions-by-token" "ticker" "open-positions-by-token"

echo ""
echo "--- Position Buckets ---"
deep_compare "/api/position-buckets" "bucket" "position-buckets"

echo ""
echo "--- Lease Value Stats ---"
deep_compare "/api/lease-value-stats" "ticker" "lease-value-stats"

echo ""
echo "--- Loans Granted ---"
deep_compare "/api/loans-granted" "ticker" "loans-granted"

echo ""
echo "--- Daily Positions ---"
deep_compare "/api/daily-positions" "date" "daily-positions"

echo ""
echo "--- Leases Monthly ---"
deep_compare "/api/leases-monthly" "month" "leases-monthly"

echo ""
echo "--- Revenue Series ---"
deep_compare "/api/revenue-series" "date" "revenue-series"

# =============================================
echo ""
echo "=================================================="
echo " Results"
echo "=================================================="
printf "${GREEN}PERFECT MATCH${NC}: %d / %d endpoints\n" "$TOTAL_PERFECT" "$TOTAL_ENDPOINTS"
printf "${YELLOW}WITH DIFFS${NC}:    %d / %d endpoints\n" "$TOTAL_ISSUES" "$TOTAL_ENDPOINTS"
echo ""

# Write report
cat > "$REPORT" <<EOF
# Deep Record-Level Comparison Report

**Date**: $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Source**: $SRC
**Reference**: $REF

---

## Summary

| | Count |
|--|-------|
| Perfect match | $TOTAL_PERFECT / $TOTAL_ENDPOINTS |
| With differences | $TOTAL_ISSUES / $TOTAL_ENDPOINTS |

---

## Detailed Results
$REPORT_BODY
EOF

echo "Report saved to $REPORT"
