# Example: PR description recognized by the framework.
#
# validate_pr.py parses two sections from a pull-request description:
#   1. "## Affected Tables" — the tables your change touches.
#   2. "## Validation Scope Mode" — how far along the lineage graph to validate.
#
# Copy the block below into a PR description (or pass it via --pr_description_file)
# to see the validator expand and check the affected tables.

## Affected Tables
- silver.fact_orders
- gold.daily_sales_summary

## Validation Scope Mode
Choose one:
- [x] affected_only
- [ ] upstream
- [ ] downstream
- [ ] full_lineage
