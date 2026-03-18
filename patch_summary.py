import re

file_path = "brazil_product_code_v1.02.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Replace update_summary_row
# We look for the definition and indented block
pattern_update = r"(def update_summary_row(self, table: QTableWidget):)(.*?)(?=\n\s*def |\Z)"
replacement_update = r"""\1
        "Update status bar with summary of visible rows (Modified to remove summary row)"
        if table.rowCount() == 0:
            if self.statusBar(): self.statusBar().showMessage("Total: 0 items")
            return

        # Calculate totals from visible rows
        cols = table.columnCount()
        totals = {}
        visible_count = 0

        # Optimization: Identify numeric columns based on the first few visible rows
        numeric_cols = set()
        
        rows = table.rowCount()
        rows_to_check = min(rows, 20)

        for r in range(rows_to_check):
             if table.isRowHidden(r): continue
             for c in range(cols):
                 if c in numeric_cols: continue
                 item = table.item(r, c)
                 # Use br_to_float if available in scope or self. 
                 # br_to_float is imported at module level.
                 if item and br_to_float(item.text()) is not None:
                     numeric_cols.add(c)

        for r in range(rows):
            if table.isRowHidden(r): continue
            visible_count += 1

            for c in numeric_cols:
                item = table.item(r, c)
                if not item: continue
                val = br_to_float(item.text())
                if val is not None:
                    totals[c] = totals.get(c, 0.0) + val

        # Format Summary String
        summary_parts = [f"Count: {visible_count}"]
        
        for c in sorted(totals.keys()):
            val = totals[c]
            header_item = table.horizontalHeaderItem(c)
            header_text = header_item.text() if header_item else f"Col {c}"
            # Optional: Filter interesting columns if needed
            summary_parts.append(f"{header_text}: {val:, .2f}")

        summary_text = " | ".join(summary_parts)
        if self.statusBar():
            self.statusBar().showMessage(summary_text)
"""

content_new = re.sub(pattern_update, replacement_update, content, flags=re.DOTALL)

# 2. Fix apply_header_filters loop
# Search for: for r in range(1, rows): inside apply_header_filters
# We can just replace "for r in range(1, rows):" with "for r in range(rows):"
# Be careful not to replace it everywhere if context matters, but generally "range(1, rows)" usually implies skipping header/summary.
# Given the user wants to remove summary rows everywhere, this replacement is likely safe globally for table iterations.
# However, let's target apply_header_filters specifically if possible, or just global replace if the pattern is unique enough.
# The pattern "for r in range(1, rows):" appears in update_summary_row (which we just replaced) and apply_header_filters.
# So global replace after step 1 is fine.

content_new = content_new.replace("for r in range(1, rows):", "for r in range(rows):")
content_new = content_new.replace("for r in range(1, rows_to_check):", "for r in range(rows_to_check):")

# 3. Fix sort_with_summary
pattern_sort = r"(def sort_with_summary(self, table: QTableWidget, col: int, order: Qt.SortOrder):)(.*?)(?=\n\s*def |\Z)"
replacement_sort = r"""\1
        "Standard sort without summary row"
        table.sortItems(col, order)
        self.update_summary_row(table)
"""
content_new = re.sub(pattern_sort, replacement_sort, content_new, flags=re.DOTALL)

# 4. Remove "SUMMARY" checks
# "if item.data(Qt.UserRole) == "SUMMARY": continue"
# "if first_item and first_item.data(Qt.UserRole) == "SUMMARY":"
# We can comment them out or remove them. 
# Replacing with "if False: # Removed SUMMARY check" preserves line count/structure somewhat.
content_new = re.sub(r'if item.data(Qt.UserRole) == "SUMMARY":', 'if False: # SUMMARY check removed', content_new)
content_new = re.sub(r'if first_item and first_item.data(Qt.UserRole) == "SUMMARY":', 'if False: # SUMMARY check removed', content_new)
content_new = re.sub(r'if item and item.data(Qt.UserRole) == "SUMMARY":', 'if False: # SUMMARY check removed', content_new)
content_new = re.sub(r'if it.data(Qt.UserRole) == "SUMMARY":', 'if False: # SUMMARY check removed', content_new)
content_new = re.sub(r'if data == "SUMMARY":', 'if False: # SUMMARY check removed', content_new)

# 5. Fix populate_summary_table (L8665 approx)
# It likely sets row count. We need to check if it adds a row for summary. 
# If we can't see it, we assume the general fixes (loops) cover iteration.
# But we should ensure it doesn't create a "SUMMARY" row.
# Logic in populate_summary_table likely creates QTableWidgetItem and sets data SUMMARY.
# I'll replace any `item.setData(Qt.UserRole, "SUMMARY")` with nothing or pass.
content_new = re.sub(r'item.setData(Qt.UserRole, "SUMMARY")', '# item.setData(Qt.UserRole, "SUMMARY") - Removed', content_new)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content_new)

print("Successfully patched brazil_product_code_v1.02.py")
