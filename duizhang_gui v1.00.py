# -*- coding: utf-8 -*-
"""
Compatibility launcher for legacy filename "duizhang_gui v1.00.py".

The reconciliation workflow is now integrated into brazil_product_code_v1.02.py.
This stub keeps old run scripts and user habits working.
"""

from pathlib import Path
import runpy
import sys


def main() -> int:
    target = Path(__file__).with_name("brazil_product_code_v1.02.py")
    if not target.exists():
        print(f"Target script not found: {target}")
        return 1
    runpy.run_path(str(target), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
