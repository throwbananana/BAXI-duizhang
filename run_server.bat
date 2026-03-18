@echo off
echo Starting Brazil Tool Payment Server...
set PYTHONPATH=%CD%
python -m brazil_tool.server
pause