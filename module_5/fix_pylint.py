"""Script to automatically fix Pylint issues."""
import re

# Fix app.py
with open('src/app.py', 'r') as f:
    app = f.read()

# Remove duplicate _build_conninfo and get_conn
app = re.sub(r'def _build_conninfo\(.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n', '', app, flags=re.DOTALL)
app = re.sub(r'def get_conn\(app=None\):\n.*?return psycopg\.connect.*?\n', '', app)

# Add module docstring if missing
if not app.startswith('"""'):
    app = '"""app.py - Flask application for GradCafe Analytics."""\n' + app

# Add import after Flask import if not present
if 'from db_utils import get_conn' not in app:
    app = app.replace('from flask import', 'from flask import Flask, render_template, jsonify\nfrom db_utils import get_conn\n\n# Removed unused imports')
    
# Remove psycopg import
app = app.replace('import psycopg  # psycopg3\n', '')

with open('src/app.py', 'w') as f:
    f.write(app)

print("Fixed app.py")

# Similar for other files...
