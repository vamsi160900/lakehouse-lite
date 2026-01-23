import re
from pathlib import Path

p = Path(".env.neon")
if not p.exists():
    raise SystemExit("ERROR: .env.neon not found. Create it with: notepad .env.neon")

s = p.read_text(encoding="utf-8").strip().strip('"').strip("'")

# Accept both postgres:// and postgresql://
m = re.search(r"postgres(?:ql)?://([^:]+):([^@]+)@([^:/]+)(?::(\d+))?/([^?\s]+)", s)
if not m:
    raise SystemExit("ERROR: Could not parse connection string. Paste the full Neon connection string into .env.neon")

user, password, host, port, db = m.group(1), m.group(2), m.group(3), m.group(4) or "5432", m.group(5)
ssl = "require" if "sslmode=require" in s else "disable"

print("FOUND_URL=", True)
print("USER=", user)
print("HOST=", host)
print("PORT=", port)
print("DB=", db)
print("SSL_MODE=", ssl)
