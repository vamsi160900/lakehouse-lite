import re
from pathlib import Path

s = Path(".env.neon").read_text(encoding="utf-8").strip()

# remove wrapping quotes if the whole line is quoted
if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
    s = s[1:-1].strip()

m = re.search(r"postgres(?:ql)?://([^:]+):([^@]+)@([^:/]+)(?::(\d+))?/([^?\s]+)", s)
if not m:
    raise SystemExit("Could not parse the connection string in .env.neon")

user, pwd, host, port, db = m.group(1), m.group(2), m.group(3), (m.group(4) or "5432"), m.group(5)
ssl = "require" if "sslmode=require" in s else "require"  # neon typically needs require

Path(".env").write_text(
    f"DB_HOST={host}\nDB_PORT={port}\nDB_NAME={db}\nDB_USER={user}\nDB_PASSWORD={pwd}\nDB_SSLMODE={ssl}\n",
    encoding="utf-8",
)

print("parsed ok")
print("Wrote .env with HOST, PORT, DB, USER, SSLMODE (password hidden)")
print("HOST=", host)
print("PORT=", port)
print("DB=", db)
print("USER=", user)
print("SSLMODE=", ssl)
