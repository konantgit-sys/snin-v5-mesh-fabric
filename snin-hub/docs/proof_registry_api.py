REGISTRY_DB = os.path.join(os.path.dirname(__file__), "proof_registry.db")
_registry_lock = threading.Lock()

def _init_registry():
    conn = sqlite3.connect(REGISTRY_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS proof_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proof_code TEXT UNIQUE,
            agent_name TEXT DEFAULT '',
            pubkey TEXT DEFAULT '',
            ip TEXT DEFAULT '',
            version TEXT DEFAULT '',
            agent_type TEXT DEFAULT 'agent_light',
            first_seen REAL DEFAULT 0,
            last_seen REAL DEFAULT 0,
            ping_count INTEGER DEFAULT 0,
            verified INTEGER DEFAULT 0,
            infinity_claimed INTEGER DEFAULT 0,
            notes TEXT DEFAULT ''
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_proof_code ON proof_registry(proof_code)
    """)
    conn.commit()
    conn.close()

_init_registry()

class RegisterBody(BaseModel):
    proof_code: str = ""
    agent_name: str = "unknown"
    pubkey: str = ""
    version: str = ""
    agent_type: str = "agent_light"

@app.post("/api/register")
async def api_register(body: RegisterBody, request: Request):
    """Регистрация агента после генерации proof-кода."""
    # Реальный IP через прокси (X-Forwarded-For)
    ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    if not ip:
        ip = request.client.host if request.client else ""
    
    if not body.proof_code or len(body.proof_code) != 14:
        return {"ok": False, "error": "invalid proof_code format"}
    
    now = time.time()
    try:
        with _registry_lock:
            conn = sqlite3.connect(REGISTRY_DB)
            try:
                conn.execute("""
                    INSERT INTO proof_registry (proof_code, agent_name, pubkey, ip, version, agent_type, first_seen, last_seen, ping_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(proof_code) DO UPDATE SET
                        last_seen = ?,
                        ping_count = ping_count + 1,
                        ip = CASE WHEN ? != '' THEN ? ELSE ip END,
                        agent_name = CASE WHEN ? != '' THEN ? ELSE agent_name END
                """, (
                    body.proof_code, body.agent_name, body.pubkey, ip, body.version, body.agent_type, now, now,
                    now, ip, ip, body.agent_name, body.agent_name
                ))
                conn.commit()
                row = conn.execute("SELECT id, proof_code, first_seen, verified FROM proof_registry WHERE proof_code=?", (body.proof_code,)).fetchone()
                return {
                    "ok": True,
                    "id": row[0] if row else 0,
                    "proof_code": body.proof_code,
