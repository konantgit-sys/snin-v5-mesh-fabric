#!/usr/bin/env python3
"""Mesh Identity — SNIN-native ключ агента.

mesh pubkey — это суть агента в сети SNIN. Не Nostr, не Solana, не Telegram.
Это просто secp256k1 ключ, сгенерированный при первом запуске агента.

Внешние привязки (Nostr npub, Telegram ID, Solana address) —
метаданные. mesh pubkey — идентичность.

Философия:
  - mesh pubkey НЕЛЬЗЯ заблокировать через relay/TG/blockchain
  - mesh pubkey — сущность на уровне пакета (ping, exchange, decision)
  - Внешние привязки — слои поверх mesh pubkey
  - SmartRouter верифицирует mesh pubkey (оракул/аттестатор)

Файл:
  ~/data/sites/relay-mesh/identities/{agent_name}.json
  {
    "mesh_pubkey": "02abc...",          # hex, 33 байта (compressed)
    "mesh_privkey": "...",              # hex, приватный ключ
    "mesh_npub_bech32": "nmesh1...",    # bech32 для удобства
    "created_at": 1234567890,
    "attestations": [                   # подписи от SmartRouter
      {"signer": "...", "sig": "...", "issued_at": ...}
    ],
    "links": {                          # внешние привязки
      "nostr_npub": "npub1...",
      "telegram_id": "12345",
      "solana_address": "...",
      "custom": {}
    }
  }
"""

import hashlib
import json
import os
import time
from pathlib import Path

IDENTITIES_DIR = Path.home() / "data" / "sites" / "relay-mesh" / "identities"
IDENTITIES_DIR.mkdir(parents=True, exist_ok=True)

# ─── Bech32 for mesh keys (custom HRP "nmesh") ───

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

def _bech32_polymod(values):
    GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
    chk = 1
    for v in values:
        b = chk >> 25
        chk = (chk & 0x1ffffff) << 5 ^ v
        for i in range(5):
            chk ^= GEN[i] if ((b >> i) & 1) else 0
    return chk

def _bech32_hrp_expand(hrp):
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]

def _convertbits(data, frombits, tobits, pad=True):
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for v in data:
        if v < 0 or (v >> frombits):
            return None
        acc = ((acc << frombits) | v) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        return None
    return ret

def _bech32_encode(hrp, data):
    combined = data + _bech32_checksum(hrp, data)
    return hrp + "1" + "".join(CHARSET[d] for d in combined)

def _bech32_checksum(hrp, data):
    values = _bech32_hrp_expand(hrp) + data
    polymod = _bech32_polymod(values) ^ 1
    return [(polymod >> (5 * (5 - i))) & 31 for i in range(6)]

def pubkey_to_bech32(pubkey_hex: str) -> str:
    """Конвертировать hex pubkey в nmesh1... bech32."""
    raw = bytes.fromhex(pubkey_hex)
    data = _convertbits(list(raw), 8, 5)
    if data is None:
        return f"nmesh1error"
    return _bech32_encode("nmesh", data)


# ─── Mesh Identity ───

def generate_mesh_key() -> dict:
    """
    Сгенерировать новый mesh-ключ secp256k1.
    Возвращает dict с mesh_pubkey (hex) и mesh_privkey (hex).
    """
    from nostr.key import PrivateKey
    key = PrivateKey()
    return {
        "mesh_pubkey": key.public_key.hex(),
        "mesh_privkey": key.raw_secret.hex() if hasattr(key.raw_secret, 'hex') else key.bech32(),
    }


def load_or_create_identity(agent_name: str) -> dict:
    """
    Загрузить mesh identity агента из файла.
    Если файла нет — сгенерировать новый ключ и сохранить.
    
    Returns:
        dict с mesh_pubkey (hex), mesh_npub_bech32, created_at, links
    """
    path = IDENTITIES_DIR / f"{agent_name}.json"
    
    if path.exists():
        # Пустой или битый — удаляем и создаём заново
        if path.stat().st_size == 0:
            path.unlink()
            print(f"⚠️ Пустой файл {agent_name}.json — удалён, будет создан заново")
            return load_or_create_identity(agent_name)
        try:
            with open(path) as f:
                data = json.load(f)
        except json.JSONDecodeError:
            path.unlink()
            print(f"⚠️ Битый файл {agent_name}.json — удалён, будет создан заново")
            return load_or_create_identity(agent_name)
        # Ensure packet keys exist (upgrade old identities)
        if not data.get("packet_privkey"):
            from cryptography.hazmat.primitives.asymmetric import ed25519
            _sk = ed25519.Ed25519PrivateKey.generate()
            data["packet_privkey"] = _sk.private_bytes_raw().hex()
            data["packet_pubkey"] = _sk.public_key().public_bytes_raw().hex()
            data["updated_at"] = time.time()
        # ═══ Фаза 2: X25519 cipher keys (для шифрования gossip) ═══
        if not data.get("cipher_privkey"):
            from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
            _xsk = X25519PrivateKey.generate()
            data["cipher_privkey"] = _xsk.private_bytes_raw().hex()
            data["cipher_pubkey"] = _xsk.public_key().public_bytes_raw().hex()
            data["updated_at"] = time.time()
        if data.get("updated_at", 0) > data.get("created_at", 0):
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
        return data
    
    # Новый ключ
    key_data = generate_mesh_key()
    mesh_pubkey = key_data["mesh_pubkey"]
    mesh_privkey = key_data["mesh_privkey"]
    mesh_npub = pubkey_to_bech32(mesh_pubkey)
    
    identity = {
        "agent_name": agent_name,
        "mesh_pubkey": mesh_pubkey,
        "mesh_privkey": mesh_privkey,
        "mesh_npub": mesh_npub,
        "packet_pubkey": "",      # Ed25519 for fast packet signing (generated below)
        "packet_privkey": "",     # Ed25519
        "cipher_pubkey": "",      # X25519 (для шифрования gossip)
        "cipher_privkey": "",     # X25519
        "created_at": time.time(),
        "updated_at": time.time(),
        "attestations": [],
        "links": {},
    }
    
    # Generate Ed25519 packet signing key
    from cryptography.hazmat.primitives.asymmetric import ed25519
    _sk = ed25519.Ed25519PrivateKey.generate()
    identity["packet_privkey"] = _sk.private_bytes_raw().hex()
    identity["packet_pubkey"] = _sk.public_key().public_bytes_raw().hex()
    
    # Generate X25519 cipher key (для шифрования gossip)
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    _xsk = X25519PrivateKey.generate()
    identity["cipher_privkey"] = _xsk.private_bytes_raw().hex()
    identity["cipher_pubkey"] = _xsk.public_key().public_bytes_raw().hex()
    
    with open(path, "w") as f:
        json.dump(identity, f, indent=2)
    
    return identity


def link_external(agent_name: str, service: str, identifier: str) -> dict:
    """
    Привязать внешний идентификатор к mesh pubkey.
    
    Args:
        agent_name: имя агента
        service: "nostr_npub", "telegram_id", "solana_address", "custom"
        identifier: значение
    
    Returns:
        обновлённый identity
    """
    identity = load_or_create_identity(agent_name)
    identity["links"][service] = identifier
    identity["updated_at"] = time.time()
    
    path = IDENTITIES_DIR / f"{agent_name}.json"
    with open(path, "w") as f:
        json.dump(identity, f, indent=2)
    
    return identity


def get_identity(agent_name: str) -> dict:
    """Получить mesh identity агента."""
    return load_or_create_identity(agent_name)


# ─── DID Resolver: did:snin:pubkey ───

DID_PREFIX = "did:snin:"

def pubkey_to_did(pubkey_hex: str) -> str:
    """Конвертировать pubkey в формат did:snin:pubkey.
    
    Пример: did:snin:02abc123... (33 байта compressed hex)
    """
    return f"{DID_PREFIX}{pubkey_hex}"


def did_to_pubkey(did: str) -> str:
    """Извлечь pubkey из did:snin:pubkey."""
    if not did.startswith(DID_PREFIX):
        raise ValueError(f"Invalid DID: {did}")
    return did[len(DID_PREFIX):]


def resolve_did(did: str) -> dict:
    """Разрешить DID → identity.
    
    Ищет по всем локальным identity файлам.
    Если не найден — возвращает базовый профиль.
    
    Args:
        did: did:snin:pubkey
    
    Returns:
        dict с pubkey, npub, agent_name (если известен), attestations, links
    """
    pubkey = did_to_pubkey(did)
    
    # Ищем среди локальных агентов
    for fpath in sorted(IDENTITIES_DIR.glob("*.json")):
        name = fpath.stem
        identity = load_or_create_identity(name)
        if identity.get("mesh_pubkey") == pubkey:
            return {
                "did": did,
                "pubkey": pubkey,
                "npub": identity.get("mesh_npub", ""),
                "agent_name": name,
                "created_at": identity.get("created_at", 0),
                "attestations": identity.get("attestations", []),
                "links": identity.get("links", {}),
                "resolved": True,
            }
    
    # Неизвестный pubkey — базовый профиль
    return {
        "did": did,
        "pubkey": pubkey,
        "npub": pubkey_to_bech32(pubkey),
        "agent_name": "unknown",
        "created_at": 0,
        "attestations": [],
        "links": {},
        "resolved": False,
    }






# ─── Signature-based Attestation ───

def sign_attestation(agent_name: str, target_did: str, role: str = "agent") -> dict:
    """Подписать soulbound аттестацию для target_did.
    
    Args:
        agent_name: кто подписывает (аттестатор)
        target_did: кому (did:snin:pubkey)
        role: роль (agent, oracle, verifier, router)
    
    Returns:
        attestation dict
    """
    identity = load_or_create_identity(agent_name)
    message = json.dumps({
        "target_did": target_did,
        "attester": identity["mesh_pubkey"],
        "role": role,
        "issued_at": time.time(),
    }, sort_keys=True)
    
    signature = sign_with_mesh_key(agent_name, message)
    
    attestation = {
        "type": "soulbound",
        "target_did": target_did,
        "attester_pubkey": identity["mesh_pubkey"],
        "attester_npub": identity["mesh_npub"],
        "role": role,
        "signature": signature,
        "issued_at": time.time(),
    }
    
    # Сохраняем в DHT-подобный файл (аттестации публичные)
    attest_dir = IDENTITIES_DIR / "attestations"
    attest_dir.mkdir(exist_ok=True)
    attest_file = attest_dir / f"{target_did.replace(':', '_')}.json"
    
    try:
        with open(attest_file) as f:
            attests = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        attests = []
    
    attests.append(attestation)
    with open(attest_file, "w") as f:
        json.dump(attests, f, indent=2)
    
    return attestation


def get_attestations(target_did: str) -> list[dict]:
    """Получить все аттестации для target_did."""
    attest_dir = IDENTITIES_DIR / "attestations"
    attest_file = attest_dir / f"{target_did.replace(':', '_')}.json"
    try:
        with open(attest_file) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def get_all_dids() -> list[dict]:
    """Получить DID всех известных агентов."""
    dids = []
    for fpath in sorted(IDENTITIES_DIR.glob("*.json")):
        if "attestations" in str(fpath):
            continue
        name = fpath.stem
        identity = load_or_create_identity(name)
        did = pubkey_to_did(identity["mesh_pubkey"])
        dids.append({
            "did": did,
            "agent_name": name,
            "npub": identity.get("mesh_npub", ""),
            "pubkey": identity["mesh_pubkey"],
            "attestations": len(identity.get("attestations", [])),
            "links": list(identity.get("links", {}).keys()),
        })
    return dids


def attest_identity(agent_name: str, signer_pubkey: str, signature: str) -> dict:
    """
    Добавить аттестацию от SmartRouter (или другого верификатора).
    
    Args:
        agent_name: имя агента
        signer_pubkey: pubkey верификатора
        signature: подпись
    
    Returns:
        обновлённый identity
    """
    identity = load_or_create_identity(agent_name)
    identity["attestations"].append({
        "signer": signer_pubkey,
        "signature": signature,
        "issued_at": time.time(),
    })
    
    path = IDENTITIES_DIR / f"{agent_name}.json"
    with open(path, "w") as f:
        json.dump(identity, f, indent=2)
    
    return identity


def sign_with_mesh_key(agent_name: str, data: str) -> str:
    """
    Подписать данные mesh-ключом агента.
    
    Args:
        agent_name: имя агента
        data: строка для подписи
    
    Returns:
        hex подпись
    """
    from nostr.key import PrivateKey
    
    identity = load_or_create_identity(agent_name)
    privkey_hex = identity["mesh_privkey"]
    
    if len(privkey_hex) == 64:
        key = PrivateKey(bytes.fromhex(privkey_hex))
    else:
        key = PrivateKey.from_nsec(privkey_hex)
    
    sig = key.sign_message_hash(hashlib.sha256(data.encode()).digest())
    return sig.hex() if hasattr(sig, 'hex') else sig


# ─── CLI ───
if __name__ == "__main__":
    import sys
    
    agents = ["forecaster_ai", "archivist_ai", "anton_ai"]
    
    print("╔════════════════════════════════════════════╗")
    print("║     SNIN Mesh Identity Manager            ║")
    print("╚════════════════════════════════════════════╝")
    print()
    
    for name in agents:
        identity = load_or_create_identity(name)
        links = identity.get("links", {})
        attest_count = len(identity.get("attestations", []))
        
        print(f"  {name}:")
        print(f"    mesh pubkey: {identity['mesh_pubkey'][:24]}...")
        print(f"    mesh npub:   {identity['mesh_npub']}")
        print(f"    created:     {time.strftime('%Y-%m-%d %H:%M', time.gmtime(identity['created_at']))}")
        print(f"    attestations: {attest_count}")
        print(f"    links:       {', '.join(links.keys()) if links else 'none'}")
        print()
    
    # Если есть аргумент — показать детально
    if len(sys.argv) > 1:
        name = sys.argv[1]
        identity = load_or_create_identity(name)
        print(f"\n{'='*50}")
        print(f"  Identity: {name}")
        print(f"{'='*50}")
        print(json.dumps(identity, indent=2))
