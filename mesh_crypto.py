"""Mesh Crypto — шифрование gossip-сообщений между агентами.

Схема (Phase 2):
1. X25519 DH(отправитель_priv, получатель_pub) → shared secret
2. HKDF-SHA256(shared_secret) → AES-256-GCM key  
3. Encrypt/Decrypt с random nonce

Использует cipher_pubkey/cipher_privkey (X25519) из mesh_identity.json.
Генерируются автоматически при load_or_create_identity().
"""

import base64, os, json
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey


def encrypt_for_agent(
    plaintext: str,
    recipient_cipher_pubkey_hex: str,
    my_cipher_privkey_hex: str
) -> str:
    """Зашифровать plaintext для агента.
    
    Args:
        plaintext: текст для шифрования
        recipient_cipher_pubkey_hex: cipher_pubkey получателя (X25519, hex)
        my_cipher_privkey_hex: МОЙ cipher_privkey (X25519, hex)
    
    Returns:
        base64: nonce(12b) + ciphertext + tag(16b)
    """
    my_priv = X25519PrivateKey.from_private_bytes(bytes.fromhex(my_cipher_privkey_hex))
    recip_pub = X25519PublicKey.from_public_bytes(bytes.fromhex(recipient_cipher_pubkey_hex))
    
    # DH shared secret
    shared = my_priv.exchange(recip_pub)
    
    # HKDF → AES-256 key
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=b"mesh-crypto-v1",
    )
    aes_key = hkdf.derive(shared)
    
    # AES-GCM encrypt
    nonce = os.urandom(12)
    aesgcm = AESGCM(aes_key)
    ciphertext = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
    
    return base64.b64encode(nonce + ciphertext).decode("utf-8")


def decrypt_from_agent(
    cipherb64: str,
    my_cipher_privkey_hex: str,
    sender_cipher_pubkey_hex: str
) -> str:
    """Расшифровать cipherb64 от агента.
    
    Args:
        cipherb64: base64: nonce(12b) + ciphertext + tag(16b)
        my_cipher_privkey_hex: МОЙ cipher_privkey (X25519, hex)
        sender_cipher_pubkey_hex: cipher_pubkey отправителя (X25519, hex)
    
    Returns:
        расшифрованный plaintext (str)
    """
    my_priv = X25519PrivateKey.from_private_bytes(bytes.fromhex(my_cipher_privkey_hex))
    sender_pub = X25519PublicKey.from_public_bytes(bytes.fromhex(sender_cipher_pubkey_hex))
    
    shared = my_priv.exchange(sender_pub)
    
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=b"mesh-crypto-v1",
    )
    aes_key = hkdf.derive(shared)
    
    raw = base64.b64decode(cipherb64)
    nonce = raw[:12]
    ciphertext = raw[12:]
    
    aesgcm = AESGCM(aes_key)
    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    return plaintext.decode("utf-8")


def load_identity(agent_name: str) -> dict:
    """Загрузить mesh identity агента."""
    import os as _os
    path = _os.path.join(
        _os.path.dirname(_os.path.abspath(__file__)),
        "identities", f"{agent_name}.json"
    )
    with open(path) as f:
        return json.load(f)


def get_cipher_keys(agent_name: str) -> tuple[str, str]:
    """Получить cipher_pubkey и cipher_privkey агента из identity."""
    ident = load_identity(agent_name)
    return ident["cipher_pubkey"], ident["cipher_privkey"]


# ═══ Self-test ═══
if __name__ == "__main__":
    # Генерируем тестовые X25519 ключи
    alice_xsk = X25519PrivateKey.generate()
    bob_xsk = X25519PrivateKey.generate()
    
    alice_pub_hex = alice_xsk.public_key().public_bytes_raw().hex()
    alice_priv_hex = alice_xsk.private_bytes_raw().hex()
    bob_pub_hex = bob_xsk.public_key().public_bytes_raw().hex()
    bob_priv_hex = bob_xsk.private_bytes_raw().hex()
    
    msg = json.dumps({
        "type": "greeting",
        "from": "alice",
        "to": "bob",
        "text": "Hello from Alice!",
        "sequence": 1,
    })
    
    # Alice → Bob
    cipher = encrypt_for_agent(msg, bob_pub_hex, alice_priv_hex)
    print(f"✅ Encrypted: {len(cipher)}b")
    
    # Bob ← Alice
    plain = decrypt_from_agent(cipher, bob_priv_hex, alice_pub_hex)
    print(f"✅ Decrypted: {plain[:50]}...")
    assert plain == msg, "MISMATCH!"
    
    # Agent → Agent: load real keys
    print("\n--- Real agents ---")
    for name in ["forecaster_ai", "archivist_ai", "anton_ai"]:
        pub, priv = get_cipher_keys(name)
        print(f"  {name}: cipher_pub={pub[:16]}... cipher_priv={priv[:16]}...")
    
    print("\n✅ All tests PASSED")
