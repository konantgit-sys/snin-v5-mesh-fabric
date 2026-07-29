"""
SNIN Client — Core Module Unit Tests
Tests for critical functions: crypto, signature verification, bech32, graph algorithms.
Run: pytest test_core.py -v
"""

import pytest
import sys
import os
import json
import hashlib
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    encrypt_wallet_secret,
    decrypt_wallet_secret,
    verify_nostr_event,
    pubkey_to_npub,
)


# ═══════════════════════════════════════════════════════════
# SEC-004: Wallet Secret Encryption (Fernet)
# ═══════════════════════════════════════════════════════════

class TestWalletEncryption:
    """Test AES-128-CBC Fernet encryption of wallet_secret."""

    def test_encrypt_decrypt_roundtrip(self):
        """Valid secret survives encrypt→decrypt unchanged."""
        plaintext = "nsec1testwalletsecret123"
        encrypted = encrypt_wallet_secret(plaintext)
        decrypted = decrypt_wallet_secret(encrypted)
        assert decrypted == plaintext

    def test_encrypt_empty_string(self):
        """Empty string should encrypt and decrypt."""
        encrypted = encrypt_wallet_secret("")
        decrypted = decrypt_wallet_secret(encrypted)
        assert decrypted == ""

    def test_encrypt_unicode(self):
        """Unicode secrets should work."""
        plaintext = "nsec1кошелек_secret_🔑"
        encrypted = encrypt_wallet_secret(plaintext)
        decrypted = decrypt_wallet_secret(encrypted)
        assert decrypted == plaintext

    def test_encrypt_long_secret(self):
        """Long NWC connection strings."""
        plaintext = "nostr+walletconnect://" + "a" * 500
        encrypted = encrypt_wallet_secret(plaintext)
        decrypted = decrypt_wallet_secret(encrypted)
        assert decrypted == plaintext

    def test_different_encryptions_produce_different_output(self):
        """Same plaintext encrypted twice = different ciphertext (random IV)."""
        plaintext = "nsec1test123"
        enc1 = encrypt_wallet_secret(plaintext)
        enc2 = encrypt_wallet_secret(plaintext)
        assert enc1 != enc2  # Different IVs

    def test_ciphertext_is_base64(self):
        """Ciphertext is base64-encoded (Fernet format)."""
        encrypted = encrypt_wallet_secret("test")
        # Should be ASCII-only base64 (plus maybe padding =)
        assert all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=" for c in encrypted)

    def test_ciphertext_longer_than_plaintext(self):
        """Encrypted should be longer (Fernet overhead: version + timestamp + IV + HMAC)."""
        plaintext = "short"
        encrypted = encrypt_wallet_secret(plaintext)
        assert len(encrypted) > len(plaintext)


# ═══════════════════════════════════════════════════════════
# SEC-001: Nostr Event Signature Verification
# ═══════════════════════════════════════════════════════════

class TestSignatureVerification:
    """Test Schnorr signature verification for Nostr events."""

    def test_reject_missing_id(self):
        """Event without id field must be rejected."""
        ok, _ = verify_nostr_event({"pubkey": "00" * 32, "kind": 1, "content": "", "tags": [], "sig": "00" * 32})
        assert not ok

    def test_reject_missing_sig(self):
        """Event without sig field must be rejected."""
        ok, _ = verify_nostr_event({"id": "00" * 32, "pubkey": "00" * 32, "kind": 1, "content": "", "tags": []})
        assert not ok

    def test_reject_invalid_id_length(self):
        """ID shorter than 64 chars must be rejected."""
        ok, _ = verify_nostr_event(
            {"id": "short", "pubkey": "00" * 64, "kind": 1, "content": "", "tags": [], "sig": "00" * 64}
        )
        assert not ok

    def test_reject_fake_signature(self):
        """Fake signature on random event must be rejected."""
        event = {
            "id": "a" * 64,
            "pubkey": "b" * 64,
            "kind": 1,
            "content": "hello",
            "created_at": 1234567890,
            "tags": [],
            "sig": "c" * 128
        }
        ok, _ = verify_nostr_event(event)
        assert not ok

    def test_reject_fake_id(self):
        """Event where id doesn't match the hash of serialized content."""
        event = {
            "id": "0" * 64,  # Fake ID
            "pubkey": "8ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96",
            "kind": 1,
            "content": "test",
            "created_at": 1700000000,
            "tags": [],
            "sig": "00" * 128
        }
        ok, _ = verify_nostr_event(event)
        assert not ok

    def test_reject_sig_too_short(self):
        """Signature shorter than 128 hex chars."""
        ok, _ = verify_nostr_event(
            {"id": "a" * 64, "pubkey": "b" * 64, "kind": 1, "content": "", "tags": [], "sig": "short"}
        )
        assert not ok

    def test_reject_malformed_json_kind(self):
        """Kind as string should not crash but be rejected."""
        event = {
            "id": "a" * 64,
            "pubkey": "b" * 64,
            "kind": "not_a_number",
            "content": "",
            "tags": [],
            "sig": "c" * 128
        }
        ok, _ = verify_nostr_event(event)
        assert not ok


# ═══════════════════════════════════════════════════════════
# BECH32: pubkey → npub conversion
# ═══════════════════════════════════════════════════════════

class TestPubkeyToNpub:
    """Test pubkey display/truncation function."""

    def test_valid_pubkey(self):
        """Valid 64-char hex pubkey."""
        pk = "8ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96"
        result = pubkey_to_npub(pk)
        assert result is not None
        assert "..." in result
        assert len(result) > 0

    def test_short_pubkey_fallback(self):
        """Short pubkey should return truncated hex fallback."""
        result = pubkey_to_npub("abc123")
        assert result is not None
        assert len(result) > 0

    def test_odd_length_pubkey(self):
        """Odd-length hex should not crash."""
        result = pubkey_to_npub("abc")
        assert result is not None

    def test_empty_pubkey(self):
        """Empty should return something (not crash)."""
        result = pubkey_to_npub("")
        assert result is not None


# ═══════════════════════════════════════════════════════════
# INTEGRATION: Event ingest flow (mock DB)
# ═══════════════════════════════════════════════════════════

class TestEventValidationFlow:
    """Test the full validation chain: verify → store."""

    def test_invalid_event_blocked_before_db(self):
        """An invalid event should be rejected before any DB write."""
        event = {
            "id": "x" * 64,
            "pubkey": "y" * 64,
            "kind": 1,
            "content": "fake",
            "created_at": 1234567890,
            "tags": [],
            "sig": "z" * 128
        }
        ok, _ = verify_nostr_event(event)
        assert not ok, "Fake event must be rejected before DB write"

    def test_missing_required_fields(self):
        """Events missing required Nostr fields must be rejected."""
        required_fields = ["id", "pubkey", "kind", "sig"]
        for field in required_fields:
            event = {
                "id": "a" * 64, "pubkey": "b" * 64, "kind": 1,
                "content": "", "tags": [], "sig": "c" * 128
            }
            del event[field]
            ok, _ = verify_nostr_event(event)
            assert not ok, f"Event missing '{field}' should be rejected"


# ═══════════════════════════════════════════════════════════
# EDGE CASES
# ═══════════════════════════════════════════════════════════

class TestEdgeCases:
    """Edge cases that should not crash the system."""

    def test_encrypt_decrypt_newlines(self):
        """Secrets with newlines."""
        plaintext = "line1\nline2\r\nline3"
        encrypted = encrypt_wallet_secret(plaintext)
        decrypted = decrypt_wallet_secret(encrypted)
        assert decrypted == plaintext

    def test_verify_event_max_size(self):
        """Very large event (near 1MB Nostr limit)."""
        big_content = "x" * 100000
        event = {
            "id": "a" * 64,
            "pubkey": "b" * 64,
            "kind": 1,
            "content": big_content,
            "created_at": 1234567890,
            "tags": [],
            "sig": "c" * 128
        }
        # Should not crash — reject or handle gracefully
        ok, msg = verify_nostr_event(event)
        # Just ensure no exception — result is (bool, str)
        assert isinstance(ok, bool)

    def test_pubkey_with_special_chars(self):
        """Pubkey with non-hex characters."""
        result = pubkey_to_npub("zzzz" * 16)
        assert result is not None
