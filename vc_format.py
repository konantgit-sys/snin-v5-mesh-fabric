"""Verifiable Credentials — W3C VC формат для SNIN L5."""

import json
import time
import uuid


def create_vc(issuer_did: str, subject_did: str, credential_type: str,
              claims: dict, expiration_days: int = 365) -> dict:
    """Создаёт Verifiable Credential в W3C формате."""
    vc = {
        "@context": [
            "https://www.w3.org/2018/credentials/v1",
            "https://www.w3.org/2018/credentials/examples/v1"
        ],
        "id": f"urn:snin:vc:{uuid.uuid4().hex[:16]}",
        "type": ["VerifiableCredential", credential_type],
        "issuer": issuer_did,
        "issuanceDate": datetime_iso(),
        "expirationDate": datetime_iso(days=expiration_days),
        "credentialSubject": {
            "id": subject_did,
            **claims
        },
        "proof": {
            "type": "Ed25519Signature2020",
            "created": datetime_iso(),
            "verificationMethod": f"{issuer_did}#key-1",
            "proofPurpose": "assertionMethod",
        }
    }
    return vc


def create_did_document(pubkey_hex: str, agent_name: str, services: list[dict] = None) -> dict:
    """Создаёт W3C DID Document для did:snin:pubkey."""
    did = f"did:snin:{pubkey_hex[:16]}"
    doc = {
        "@context": [
            "https://www.w3.org/ns/did/v1",
            "https://w3id.org/security/suites/ed25519-2020/v1"
        ],
        "id": did,
        "alsoKnownAs": [f"snin://agent/{agent_name}"],
        "verificationMethod": [{
            "id": f"{did}#key-1",
            "type": "Ed25519VerificationKey2020",
            "controller": did,
            "publicKeyMultibase": f"z{pubkey_hex}"
        }],
        "authentication": [f"{did}#key-1"],
        "assertionMethod": [f"{did}#key-1"],
        "service": services or [
            {
                "id": f"{did}#mesh",
                "type": "SNINMeshEndpoint",
                "serviceEndpoint": f"https://snin-hub.v2.site"
            }
        ]
    }
    return doc


def attestation_to_vc(attestation: dict) -> dict:
    """Конвертирует внутреннюю аттестацию в VC."""
    return create_vc(
        issuer_did=attestation.get("issuer_did", "did:snin:unknown"),
        subject_did=attestation.get("target_did", attestation.get("target", "did:snin:unknown")),
        credential_type=f"SNIN{attestation.get('role', 'agent').capitalize()}Attestation",
        claims={
            "role": attestation.get("role", "agent"),
            "weight": attestation.get("weight", 1.0),
            "description": attestation.get("description", ""),
        }
    )


def datetime_iso(days: int = 0) -> str:
    """ISO 8601 timestamp, опционально +N дней."""
    t = time.time() + days * 86400
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
