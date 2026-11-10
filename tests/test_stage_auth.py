"""Stage side-channel auth (TD-2295).

The attachments stream PUTs bytes into a Snowflake stage with its own
connection. It must accept the key-pair the MELTANO user has used since
dagster-vertex #47 (TARGET_SNOWFLAKE_PRIVATE_KEY) and must fail the sync,
not swallow the error, when it cannot connect at all.
"""

from __future__ import annotations

import base64

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from tap_microsoft_graph.streams.attachments import (
    StageUploadConfigError,
    snowflake_connect_kwargs,
    snowflake_private_key_der,
)

BASE_ENV = {
    "TARGET_SNOWFLAKE_ACCOUNT": "acct",
    "TARGET_SNOWFLAKE_USER": "MELTANO",
    "TARGET_SNOWFLAKE_ROLE": "MELTANO",
    "TARGET_SNOWFLAKE_WAREHOUSE": "ARCH",
}


@pytest.fixture(scope="module")
def key_material() -> dict[str, object]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    der = key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return {
        "der": der,
        "pem": pem.decode(),
        "b64_pem": base64.b64encode(pem).decode(),
        "b64_der": base64.b64encode(der).decode(),
    }


@pytest.mark.parametrize("encoding", ["pem", "b64_pem", "b64_der"])
def test_private_key_normalizes_every_encoding_to_der(key_material, encoding):
    assert snowflake_private_key_der(key_material[encoding]) == key_material["der"]


def test_private_key_tolerates_surrounding_whitespace(key_material):
    assert snowflake_private_key_der("\n " + key_material["b64_der"] + " \n") == key_material["der"]


def test_keypair_wins_over_password(key_material):
    env = {
        **BASE_ENV,
        "TARGET_SNOWFLAKE_PRIVATE_KEY": key_material["b64_der"],
        "TARGET_SNOWFLAKE_PASSWORD": "stale",
    }
    kwargs = snowflake_connect_kwargs(env)
    assert kwargs["private_key"] == key_material["der"]
    assert "password" not in kwargs
    assert kwargs["user"] == "MELTANO"


def test_password_is_the_fallback():
    kwargs = snowflake_connect_kwargs({**BASE_ENV, "TARGET_SNOWFLAKE_PASSWORD": "pw"})
    assert kwargs["password"] == "pw"
    assert "private_key" not in kwargs


def test_no_credential_is_a_config_error():
    with pytest.raises(StageUploadConfigError, match="neither TARGET_SNOWFLAKE_PRIVATE_KEY"):
        snowflake_connect_kwargs(dict(BASE_ENV))


def test_missing_account_is_a_config_error():
    env = {k: v for k, v in BASE_ENV.items() if k != "TARGET_SNOWFLAKE_ACCOUNT"}
    with pytest.raises(StageUploadConfigError, match="TARGET_SNOWFLAKE_ACCOUNT"):
        snowflake_connect_kwargs({**env, "TARGET_SNOWFLAKE_PASSWORD": "pw"})


def test_garbage_key_is_a_config_error():
    env = {**BASE_ENV, "TARGET_SNOWFLAKE_PRIVATE_KEY": base64.b64encode(b"not a key").decode()}
    with pytest.raises(StageUploadConfigError, match="not a PEM"):
        snowflake_connect_kwargs(env)
