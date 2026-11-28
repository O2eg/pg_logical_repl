"""Version pairs and the narrow schema equivalences allowed during upgrades."""

import json

import pytest

from pg_logical_repl.capabilities import describe, supports_pair
from pg_logical_repl.catalog import schema_differences
from pg_logical_repl.roles import target_memberships


def test_advertised_directions_match_preflight():
    caps = describe()
    pairs = {(p["source"], p["target"]) for p in caps["cross_major_pairs"] if p["ddl_modes"] == ["frozen"]}
    for source in range(14, 21):
        for target in range(14, 21):
            assert supports_pair(source, target) == (15 <= source <= target <= 19)
            assert ((source, target) in pairs) == (15 <= source < target <= 19)


def inventory(major):
    privileges = ["DELETE", "INSERT", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE"]
    if major >= 17:
        privileges.insert(2, "MAINTAIN")
    grants = [dict(privilege=p, grantor="owner", grantee="owner", grantable=False) for p in privileges]
    rel = dict(
        owner="owner",
        extension=None,
        columns=[dict(attname="amount", attnotnull=True)],
        acl=json.dumps(grants),
        constraints=[dict(contype="n", definition="NOT NULL amount", convalidated=True, comment=None)]
        if major >= 18
        else [],
    )
    return dict(
        identity=dict(major=major),
        relations=[rel],
        functions=[],
        extensions=[],
        schemas=[],
        types=[],
        default_acl=[],
        database={},
        role_settings=[],
        unsupported_objects=[],
    )


@pytest.mark.parametrize("target_major", [18, 19])
def test_catalog_only_normalizes_expected_cross_version_differences(target_major):
    source, target = inventory(16), inventory(target_major)
    assert schema_differences(source, target) == []
    rel = target["relations"][0]
    rel["constraints"][0]["convalidated"] = False
    assert schema_differences(source, target)
    rel["constraints"][0]["convalidated"] = True
    rel["columns"][0]["attnotnull"] = False
    assert schema_differences(source, target)
    rel["columns"][0]["attnotnull"] = True
    grants = json.loads(rel["acl"])
    grants.append(dict(grantor="owner", grantee="reader", privilege="MAINTAIN", grantable=False))
    rel["acl"] = json.dumps(grants)
    assert schema_differences(source, target)
    assert schema_differences(inventory(target_major), target)


@pytest.mark.asyncio
async def test_pg15_memberships_keep_noinherit_and_set_role():
    class Source:
        async def fetch(self, query):
            return [dict(rolname="reader", rolinherit=False)]

    membership = dict(role="group", member="reader", grantor="admin", admin_option=False)
    assert await target_memberships(Source(), [membership], 15, 18) == [
        dict(membership, inherit_option=False, set_option=True)
    ]
    assert await target_memberships(Source(), [membership], 15, 15) == [membership]
