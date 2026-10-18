# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native control CREATE with a distinct protected namespace owner."""

import secrets
from contextlib import asynccontextmanager
from types import SimpleNamespace

from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import provision_plan_release_storage
from tests.plan_release_storage_postgres_support import install_legacy_guards, storage_case


@asynccontextmanager
async def control_reader_case(*, provisioned=True, legacy_guards=False):
    async with storage_case(migrated=False) as case:
        admin = await case.connect()
        control = case.reader.replace("plan_reader_", "plan_control_")
        namespace_owner = case.reader.replace("plan_reader_", "plan_schema_")
        for role, login in ((control, True), (namespace_owner, False)):
            credential = ""
            if login:
                case.role_passwords[role] = secrets.token_hex(32)
                password = await admin.fetchval("SELECT quote_literal($1::text)", case.role_passwords[role])
                credential = " PASSWORD " + password
            await admin.execute(f'CREATE ROLE "{role}" ' + ("LOGIN" if login else "NOLOGIN") + credential)
            case.created_roles.append(role)
        await admin.execute(f'ALTER SCHEMA "{case.schema}" OWNER TO "{namespace_owner}"')
        await admin.execute(f'GRANT USAGE,CREATE ON SCHEMA "{case.schema}" TO "{control}"')
        if legacy_guards:
            async with case.engine.begin() as connection:
                await connection.run_sync(lambda native: install_legacy_guards(native, case.schema))
        await case.migrate()
        profile_by_name = {**case.profile, "reader_roles": (control, case.reader), "control_reader_roles": (control,)}

        async def verify(connection, schema_name, **observed):
            # Mechanics fixture only: the production IC callback verifies the
            # complete enrolled role/profile, never this supplemental checker.
            assert observed == profile_by_name
            await storage_roles(connection, schema_name, **observed)
            assert await connection.fetchval("SELECT current_user=session_user")

        if provisioned:
            async with admin.transaction():
                await provision_plan_release_storage(admin, case.schema, **profile_by_name, verify_role_profile=verify)
        yield SimpleNamespace(
            case=case,
            admin=admin,
            control=control,
            namespace_owner=namespace_owner,
            profile=profile_by_name,
            verify=verify,
        )
