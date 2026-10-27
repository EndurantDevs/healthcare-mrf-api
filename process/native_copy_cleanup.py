# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native COPY driver ownership and bounded cancellation handshakes."""

from process.uhc_flex_practitioner_async_safety import drain_operation


async def native_model_copy_driver(session, methods, require_transaction, error_type):
    """Require the caller's actual transaction before entering the native protocol."""
    if callable(getattr(session, "is_in_transaction", None)):
        driver = session
    else:
        require_transaction(session)
        connection = await session.connection()
        driver = (await connection.get_raw_connection()).driver_connection
    if (
        not callable(getattr(driver, "is_in_transaction", None))
        or not driver.is_in_transaction()
        or any(not callable(getattr(driver, name, None)) for name in methods)
    ):
        raise error_type("native model acquisition COPY is unavailable")
    return driver


async def close_interrupted_copy(driver):
    """Abort the exact unusable COPY transport and its pending cancellation work."""
    # A graceful close can wait forever for cancellation after COPY already ended.
    driver.terminate()


async def invalidate_copy_owner(session, driver):
    """Tell SQLAlchemy its native transport closed before transaction scope exit."""
    invalidate = getattr(session, "invalidate", None) if session is not driver else None
    if callable(invalidate):
        await drain_operation(invalidate(), preserve_cancellation=False)
