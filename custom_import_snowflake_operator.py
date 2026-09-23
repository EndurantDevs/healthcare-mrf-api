# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Redacted entry point for the retained Snowflake source-binding operator."""

from __future__ import annotations

import sys
import warnings

_CANCELED = '{"code":"canceled","status":"error"}'
_FAILED = '{"code":"failed","status":"error"}'


def main() -> int:
    """Load the operator command behind a pre-import redaction boundary."""

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            from process.custom_import.snowflake_operator_cli import run_command
        except KeyboardInterrupt:
            print(_CANCELED, file=sys.stderr)
            return 130
        except BaseException:
            print(_FAILED, file=sys.stderr)
            return 1

        try:
            return run_command()
        except SystemExit:
            raise
        except KeyboardInterrupt:
            print(_CANCELED, file=sys.stderr)
            return 130
        except BaseException:
            print(_FAILED, file=sys.stderr)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
