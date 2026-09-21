# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Redacted standalone entry point for custom-import operator commands."""

from __future__ import annotations

import sys


_CANCELED = '{"code":"canceled","status":"error"}'
_FAILED = '{"code":"failed","status":"error"}'


def main() -> int:
    """Load the application command behind a pre-import redaction boundary."""

    try:
        from process.custom_import.cli import run_command
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
