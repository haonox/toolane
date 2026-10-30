#!/usr/bin/env python3
"""Create a private, runnable Toolane configuration from this directory only."""

import argparse
import json
import os
import secrets
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROFILE = ROOT / "config" / "profile.json"
OUTPUT = ROOT / "generated" / "server.json"
FIXTURE = ROOT / "generated" / "io-fixture.sqlite"


def create_io_fixture() -> None:
    """Create a stable 1 MiB read-only fixture for the optional I/O tool."""
    if FIXTURE.exists():
        with sqlite3.connect(f"file:{FIXTURE}?mode=ro", uri=True) as database:
            count, minimum, maximum = database.execute(
                "SELECT COUNT(*), MIN(LENGTH(payload)), MAX(LENGTH(payload)) FROM records"
            ).fetchone()
        if (count, minimum, maximum) == (1024, 1024, 1024):
            return
        raise ValueError(f"invalid diagnostic I/O fixture: {FIXTURE}")
    descriptor, temporary = tempfile.mkstemp(prefix="io-fixture-", suffix=".sqlite", dir=FIXTURE.parent)
    os.close(descriptor)
    try:
        with sqlite3.connect(temporary) as database:
            database.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            database.executemany(
                "INSERT INTO records(id, payload) VALUES (?, ?)",
                ((index, f"{index:08d}" * 128) for index in range(1, 1025)),
            )
        os.chmod(temporary, 0o600)
        os.replace(temporary, FIXTURE)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--diagnostics", action="store_true", help="include optional diagnostic tools")
    args = parser.parse_args()
    profile = json.loads(PROFILE.read_text(encoding="utf-8"))
    tools = profile.pop("process_tools")
    if args.diagnostics:
        tools.extend(json.loads((ROOT / "config" / "diagnostic-tools.json").read_text(encoding="utf-8"))["process_tools"])
    tenant_count = profile.pop("tenant_count")
    if not isinstance(tenant_count, int) or not 1 <= tenant_count <= 5000:
        raise ValueError("tenant_count must be between 1 and 5000")
    if not isinstance(tools, list) or not tools:
        raise ValueError("process_tools must be a nonempty list")
    names = [item["name"] for item in tools]
    if len(names) != len(set(names)):
        raise ValueError("process tool names must be unique")
    config = dict(profile)
    config["tokens"] = [
        {"tenant_id": f"bench-{index + 1:04d}", "token": secrets.token_urlsafe(32), "tools": names}
        for index in range(tenant_count)
    ]
    config["process_tools"] = []
    for item in tools:
        script = (ROOT / item["script"]).resolve(strict=True)
        if not script.is_relative_to(ROOT) or not script.is_file():
            raise ValueError(f"tool script must be a file inside benchmarks: {script}")
        tool = {key: value for key, value in item.items() if key != "script"}
        tool.update({
            "executable": sys.executable,
            "directory": str(ROOT),
            "environment": {"PYTHONDONTWRITEBYTECODE": "1"},
            "args": [{"literal": str(script)}, *item["args"]],
        })
        config["process_tools"].append(tool)
    OUTPUT.parent.mkdir(mode=0o700, exist_ok=True)
    if args.diagnostics:
        create_io_fixture()
    if OUTPUT.exists():
        existing = json.loads(OUTPUT.read_text(encoding="utf-8"))
        existing_without_tokens = {key: value for key, value in existing.items() if key != "tokens"}
        config_without_tokens = {key: value for key, value in config.items() if key != "tokens"}
        if existing_without_tokens == config_without_tokens and len(existing.get("tokens", [])) == tenant_count:
            print(f"Using existing {OUTPUT} with {tenant_count} private tenant tokens")
            return 0
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = OUTPUT.with_name(f"server.backup-{stamp}-{os.getpid()}.json")
        OUTPUT.replace(backup)
        print(f"Saved previous private config as {backup}")
    with os.fdopen(os.open(OUTPUT, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w", encoding="utf-8") as stream:
        json.dump(config, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(f"Created {OUTPUT} with {tenant_count} private tenant tokens")
    print(f"Business: http://{config['business_listen']}  Metrics: http://{config['metrics_listen']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
