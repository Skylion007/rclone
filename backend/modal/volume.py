"""Process bridge for the public Modal Python SDK."""

import contextlib
import json
import posixpath
import random
import sys
import time

try:
    import modal
except ImportError:
    sys.exit("Install the Modal Python SDK in the configured interpreter (python -m pip install modal).")


def list_entries(volume, remote, recursive):
    # Volume v1's listing RPC can throttle bursts across checker processes.
    for attempt in range(6):
        with contextlib.closing(volume.iterdir(remote, recursive=recursive)) as entries:
            try:
                first = next(entries, None)
            except modal.exception.ResourceExhaustedError:
                if attempt == 5:
                    raise
            else:
                if first is not None:
                    yield first
                    yield from entries
                return
        time.sleep(random.uniform(0.25, 0.5) * 2**attempt)


def write_entry(entry):
    print(json.dumps({
        "path": entry.path, "type": entry.type.name,
        "size": entry.size, "mtime": entry.mtime,
    }))


def read_file(volume, remote, offset, limit):
    with contextlib.closing(volume.read_file(remote)) as chunks:
        for chunk in chunks:
            if offset >= len(chunk):
                offset -= len(chunk)
                continue
            # The SDK exposes sequential reads. Slice only the blocks we emit.
            data = memoryview(chunk)[offset:]
            offset = 0
            if limit >= 0:
                data = data[:limit]
                limit -= len(data)
            sys.stdout.buffer.write(data)
            if limit == 0:
                break


def listing_path(remote):
    return remote + "/" if remote.endswith("*") else remote


def run():
    operation, name, volume_id, environment, remote, *args = sys.argv[1:]
    volume = (modal.Volume.from_id(volume_id) if volume_id else
              modal.Volume.from_name(name, environment_name=environment or None))
    if operation == "resolve":
        volume.hydrate()
        print(json.dumps(volume.object_id))
    elif operation in ("stat", "list", "list-recursive"):
        # The SDK reserves a terminal '*' for its retired glob syntax.
        query = listing_path(remote)
        if operation == "stat" and remote.endswith("*"):
            # Volume v1 treats a terminal slash as part of a file lookup.
            parent = listing_path(posixpath.dirname(remote))
            with contextlib.closing(list_entries(volume, parent, False)) as entries:
                for entry in entries:
                    if entry.path.strip("/") == remote.strip("/"):
                        write_entry(entry)
                        return
            raise FileNotFoundError(remote)
        with contextlib.closing(list_entries(volume, query, operation == "list-recursive")) as entries:
            if operation == "stat":
                entry = next(entries, None)
                if entry is None or entry.path.strip("/") != remote.strip("/"):
                    print(json.dumps({"type": "DIRECTORY", "nonempty": entry is not None}))
                else:
                    write_entry(entry)
            else:
                for entry in entries:
                    write_entry(entry)
    elif operation == "read":
        read_file(volume, remote, int(args[0]), int(args[1]))
    elif operation == "put":
        with volume.batch_upload(force=True) as batch:
            batch.put_file(args[0], remote, mode=0o644)
    elif operation == "remove":
        volume.remove_file(remote, recursive=False)
    else:
        raise ValueError(f"Unknown operation {operation!r}")


if __name__ == "__main__":
    try:
        run()
    except (FileNotFoundError, modal.exception.NotFoundError) as exc:
        print(exc, file=sys.stderr)
        sys.exit(20)
    except modal.exception.InvalidError as exc:
        message = str(exc)
        print(message, file=sys.stderr)
        if message.rstrip(".") == "No such file or directory":
            sys.exit(20)
        if message.rstrip(".") == "Cannot remove directory without enabling recursive removal":
            sys.exit(21)
        sys.exit(1)
    except modal.exception.ConflictError as exc:
        print(exc, file=sys.stderr)
        sys.exit(22)
    except Exception as exc:
        # HTTP client exceptions may contain signed transfer URLs.
        if isinstance(exc, modal.exception.Error):
            print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        else:
            print(type(exc).__name__, file=sys.stderr)
        sys.exit(1)
