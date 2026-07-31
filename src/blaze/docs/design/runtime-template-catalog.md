# Runtime Template Catalog

The daemon can publish a reusable runtime artifact set into the directory
configured by `runtime_templates.dir`. Imports are disabled unless an operator
also configures `runtime_templates.import_root`.

The catalog is separate from the existing in-memory `/v1/templates` registry.
It provides durable publication and lookup, but sandbox creation does not
select catalog entries.

## Import request

```http
POST /v1/runtime-templates/import
Content-Type: application/json

{
  "name": "runtime-base",
  "source": "runtime-base",
  "description": "base runtime"
}
```

`source` is a relative path below the configured import root. Absolute paths,
parent traversal, and symbolic links in the path are rejected. Every source
directory and file must be owned by the daemon user and must not be writable
by group or other users.

The source must contain top-level regular files named `vmstate.snap`,
`mem.bin`, and `rootfs.ext4`. An optional `template.json` must contain a JSON
object. Nested directories, links, and special files are rejected. The daemon
sets `name` from the request, applies a non-empty request description, and
fills numeric `rootfs_size` and `memory_size` defaults when they are absent.
It returns `409 Conflict` when the destination exists or another import of the
same name is active.

## Limits and owned paths

The following settings bound work before data is published:

| Setting | Meaning |
|---------|---------|
| `max_files` | Maximum files in one published entry, including `template.json` |
| `max_bytes` | Maximum artifact and generated metadata bytes in one entry |
| `max_metadata_bytes` | Maximum input and generated metadata size |
| `max_total_bytes` | Maximum committed bytes plus concurrent reservations |

`runtime_templates.dir` and `runtime_templates.import_root` must be absolute,
must not contain parent components, and must not overlap each other. They also
must not overlap the storage image, storage instance, or configured template
directories.

The catalog, staging directories, and published directories use mode `0700`.
Published files use mode `0600`.

## Publication and recovery

The importer opens source entries without following links, reserves catalog
capacity, and copies them into a private, uniquely named staging directory.
It checks the source identity and size again after copying. The complete
directory is synchronized and renamed into place without replacing an
existing entry, so readers see either no entry or the complete entry.

A failed import removes its staging directory. If cleanup cannot be completed,
or publication has occurred but catalog durability cannot be confirmed, the
daemon rejects later imports until the catalog is repaired and the daemon
restarts. Startup removes owned staging directories left by an interrupted
run and validates the type, ownership, permissions, contents, and capacity of
published entries.

During graceful shutdown, the daemon rejects new imports, requests
cancellation of active imports, waits for their file handles and staging data
to be released, and then continues normal runtime cleanup.

## Lookup and current limits

Published metadata is available through:

- `GET /v1/runtime-templates`
- `GET /v1/runtime-templates/{name}`

Catalog listing is sorted by template name and reports corrupt published
metadata instead of silently hiding it. These routes manage stored artifacts
only. Validation is structural; it does not prove that a snapshot is bootable
or compatible with a particular backend. This capability does not make
sandbox creation select an imported template, reference-count imported
entries, or remove their directories through the existing `/v1/templates/gc`
registry route.
