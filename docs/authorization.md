# Authorization

GWASStudio uses **HashiCorp Vault tokens** as the sole authentication mechanism.
There are no usernames or passwords: when you run a command, GWASStudio introspects
the Vault token you present, derives your identity (policies, username, stable
token accessor), and compares it against the **access rules stored in each
dataset's metadata record** in MongoDB.

Access is **default-deny**: unless the dataset is `public` or you match one of
its allow-lists, you cannot read it.

---

## Access levels

Every metadata record has an `access_level` field with one of three values.
If a record does not specify one, it defaults to `protected`.

| Level | Who can read | What you must present |
|---|---|---|
| `public` | Everyone | Nothing (anonymous access is allowed by default) |
| `protected` | Any authenticated user | Any **valid** Vault token |
| `restricted` | Only explicitly-allowed users | A token whose **policy**, **username**, or **accessor** is in the dataset's allow-lists |

> `public` datasets also accept authenticated tokens — being public never *blocks*
> a valid token, it just doesn't require one.

### Decision flow

```
no --vault-token / token invalid?
├── public dataset?     → allow only if anonymous public access is enabled (default: yes)
└── protected/restricted → DENY

valid token present?
├── public     → ALLOW
├── protected  → ALLOW
└── restricted → ALLOW if token matches ANY allow-list, else DENY
```

---

## The three allow-lists (for `restricted` datasets)

A `restricted` dataset can grant access through up to three independent mechanisms.
**Matching any one of them is sufficient** (logical OR). All three are enabled by
default and can be individually toggled in the `AuthConfig`.

| Metadata field | Matches against | How to grant it |
|---|---|---|
| `allowed_policies` | The Vault **policies** attached to your token | Attach a policy to your token |
| `allowed_users` | Your **username** (parsed from the token's `display_name`) | Name your token with the right `display_name` |
| `allowed_token_accessors` | The token's stable **accessor** ID | Record the accessor of a specific token |

Each field holds a **list of strings**. When ingesting, values are normalized so a
comma-separated string like `"team-a-reader, team-b-reader"` becomes a proper list
`["team-a-reader", "team-b-reader"]` — you do not have to worry about list vs. string.

> **Order of checks** (first match wins): accessor → username → policy.
> All checks run against the same `restricted` dataset; the first that matches
> grants access.

### What `restricted` + empty allow-lists means

If a dataset is `restricted` but has **no** `allowed_policies`, `allowed_users`, or
`allowed_token_accessors`, it is effectively **read-inaccessible to everyone**,
because the intersection of "your policies" with an empty set is always empty.
This is a safe default but easy to miss — see [Troubleshooting](#troubleshooting).

---

## Granting access

### 1. Set the level and allow-lists on the dataset (owner/admin)

Access rules live in the dataset's **metadata record**, which is populated from the
metadata table (TSV) at `ingest` time. Add the relevant columns to your metadata
row:

| Column | Example values |
|---|---|
| `access_level` | `public` · `protected` · `restricted` |
| `allowed_policies` | `computationalbiobank-reader` (or comma-separated: `team-a-reader, team-b-reader`) |
| `allowed_users` | `alice`, `bob` (comma-separated OK) |
| `allowed_token_accessors` | `dkE5rNWmE6rCyg41gTB0WJT2` |

Examples:

```
# public  — anyone, no token needed
access_level = public

# protected — anyone with a valid token
access_level = protected

# restricted — only holders of the "computationalbiobank-reader" policy
access_level = restricted
allowed_policies = computationalbiobank-reader

# restricted — only a named individual (username)
access_level = restricted
allowed_users = alice

# restricted — multiple policies AND/OR multiple users all work (OR across all three)
access_level = restricted
allowed_policies = computationalbiobank-reader, fht-internal
allowed_users    = alice, bob
```

> Re-ingest (or update the metadata record) after changing these values so that
> MongoDB reflects the new rules.

### 2. Create a Vault token that satisfies the rules

Below are full, copy-pasteable Vault CLI examples. They assume the FHT Vault at
`https://vault.fht.org` and the shared read policy `computationalbiobank-reader`.
Replace the policy and address with your environment's values.

> Create tokens with the **root** or a **token-creator** capable token, or via your
> organization's standard `vault token create` flow.

#### A. Access a `public` study — no token required

You can query a public study without any token at all:

```
gwasstudio list
gwasstudio export --search-file search.yml --output-prefix ./out
```

If you *do* have a token, it works too (public never rejects a valid token).

#### B. Access a `protected` study — any valid token

Any valid token is sufficient; the policy is irrelevant. Give the token a stable,
human-readable `display_name` (recommended) and a sensible TTL:

```
vault token create \
  -address=https://vault.fht.org \
  -display-name="token-GWASStudio-alice-fht-org" \
  -ttl=720h \
  -renewable
```

Use the returned token (see [Using your token](#using-your-token)).

#### C. Access a `restricted` study — via a matching **policy** (most common)

1. Create (or reuse) the policy that the dataset's `allowed_policies` lists.
2. Create a token that carries that policy:

```
# Create a named policy once (admin) — only if it does not already exist
cat > cb-reader.hcl <<'HCL'
# grants read access to the computational biobank
path "metadata/computationalbiobank/*" {
  capabilities = ["read"]
}
HCL
vault policy write computationalbiobank-reader cb-reader.hcl -address=https://vault.fht.org

# Create a token that carries the policy
vault token create \
  -address=https://vault.fht.org \
  -policy=computationalbiobank-reader \
  -display-name="token-GWASStudio-alice-fht-org" \
  -ttl=720h \
  -renewable
```

#### D. Access a `restricted` study — via a matching **username**

The username is parsed from the token `display_name` using the convention
`token-GWASStudio-<username>[-<org>]`. Name the token accordingly and make sure
`allowed_users` on the dataset contains that username:

```
vault token create \
  -address=https://vault.fht.org \
  -display-name="token-GWASStudio-alice-fht-org" \
  -ttl=720h \
  -renewable
```

with the dataset configured as `access_level=restricted` and
`allowed_users=alice`.

#### E. Access a `restricted` study — via a matching **accessor**

For a specific, revocable, machine-owned token, grant by its stable accessor:

```
# Create the token
vault token create \
  -address=https://vault.fht.org \
  -display-name="gwasstudio-service-account" \
  -ttl=8760h \
  -renewable

# Read back its accessor (also shows the token string)
vault token capabilities -address=https://vault.fht.org
vault token lookup -address=https://vault.fht.org
```

Record the `accessor` value into the dataset's
`allowed_token_accessors` column. Access by accessor is the most stable identity
(independent of TTL renewals and display names) and is checked **first**.

> **Combining mechanisms**: a token can satisfy *any* of the three. For example a
> token carrying `computationalbiobank-reader` *and* named `token-GWASStudio-alice-*`
> would be granted if *either* the policy or the username is allow-listed.

---

## Using your token

Pass the token explicitly:

```
gwasstudio --vault-token=<TOKEN> list
gwasstudio --vault-token=<TOKEN> export --search-file search.yml --output-prefix ./out
```

Or save it once and it is picked up automatically from your home directory:

```
echo '<TOKEN>' > "${HOME}/.vault-token"
```

> The token is **personal and confidential** — do not commit it or share it.
> If `~/.vault-token` is missing and no `--vault-token` is given, you will be
> prompted to paste the token interactively.

Other Vault connection options (usually server-side defaults are fine):
`--vault-url`, `--vault-auth basic|oidc`, `--vault-mount-point`, `--vault-path`.

Inspect a token (what policies it carries, when it expires) with the helper:

```
python scripts/test_vault_token_info.py
```

---

## What happens when you're denied

When a query matches only datasets you are not allowed to read, they are routed to a
"skipped" bucket and no data is written. By default this is a **silent no-op**
(exit 0) so interactive use stays quiet, but that is dangerous in pipelines.

Use `--fail-on-empty` to make denial a **hard failure** (exit code `3`) with a
structured, greppable message:

```
gwasstudio --vault-token=<TOKEN> export --search-file search.yml \
  --output-prefix ./out --fail-on-empty
# on denial:
#   ERROR: NO_DATA: authorization - No accessible datasets found: 1 dataset(s)
#         matched the query but were denied by authorization. Check the dataset's
#         access_level and its allowed_policies / allowed_users /
#         allowed_token_accessors, or present a token carrying a matching policy.
#   → exit 3
```

Related `--fail-on-empty` messages: `NO_DATA: no-results` (query matched nothing)
and `NO_DATA: query returned 0 rows` (matched, but region/traits yield no data).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Denied on a `restricted` study | Token lacks the required policy / wrong username / accessor not allow-listed | Create a token with the matching policy (or name/accessor) |
| Denied even with a valid token | Dataset is `restricted` with **empty** allow-lists (default-deny) | Add `allowed_policies` (or users/accessors) to the metadata and re-ingest |
| Comma list not matching | Allow-list stored as one comma-joined string | Re-ingest — values are now normalized into a list automatically |
| Works interactively, fails in CI | Token not present in the non-interactive shell | Pass `--vault-token=<TOKEN>` explicitly |
| "Vault configuration incomplete" | `--vault-url` / `--vault-token` / `--vault-path` missing | Supply all three (usually only the token is required) |
| Token expired | TTL elapsed and not renewed | Create a `-renewable` token or renew it, then re-test |

---

## Reference

- Access levels: `public`, `protected`, `restricted` (`AccessLevel`)
- Allow-lists: `allowed_policies`, `allowed_users`, `allowed_token_accessors`
- Auth toggles: `auth.enabled`, `auth.allow_anonymous_public`, `auth.use_policies`,
  `auth.use_username`, `auth.use_accessor` (defaults all `true`)
- Default level when unset: `protected`
- Implementation: `src/gwasstudio/core/authorization.py`
