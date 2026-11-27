# Connector credential encryption: does a per-app key help?

Checked on 2026-11-23 at `2ef00b1c6f161f13045511dade4baaa347e4b5b6` (`2ef00b1c6` below).

## Answer first

1. No. A key derived from the master key and the `app_id` (HKDF salt or info) does not limit what a leaked master key exposes: the derived key is a public function of the master key ([S1], [S2]).
2. The swap protection that per-app keys would give, we already have: every connector credential blob is AEAD-bound to customer, connection and revision ([C4]), and AAD is authenticated, not secret ([S3], [S4]).
3. Blast radius shrinks only when no single secret outside the HSM opens every tenant: per-tenant (or per-record) data keys wrapped by a KMS key that never leaves the KMS, decrypted per use, or a KMS key per tenant ([V1], [V5], [V8]).
4. Vercel Connect documents only that refresh tokens and connector secrets are stored and encrypted on Vercel's infrastructure; it does not document its key hierarchy ([VC1]–[VC4]). Nango and the other stores are in the table below.
5. Recommendation: keep the design, drop the salt idea, alert on every Cloud KMS `Decrypt` of the router key outside a pod start, finish the KEK rotation (AI-3343), and move to KMS-wrapped per-tenant data keys only on a named trigger.

## Decision

Decided on 2026-11-23.

- Keep one KEK. Cloud KMS wraps it. The router unwraps it once, at start.
- Do not salt the key with `app_id`.
- Bind every sealed blob to its row with AAD. API key secrets: AI-3631. Channel and custom model secrets: AI-3632.
- Ship each binding in three releases, so a rollback to the release before always opens every row:
  1. Read both forms. Open with the row's AAD first, then without it, and count the opens without it. Write the old form.
  2. When every environment runs step 1: write the bound form, and seal an old row again when it is read.
  3. When every live row opens with its AAD: delete the fallback.
- Alert on every Cloud KMS `Decrypt` of the router key outside a pod start: AI-3633.
- KEK rotation stays in AI-3343.
- Add per-tenant data keys only when a trigger fires: a customer asks for CMEK or BYOK, a SEC or SOC 2 review requires them, or a real alert fires.

## Terms

| Term | Meaning here | Example |
| --- | --- | --- |
| KEK | Key encryption key. In our router it is the key that seals rows | `ROUTER_AUTH_KEK_V1_WRAPPED`, unwrapped at start |
| DEK | Data encryption key: the key that encrypts the data itself ([V1]) | One random AES key per connection row |
| KDK | Key-derivation key: the secret input of a KDF ([S2] §4) | The master key in `HKDF(master, app_id)` |
| AAD | Additional authenticated data: input that is authenticated, not encrypted ([S4] §2) | `accelerate:connector-material:v1:…:<customer>:…:<connection>:<revision>` |
| Envelope encryption | Data under a DEK, the DEK under a KEK ([V5] "Envelope encryption") | Tink and the AWS Encryption SDK make a new DEK per encryption ([V7], [V6]) |

## (b) What our router does today

| # | Fact | Evidence |
| --- | --- | --- |
| C1 | The `Sealer` turns each keyring entry into an AES-256-GCM key by SHA-256 of the configured string | `acceleration/internal/auth/secret.go:66-75@2ef00b1c6` |
| C2 | It already derives one separate key, the HMAC key for `Derive`, under the label `auth: derived credential key v1` | `secret.go:24-25`, `secret.go:76-78@2ef00b1c6` |
| C3 | Every seal draws a random 12-byte nonce and prepends it; every row of every tenant is sealed under the same key per version | `secret.go:114-121@2ef00b1c6` |
| C4 | Connector credentials are sealed with AAD of customer id, connection id and revision, length-prefixed | `acceleration/internal/connectors/credentialstores/pgsealed/pgsealed.go:204`, `:217-218`, `:239-242@2ef00b1c6` |
| C5 | A blob that fails AAD (another row, customer or revision) does not open, and the connection goes to `needs_reauthorization` | `pgsealed.go:70-81`, `:222-224@2ef00b1c6` |
| C6 | The same keyring also seals OAuth client secrets, authorization attempts, MCP event secrets, forwarded Stream tokens, provider keys, SIP and bridge passwords, channel and custom-model secrets | `grep -rn "SealWithAAD(" acceleration --include=*.go` at `2ef00b1c6`: `internal/api/oauth_clients.go:254`, `internal/api/authorizations.go:444`, `internal/mcpevents/mcpevents.go:245`, `internal/streamapp/forwarded.go:114`, `internal/providerkeys/providerkeys.go:68`, `internal/phone/lines.go:152`, `internal/api/channels.go:286`, `internal/api/custom_models.go:180` |
| C7 | Not every call site binds a record id: `channels.go:286` and `custom_models.go:180` bind only the customer id; `router keys create` seals API key secrets with no AAD | `internal/api/channels.go:286`, `internal/api/custom_models.go:180`, `cmd/router/keys.go:71@2ef00b1c6` |
| C8 | A hosted router reads each key as `ROUTER_AUTH_KEK_V<n>_WRAPPED` and unwraps it with one Cloud KMS `Decrypt` per version, AAD `router-kek-v<n>` | `acceleration/cmd/router/kms.go:23`, `:64-74`, `:83-98`; `cmd/router/main.go:476-493@2ef00b1c6` |
| C9 | The keys are unwrapped once, at start; the KMS client is then closed, so the plaintext KEK lives in router memory for the process lifetime | `cmd/router/main.go:449-455@2ef00b1c6` ("The keys are unwrapped once, here, so nothing needs the client after.") |
| C10 | A KMS that does not answer fails startup, not a request: KMS is a start-time dependency only | `cmd/router/kms.go:38-41@2ef00b1c6` |
| C11 | `ROUTER_DEV_KEK` is the one plain key, version 1, for laptops and CI, and it is refused beside a KMS setting | `cmd/router/main.go:150-152`, `:472-474@2ef00b1c6`; commit `c37de15ff` (#2068) |
| C12 | On hosted routers only the router's service account holds Decrypter on the KMS key | Linear AI-1541 (staging, comment of 2026-10-23) and AI-3332 (prod, Done) |

Prior discussion of the same risk:

- `acceleration/docs/connectors/competitor-analysis.md:1726@2ef00b1c6`: "A DB leak together with the KEK gives access to every connected customer account."
- `acceleration/docs/connectors/architecture.md:573@2ef00b1c6`: more secret kinds under the same KEK make "a leak of the database plus the key" worse; "KMS is a credential store behind the interface when the time comes".
- `architecture.md:429@2ef00b1c6`: the choice "sealed Postgres, KMS envelope, or a broker (Nango, Vercel Connect)" is deferred "After a month of Slack and Linear in staging, from data".
- Linear AI-1541 and AI-1748 (Done): the KEK moved behind Cloud KMS. AI-3332 (Done): prod too, plain key refused. AI-3343 (Backlog): rotate to a new KEK version and rewrap every row.
- No Slack message or Linear issue on "app_id as salt" was found (Slack search `salt` and `"master key"` after 2026-10-01; Linear search `KEK`). The question has no recorded decision.

## (a) What the standards say

| # | Claim | Source |
| --- | --- | --- |
| S1 | The HKDF salt is "a non-secret random value" and "can be re-used"; `info` binds keys to context such as "user identities", and must be "independent of the input keying material" | RFC 5869 §2.2, §3.1, §3.2 |
| S2 | A KDF's strength is measured "under the assumption that the key-derivation key, KIN, is the only unknown input to the KDF". Context binding "may not necessarily increase the security strength"; it detects protocol errors | NIST SP 800-108r1-upd1 §6.1, §6.6 |
| S3 | AAD is "authenticated but not encrypted" | NIST SP 800-38D §4.1 (Definitions) |
| S4 | Associated data "needs to be authenticated, but does not need to be kept confidential" | RFC 5116 §2.1 |
| S5 | With random IVs, "The total number of invocations of the authenticated encryption function shall not exceed 2^32 … with the given key" | NIST SP 800-38D §8.3 |
| S6 | Disclosure of an encryption key "means that all information encrypted by that key could be determined"; exposure is limited by "using different keys for different purposes … and limiting the amount of information protected by a single key" | NIST SP 800-57 Pt 1 Rev 5 §5.5, §5.5.1 |
| S7 | "Limiting the use of a key limits the damage that could be done if the key is compromised" | NIST SP 800-57 Pt 1 Rev 5 §5.2 |
| S8 | In a key hierarchy, child keys come from a parent KDK; only designated KDKs derive keys | NIST SP 800-108r1-upd1 §5 |

## (a) What the KMS vendors say

| # | Claim | Source |
| --- | --- | --- |
| V1 | "Generate a new DEK every time you write the data." "Do not use the same DEK to encrypt data from two different users." "The KEK never leaves Cloud KMS." "Do NOT store a plaintext DEK." | Google Cloud KMS, Envelope encryption |
| V2 | Cloud KMS AAD "is not stored as part of the ciphertext", "is not logged by" Cloud Audit Logs, and "does not increase the cryptographic strength of the ciphertext" | Google Cloud KMS, Additional authenticated data |
| V3 | Encrypt and Decrypt are `DATA_READ` methods and produce Data Access audit logs | Google Cloud KMS, Cloud KMS audit logging |
| V4 | IAM roles can be granted on "A key directly"; least privilege means "the lowest object in the resource hierarchy" | Google Cloud KMS, Access control with IAM |
| V5 | KMS keys "never leave the AWS KMS FIPS 140-3 Security Level 3 validated hardware security modules unencrypted. To use a KMS key, you must call AWS KMS." | AWS KMS, Cryptography essentials, "Envelope encryption" |
| V6 | "By default, the AWS Encryption SDK generates a new data key for every encryption operation." Caching is "an optional feature … that you should use cautiously" | AWS Encryption SDK, Data key caching |
| V7 | Tink "creates a new AEAD DEK, encrypts it with a cloud KMS KEK, and stores it as part of the ciphertext" | Google Tink, Client-side encryption |
| V8 | AWS encryption context is bound as AAD, "appears in plaintext in AWS CloudTrail Logs", and can gate `kms:Decrypt` through `kms:EncryptionContext:` condition keys | AWS KMS, Encryption context ("Encryption context in policies", "Logging encryption context") |
| V9 | The Hierarchical keyring keeps KMS-protected branch keys, caches them locally for a set time, and derives "a unique wrapping key" per message from the branch key and "a 16-byte random salt"; AWS "strongly recommend[s] using a branch key ID supplier for multitenant databases where each tenant has their own branch key" | AWS Encryption SDK, AWS KMS Hierarchical keyrings |
| V10 | Vault transit derives keys "based on a user-supplied context value"; with `derived`, every encrypt and decrypt "must provide a context"; "Vault doesn't store the data sent to the secrets engine" | HashiCorp Vault, Transit secrets engine; Transit API |
| V11 | Cloud KMS pricing "is based on the number of active key versions … and a usage rate for key operations"; "A cryptographic operation is any form of encryption, decryption, signing, or generating random bytes" | Google Cloud KMS pricing |
| V12 | Cloud KMS has a "Cryptographic requests" quota on the calling project, which counts encrypt and decrypt; a request over it fails with `RESOURCE_EXHAUSTED` (HTTP 429) | Google Cloud KMS, Quotas |

## Does "app_id as salt" reduce the blast radius?

```
today                                  with app_id salt
-----                                  ----------------
KEK ──AES-GCM──▶ every tenant's row    KEK ──HKDF(salt=app_id)──▶ K_app ──AES-GCM──▶ that app's rows
        AAD = customer|connection|rev           AAD = customer|connection|rev (kept)
```

- **If the KEK leaks, nothing changes.** `app_id` is in every row (`customer_id`), so the attacker who has the KEK computes each `K_app` themselves. RFC 5869 calls the salt non-secret [S1], and SP 800-108r1 rates a KDF only with the KDK as "the only unknown input" [S2]. One secret still opens every tenant.
- **If a DB dump leaks alone, nothing changes.** Today's KEK is not in the database already ([C8], [C9]).
- **If a blob is swapped between tenants, nothing changes.** Our AAD already makes that blob fail to open ([C4], [C5]). A per-app key would also fail it, so the two are equal here. Context binding "may not necessarily increase the security strength" [S2].
- **What it does give** is hygiene, not isolation:
  - It meets the letter of Google's "Do not use the same DEK to encrypt data from two different users" [V1]. Today our KEK is, in Google's terms, the DEK for all users ([C3]).
  - It splits the AES-GCM invocation budget of 2^32 random-nonce seals per key [S5] across apps. Our seal count per key is `unverified`: not measured.
  - It does not separate "different purposes" ([S6], [S7]): connector tokens, provider keys and SIP passwords of one app would still share `K_app` unless the purpose also went into `info` ([C6]).
- **A secret salt** would help (RFC 5869 §3.1 notes "an even stronger security guarantee"), but then the salt is a per-tenant key. Where it lives decides everything: in the DB it leaks with the DB; wrapped by KMS it is a per-tenant DEK, which is the next section.

## What does limit the blast radius

Threat model. "Stops" means the attacker gets no tenant's plaintext credentials.

| Attacker | A. Today: KMS-wrapped KEK, unwrapped once; AAD | B. A + HKDF(KEK, app_id) | C. Random DEK per tenant or row, wrapped by the in-memory KEK | D. DEK per tenant, wrapped by Cloud KMS, `Decrypt` per use (short cache) | E. One Cloud KMS key per tenant |
| --- | --- | --- | --- | --- | --- |
| 1. DB dump only | Stops ([C8]) | Stops | Stops | Stops | Stops |
| 2. DB dump + the plaintext KEK (memory dump, old backup of the key) | All tenants ([S6]) | All tenants ([S1], [S2]) | All tenants: wrapped DEKs open with the KEK | No KEK exists outside KMS ([V1], [V5]); only DEKs cached at that moment leak | Same as D |
| 3. Code execution in a router process | All tenants ([C9]) | All tenants | All tenants | Partial: can call `Decrypt` as the router for every tenant, but only while inside, each call is logged ([V3]) and quota-bound ([V12]) | Same as D, and each log names the tenant's key |
| 4. Insider with Decrypter on the router's KMS key + DB read | All tenants with one `Decrypt` ([C8]) | All tenants | All tenants | One logged `Decrypt` per tenant ([V3]); GCP logs do not show our AAD ([V2]), so the log shows the key, not the tenant | Per-tenant IAM on each key ([V4]); logs name the tenant. The router's account still needs all keys |
| 5. Cross-tenant mix-up or blob swap bug | Stops for connector credentials ([C4], [C5]); weaker at the [C7] call sites | Stops | Stops if AAD is kept | Stops if AAD is kept | Stops |
| Offboard one tenant by destroying its key | No | No: keys re-derive from the KEK | Only if no backup holds the wrapped DEK | Yes, if its DEK is wrapped under a per-tenant KMS key version; else as C | Yes (destroy the key version) |

Costs, qualitative:

| Option | Code and data | Runtime | Money |
| --- | --- | --- | --- |
| B | A derivation and a per-app AEAD cache in `Sealer`; every sealed row rewrapped | None measurable (local HMAC) | None |
| C | A wrapped-DEK column, rewrap of every row | None measurable (local AES) | None |
| D | As C, plus a KMS call on every cache miss | KMS becomes a request-path dependency, today only a start-time one ([C10]); latency on the voice tool path is `unverified` (not measured) | Billed per operation ([V11]); bounded by quota ([V12]); the AWS SDKs add caches for exactly this reason ([V6], [V9]) |
| E | Key creation at app onboarding, a key name per app | As D | Billed per active key version and per operation ([V11]) |

The shape that vendors document for many tenants is D with a cache: per-tenant branch keys under one KMS key, a bounded local cache, and a fresh wrapping key per message ([V9]). Vault transit is a different shape: the key never reaches the application, and the application sends the data to Vault ([V10]).

## How other token stores do it

Code refs are `file:line@SHA` in each project's public repository, read on 2026-11-23. Closed-source stores are docs only.

| Store | Algorithm | Key hierarchy | Per-tenant key | Record binding (AAD) | Rotation | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| **Our router** | AES-256-GCM, random 12-byte nonce | One KEK per version, wrapped by Cloud KMS, unwrapped once at start | No | Yes: customer, connection, revision | Keyring versions, lazy rewrap | [C1]–[C9] |
| **Vercel Connect** | Not documented. Platform-wide: "Vercel encrypts data at rest (when on disk) with 256 bit Advanced Encryption Standard (AES-256)" | Not documented | Not documented | Not documented | Not documented | [VC1]–[VC4] |
| **Nango** | AES-256-GCM, random 12-byte IV; IV and tag in their own columns | One global DEK, plain `NANGO_ENCRYPTION_KEY` or `NANGO_ENCRYPTION_KEY_WRAPPED` unwrapped by AWS KMS, GCP KMS or Azure Key Vault | No | No per-record AAD; a KMS context only on the DEK wrap | "Rotation of NANGO_ENCRYPTION_KEY is not supported." | [N1] |
| **Infisical** | AES-256-GCM | Root key (env or HSM) → KMS key → a data key per org and per project; data keys can sit behind AWS or GCP KMS | Yes | No `setAAD` in the cipher | KMS keys rotate; staged root-key rotation | [I1] |
| **Supabase Vault** | Deterministic XChaCha20 AEAD (libsodium) | Root key loaded from outside the database; one subkey derived with `key_id := 0` | One root key per project; secrets in a project share the subkey | Yes: the secret's row id | `unverified`: not found in code or docs | [SV1] |
| **Composio** | AES-256-GCM (docs) | Docs: none. Customer-managed keys on AWS KMS, Google Cloud KMS or Vault Transit | `unverified`: a per-organization key appears only in a Composio blog post, not in its docs | `unverified` | `unverified` | [CO1] |
| **Pipedream** | AES-256-GCM (KMS keys) | "The key used to encrypt this database is managed by AWS KMS and controlled by Pipedream." | `unverified` | `unverified` | "Keys are automatically rotated once a year." | [PD1] |

What Vercel Connect does document:

- "For OAuth-based connectors, Vercel Connect drives the full authorization-code flow including refresh, and stores the refresh token on Vercel's infrastructure. Your code never sees the refresh token directly" ([VC1], section on OAuth-based connectors).
- Deleting a connector "Keeps the connector's encrypted configuration for 40 days. This includes client secrets and private keys." ([VC2], "Deleting a connector").
- Every connector has an Observability tab with a **Token Request** event per `getToken` call ([VC4]).
- How those tokens are keyed (one key, per team, per connector, KMS or not) is not in Vercel's docs. Searched: the Connect pages listed in Sources, `security/compliance`, and the Vercel docs search tool for "Vercel Connect connector tokens storage encryption security". Treat any claim about it as `unverified`.

Reading of the table:

- Nango, the open-source store closest to ours, has the same hierarchy as our router: one global DEK behind a KMS, unwrapped into the process ([N1]). It binds no record AAD; we do ([C4]).
- Infisical is the only open-source store here with a key per tenant, and those keys are random, not derived from the root ([I1]).
- Supabase Vault derives its subkey from a root key with libsodium's KDF ([SV1]): derivation from a root, as with the salt idea, so a root-key leak opens every secret of the project.
- No store documents a per-tenant key derived from a public tenant id as an isolation measure.

## Recommendation for the router

Each item names the evidence it follows from.

| Item | Do | Why (evidence) | YAGNI: risk now, cost, simpler variant, trigger |
| --- | --- | --- | --- |
| KMS-wrapped KEK, keyring versions, lazy rewrap | **Keep** | Stops attacker 1 ([C8]) | Nothing to add |
| AAD with customer, connection, revision | **Keep** | Stops attacker 5 ([C4], [C5], [S3]) | Nothing to add |
| `app_id` as HKDF salt or info | **Do not build** as a blast-radius measure | Changes no row of the threat table ([S1], [S2]); swap protection exists ([C4]) | Hygiene only ([V1], [S5]). Trigger: we rewrap every row anyway (C or D); then put app and purpose into `info` in the same change, at no extra rewrap |
| Alert on Cloud KMS `Decrypt` of the router key | **Add** (AI-3633) | Legit `Decrypt` happens only at pod start ([C9]); any other `Decrypt` is attacker 4 or a stolen wrapped key. Decrypt produces Data Access logs ([V3]) | Cost: a log-based alert, no code. Simpler variant: a weekly manual audit-log check |
| Rotate the KEK and rewrap every row (AI-3343) | **Do** (already planned) | Once every row is rewrapped, the earlier key versions can be retired ([S6] §5.5: a compromised key "shall be revoked") | Gated as a data move (AI-3343) |
| Record-id AAD at the [C7] call sites | **Fix in place** when touched | Same reason as [C4]; out of connector scope | Small |
| Per-tenant DEK wrapped by Cloud KMS, `Decrypt` on cache miss (D) | **Defer** | The only option that changes rows 2–4 ([V1], [V5], [V9]) | Cost: request-path KMS dependency, per-operation billing, a rewrap. Trigger: a customer asks for tenant-level key isolation or crypto-shredding, a compliance review requires it, or attacker 2 becomes realistic (for example the KEK must reach a host we do not run) |
| One KMS key per tenant (E) | **Defer** | Adds per-tenant IAM and tenant-named audit ([V4], [V3]) | Trigger: a customer-managed key (CMEK/BYOK) request |
| A broker (Vercel Connect, Nango Cloud) | **Out of scope here** | Moves the store to a vendor that does not document its key hierarchy ([VC1]–[VC4]) | Decision stays where `architecture.md:429` put it |

## Claims marked unverified

- How many seals each KEK version has made, against the 2^32 budget of [S5]: not measured.
- The latency a Cloud KMS `Decrypt` adds on the voice tool path for option D: not measured.
- Vercel Connect's key hierarchy, KMS use and tenant isolation of stored refresh tokens: not in Vercel's docs (see Vercel Connect row).
- Composio's per-organization key (a vendor blog post only, not in its docs), and AAD, derivation and per-tenant keys at Composio and Pipedream.
- Supabase Vault key rotation, and the hash inside libsodium's `crypto_kdf_derive_from_key` (libsodium source not read).
- The live Nango docs pages: the repository's `docs/` was read; `docs.nango.dev` redirected and the fetch timed out.
- Whether Cloud KMS IAM Conditions can gate `Decrypt` on AAD the way AWS encryption context can ([V8]): the Cloud KMS IAM page does not mention IAM Conditions ([V4]); not checked elsewhere.

## Sources

Standards

- [S1] RFC 5869, HKDF, §2.2, §3.1, §3.2: https://www.rfc-editor.org/rfc/rfc5869
- [S2] [S8] NIST SP 800-108r1-upd1, Key Derivation Using Pseudorandom Functions, §4, §5, §6.1, §6.6: https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-108r1-upd1.pdf
- [S3] [S5] NIST SP 800-38D, GCM, §4.1, §8.3: https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf
- [S4] RFC 5116, AEAD, §2.1: https://www.rfc-editor.org/rfc/rfc5116
- [S6] [S7] NIST SP 800-57 Part 1 Rev 5, §5.2, §5.3, §5.5, §5.5.1: https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-57pt1r5.pdf

Vendors

- [V1] https://docs.cloud.google.com/kms/docs/envelope-encryption
- [V2] https://docs.cloud.google.com/kms/docs/additional-authenticated-data
- [V3] https://docs.cloud.google.com/kms/docs/audit-logging
- [V4] https://docs.cloud.google.com/kms/docs/iam
- [V5] https://docs.aws.amazon.com/kms/latest/developerguide/kms-cryptography.html#enveloping
- [V6] https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/data-key-caching.html
- [V7] https://developers.google.com/tink/client-side-encryption
- [V8] https://docs.aws.amazon.com/kms/latest/developerguide/encrypt_context.html
- [V9] https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/use-hierarchical-keyring.html
- [V10] https://developer.hashicorp.com/vault/docs/secrets/transit and https://developer.hashicorp.com/vault/api-docs/secret/transit
- [V11] https://cloud.google.com/kms/pricing
- [V12] https://docs.cloud.google.com/kms/quotas

Vercel Connect

- [VC1] https://vercel.com/docs/connect/concepts/authentication
- [VC2] https://vercel.com/docs/connect/concepts/connectors ("Deleting a connector")
- [VC3] https://vercel.com/docs/security/compliance#data-encryption
- [VC4] https://vercel.com/docs/connect/observability and https://vercel.com/docs/connect/concepts/tokens

Token stores

- [N1] Nango, https://github.com/NangoHQ/nango at `970f9e6715806ff96e7096b12afc86dce22827f6`: `packages/utils/lib/encryption.ts:18` (`aes-256-gcm`), `:54` (`crypto.randomBytes(12)`); `packages/shared/lib/utils/encryption.manager.ts:46-51` (IV and tag columns), `:213-214` (rotation throws), `:341` (one `EncryptionManager`); `packages/kms/lib/registry.ts:16-26` (`GLOBAL_DEK_CONTEXT`, key env vars); `docs/guides/platform/security.mdx:41` ("Nango uses envelope encryption. A 256-bit data encryption key (DEK) … is itself stored only in wrapped form, encrypted under a key held in AWS KMS."); `docs/guides/platform/self-hosting/self-managed.mdx:224` ("Key rotation is not supported yet"). `grep -rn setAAD packages` finds nothing.
- [I1] Infisical, https://github.com/Infisical/infisical at `bb9820f83a0cec8c39e953f1c312081d49a5246b`: `backend/src/lib/crypto/cipher/cipher.ts:5-27`; `backend-go/internal/services/kms/kms.go:87-91` ("ROOT_ENCRYPTION_KEY (in memory) → decrypts KMS key material → decrypts data key (org/project encrypted data key)"); `backend/src/domains/platform/kms-core/ce/models/data-key.ts:24`; `…/kms-key.ts:304`, `:325`; `docs/internals/security.mdx:173` ("Each organization and each project has its own dedicated data key, providing cryptographic isolation between tenants."); https://infisical.com/docs/internals/security
- [SV1] Supabase Vault, https://github.com/supabase/vault at `edd9ef9fb276ad733460c4e45c10bbe1e06c678c`: `supabase_vault.sql:52-57` (`additional := … s.id …, key_id := 0, context := 'pgsodium'`); `src/pgsodium.h:94-96` (`crypto_kdf_derive_from_key`); `src/supabase_vault.c:41-42` (root key from `vault.getkey_script`); https://supabase.com/docs/guides/database/vault ("the encryption key is never stored in the database alongside the encrypted data"; "Each Supabase project has its own root encryption key")
- [CO1] Composio, https://docs.composio.dev/docs/security/overview ("Credential protection": "Provider credentials are encrypted at rest using AES-256-GCM") and https://docs.composio.dev/docs/security/token-custody ("Deployment options": "The customer-managed key architecture supports AWS KMS, Google Cloud KMS, and HashiCorp Vault Transit")
- [PD1] Pipedream, https://pipedream.com/docs/privacy-and-security/ ("Third party OAuth grants, API keys, and environment variables")

Code and tickets (this repo at `2ef00b1c6`)

- [C1]–[C3] `acceleration/internal/auth/secret.go`
- [C4], [C5] `acceleration/internal/connectors/credentialstores/pgsealed/pgsealed.go`
- [C6], [C7] the call sites listed in the table
- [C8]–[C11] `acceleration/cmd/router/kms.go`, `acceleration/cmd/router/main.go`, commits `fe655d0d6` (#1157), `93fe0d4da` (#1165), `c37de15ff` (#2068)
- [C12] Linear AI-1541, AI-1748, AI-3332, AI-3343
- Prior design notes: `acceleration/docs/connectors/architecture.md:429`, `:573`; `acceleration/docs/connectors/competitor-analysis.md:1726`
