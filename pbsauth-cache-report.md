# PBSAuth Cache Implementation Report

## Summary

Implemented a cached singleton for `PBSAuth` to avoid parsing the TLS key on every proxied API request.

## Changes Made (internal/web/auth.go)

1. **Added `sync` import** - Required for `sync.Once`

2. **Added package-level cached variables**:

   ```go
   var (
       cachedPBSAuth *PBSAuth
       pbsAuthOnce   sync.Once
       pbsAuthErr    error
   )
   ```

3. **Created `GetPBSAuth()` function** - Uses `sync.Once` to lazily initialize and return the cached instance. If initialization fails at startup, logs the error and returns the error on subsequent calls.

4. **Updated `checkProxyAuth()`** - Now calls `GetPBSAuth()` instead of `NewPBSAuth()` to use the cached instance.

5. **Kept `NewPBSAuth()` exported** - No external callers exist, but it's kept exported for potential external use.

## Verification

- `gofmt -e` passes - no syntax errors in auth.go
- `go build ./internal/web/...` and `go vet ./internal/web/...` fail due to **pre-existing issues** in `middleware.go` (undefined: `AgentStore`, `syslog`), not related to these changes
- The auth.go changes are syntactically correct and follow the singleton pattern correctly

## Behavior

- The TLS key file is now read and parsed only once at the first call to `GetPBSAuth()`, not on every request
- If the key file is missing/corrupted at startup, the error is logged and subsequent auth checks will fail with a clear error message
- `VerifyTicket` and `splitPBS` remain unchanged as required
