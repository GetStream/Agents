// Package redisscript runs a Lua script once or many times in one round trip.
//
// rueidis' own Lua.ExecMulti sends SCRIPT LOAD to every node before each batch, so a batch is
// never fewer than two round trips. A Script sends the batch as EVALSHA, and EVAL only for the
// calls Redis answered NOSCRIPT, which it does only after a restart or a flush.
package redisscript

import (
	"context"
	"crypto/sha1" //nolint:gosec // EVALSHA names a script by its SHA-1
	"encoding/hex"

	"github.com/redis/rueidis"
)

// Script is one Lua script.
type Script struct {
	lua    *rueidis.Lua
	source string
	sha1   string
}

// New returns the script with source.
func New(source string) *Script {
	sum := sha1.Sum([]byte(source))
	return &Script{lua: rueidis.NewLuaScript(source), source: source, sha1: hex.EncodeToString(sum[:])}
}

// Exec runs the script once.
func (s *Script) Exec(ctx context.Context, client rueidis.Client, keys, args []string) rueidis.RedisResult {
	return s.lua.Exec(ctx, client, keys, args)
}

// ExecMulti runs the script once per call, in one round trip, and a second only for the calls
// Redis did not have the script for. The answers are in the order of the calls.
func (s *Script) ExecMulti(ctx context.Context, client rueidis.Client, calls ...rueidis.LuaExec) []rueidis.RedisResult {
	if len(calls) == 0 {
		return nil
	}
	commands := make(rueidis.Commands, len(calls))
	for i, call := range calls {
		commands[i] = client.B().Evalsha().Sha1(s.sha1).Numkeys(int64(len(call.Keys))).Key(call.Keys...).Arg(call.Args...).Build()
	}
	results := client.DoMulti(ctx, commands...)
	var missing []int
	for i, result := range results {
		if redisErr, ok := rueidis.IsRedisErr(result.Error()); ok && redisErr.IsNoScript() {
			missing = append(missing, i)
		}
	}
	if len(missing) == 0 {
		return results
	}
	commands = make(rueidis.Commands, len(missing))
	for j, i := range missing {
		commands[j] = client.B().Eval().Script(s.source).Numkeys(int64(len(calls[i].Keys))).Key(calls[i].Keys...).Arg(calls[i].Args...).Build()
	}
	for j, result := range client.DoMulti(ctx, commands...) {
		results[missing[j]] = result
	}
	return results
}
