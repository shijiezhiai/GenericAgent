# ══════════════════════════════════════════════════════════════════════════════
#  GenericAgent — mykey.py configuration template (copy to mykey.py and fill in)
# ══════════════════════════════════════════════════════════════════════════════
#
#  Quick start:
#    1. Copy this file to mykey.py
#    2. Uncomment one of the configs below and fill in your apikey
#    3. Run `python agentmain.py` or `python launch.pyw`
#
#  GA auto-detects any variable whose name contains 'api'/'config'/'cookie'
#  and picks the session class by keyword:
#      name contains 'native' + 'claude'  → NativeClaudeSession  (Anthropic API)
#      name contains 'native' + 'oai'     → NativeOAISession     (OpenAI API)
#      name contains 'mixin'              → MixinSession         (failover)
#
#  Native = tools go in the API's native `tool` field (function calling), same
#  way Claude Code and Codex do it. Recommended for GPT / Claude / Gemini.
#
#  Tip: runtime overrides via `/session.<attr>=<val>` in the REPL, e.g.
#      /session.reasoning_effort=high
#      /session.thinking_type=adaptive
#      /session.thinking_display=brief     # full(show all)/brief(truncate)/off(hide) thinking
#      /session.thinking_display_chars=400 # chars of thinking kept in brief mode
#      /session.temperature=0.3
#
# ══════════════════════════════════════════════════════════════════════════════


# ── 1. NativeClaudeSession — Anthropic direct ────────────────────────────────
#  Official Anthropic endpoint. apikey starting with 'sk-ant-' is auto-sent
#  as x-api-key; any other prefix uses Authorization: Bearer.
#  Model suffix '[1m]' triggers the 1M-context beta (stripped before sending).
native_claude_config = {
    'name': 'claude',                         # display name & mixin reference
    'apikey': 'sk-ant-<your-anthropic-key>',
    'apibase': 'https://api.anthropic.com',
    'model': 'claude-opus-4-7[1m]',           # or 'claude-sonnet-4-6'
    'thinking_type': 'adaptive',              # 'adaptive' | 'enabled' | 'disabled'
    # 'thinking_budget_tokens': 32768,        # required if thinking_type='enabled'
    # 'max_retries': 3,
    # 'read_timeout': 180,
}


# ── 2. NativeOAISession — OpenAI direct ──────────────────────────────────────
#  Standard OpenAI chat/completions endpoint. Also works for any OpenAI-
#  compatible provider that supports native function-calling tool fields.
native_oai_config = {
    'name': 'gpt',                            # display name & mixin reference
    'apikey': 'sk-<your-openai-key>',
    'apibase': 'https://api.openai.com/v1',
    'model': 'gpt-5.4',                       # or 'o4', 'gpt-5.3-codex', etc.
    'api_mode': 'chat_completions',           # or 'responses' for /v1/responses
    # 'reasoning_effort': 'high',             # none|minimal|low|medium|high|xhigh
    # 'max_retries': 3,
    # 'read_timeout': 120,
}


# ── 3. Mixin failover (optional) ─────────────────────────────────────────────
#  List sessions by 'name'; if one fails, the next is tried automatically.
#  Constraint: all referenced sessions must be Native (mixing Native Claude
#  and Native OAI is fine; mixing Native with non-Native is not).
# mixin_config = {
#     'llm_nos': ['claude', 'gpt'],
#     'max_retries': 5,
#     'base_delay': 0.5,
# }


# ── 3.5 Global defaults (optional, applied to every backend session) ────────
#  Fields here act as fallback defaults for all sessions. Precedence:
#    backend's own field > GLOBAL_DEFAULT > built-in default ('full').
#  Runtime override via /session.<attr>=<val> wins over everything.
#  Supported fields:
#    thinking_display        default thinking output mode: full / brief / off
#    thinking_display_chars  chars kept in 'brief' mode (default 400)
GLOBAL_DEFAULT = {
    'thinking_display': 'brief',       # ← global default thinking output mode
    'thinking_display_chars': 400,
}


# ── 4. Global HTTP proxy (optional) ──────────────────────────────────────────
#  Applies to every session that doesn't set its own 'proxy' field.
# proxy = 'http://127.0.0.1:7890'


# ── 5. Chat platform integrations (optional) ─────────────────────────────────
# tg_bot_token = '...'
# tg_allowed_users = [123456789]


# ── 6. Git platform credentials (optional; used by git_checkpoint action=pr) ──
#  Without these, the tool still works: it tries `gh pr create` first, and falls back
#  to a comparison-URL hint if neither gh nor a token is available. Configure here
#  to enable automatic PR creation via GitHub/GitLab REST API. Minimum scopes:
#    github_token: repo
#    gitlab_token: api
#  Alternatively set the env var $GITHUB_TOKEN / $GITLAB_TOKEN (takes precedence).
#
# github_token = 'ghp_xxxxxxxxxxxxxxxxxxxx'
# gitlab_token = 'glpat-xxxxxxxxxxxxxxxxxxxx'


# ── 7. Embedding backend (optional; enables semantic_search vector mode) ─────
#  Without these, semantic_search auto-falls back to keyword+synonym ripgrep.
#  With them, it does true vector retrieval via any OpenAI-compatible
#  /v1/embeddings endpoint. If absent but a native_oai_* config exists, its
#  credentials are reused with text-embedding-3-small.
#
# embedding_apikey = 'sk-...'
# embedding_apibase = 'https://api.openai.com/v1'
# embedding_model = 'text-embedding-3-small'
