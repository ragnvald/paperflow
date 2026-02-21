This file guides work in this paperless workspace.

Read `learning.md` before making operational decisions.

Update `learning.md` whenever a session produces reusable knowledge, preferably in a generic form.

Preferred note structure:
- Organize by **object of concern** (for example: software, service, path, host, physical place).
- Add short action/fact lines under each object.

Use object/verb style when natural, for example:
- **Install**: package X
- **Store**: data in path Y

If strict verb/object phrasing is awkward, use a simple format:
- **Location**: ...
- **Purpose**: ...
- **Constraint**: ...
- **Command**: ...

Keep entries concise, factual, and easy to reuse.

Execution rules:
- Follow user directives exactly.
- At decision points, ask for clarification instead of assuming.
- Before restructuring a current system, ask first.
- In restore scenarios, do not assume DB/backend migration is desired.
- Treat current architecture as intentional unless the user explicitly asks to change it.
- Execute-first policy: when the user asks for an operational run (for example: "run 120 samples now"), perform that run first before adding new features or refactoring.
- Scope discipline: do not convert an immediate run request into parameter/API design work unless the user explicitly asks for script improvements first.
- Minimal-change rule: during active troubleshooting, prefer the smallest change needed to complete the requested run; postpone enhancements until after results are delivered.
- Report-first workflow: after the run completes, report results and paths; only then propose optional improvements.
- If a requested run is blocked (permissions, missing dependency, etc.), stop and report the blocker immediately with the exact command that failed.

Large transfer rules:
- Before big copy/sync operations, estimate source size and compare with free destination space.
- If space is tight, prefer `move` over `copy` when a duplicate is not required.
- Avoid creating full duplicate media trees on the same filesystem.
