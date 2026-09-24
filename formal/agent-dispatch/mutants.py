"""Mutant ledger. Each entry re-introduces one defect the GH-4537 review rounds found, so a
green model can be shown to be green for a reason. Literal replaces on a pristine copy, with
the replacement verified before the checker runs."""
import io, shutil, subprocess, sys

PRISTINE = "/tmp/claude-1000/-home-michael-GitHub-wolverine/b1e47487-3425-43c1-b9c1-ebc48a87cf70/scratchpad/pristine.p"
TARGET = "AgentDispatchModel.p"

MUTANTS = {
 "m1-no-putback": [("""    if (!ok) {
      send disp, ePutBack, (dest = dest, displaced = displaced, op = this);
      receive { case ePutBackR: { } }
      return;
    }""", """    if (!ok) {
      return;
    }""")],

 "m2-claim-after-entry": [("""    send disp, eTakeClaim, (dest = dest, op = this);
    receive { case eTakeClaimR: (r: (displaced: int)) { displaced = r.displaced; } }

    send disp, eNewTicket, (op = this,);""", """    send disp, eNewTicket, (op = this,);"""),
   ("""    if (!ok) {
      send disp, ePutBack, (dest = dest, displaced = displaced, op = this);
      receive { case ePutBackR: { } }
      return;
    }""", """    if (!ok) {
      return;
    }

    send disp, eTakeClaim, (dest = dest, op = this);
    receive { case eTakeClaimR: (r: (displaced: int)) { displaced = r.displaced; } }""")],

 "m3-scan-by-destination": [("      if (dLane[queued[d]] == lane) {", "      if (d == node) {")],

 "m4-release-without-ticket": [("""    if (!(dest in queued) || queued[dest] != ticket) {
      return;
    }""", """    if (!(dest in queued)) {
      return;
    }
    ticket = queued[dest];""")],

 "m5-no-admission-check": [("      send p.op, eAdmitR, (ok = !(p.lane in abandoned && abandoned[p.lane]),);",
                            "      send p.op, eAdmitR, (ok = true,);")],
}

CASES = ["tcLockFreeSteady", "tcLockFreeDeparture", "tcLockFreeStale"]

def run(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout

results = {}
for name, edits in MUTANTS.items():
    src = io.open(PRISTINE, encoding="utf-8").read()
    for old, new in edits:
        assert src.count(old) == 1, f"{name}: pattern not unique/found ({src.count(old)})"
        src = src.replace(old, new)
    io.open(TARGET, "w", encoding="utf-8").write(src)

    out = run("p compile --pfiles AgentDispatchModel.p AgentDispatchSpec.p AgentDispatchTest.p "
              "--projname AgentDispatch --outdir . 2>&1")
    if "Build succeeded" not in out:
        results[name] = "COMPILE FAILED"
        continue

    caught = []
    for tc in CASES:
        out = run(f"p check -tc {tc} -s 30000 2>&1")
        bugs = "Found 0 bugs" not in out
        why = ""
        if bugs:
            err = run("grep -hoE '(Assertion Failed|Deadlock detected).*' "
                      "PCheckerOutput/BugFinding/AgentDispatch_0_0.txt | head -1").strip()
            why = err.split("settled")[-1][:70] if "settled" in err else err[:70]
        caught.append(f"{tc}={'CAUGHT' if bugs else 'missed'}{(' :: ' + why) if why else ''}")
    results[name] = " | ".join(caught)

shutil.copyfile(PRISTINE, TARGET)
for k, v in results.items():
    print(f"{k:28} {v}")
