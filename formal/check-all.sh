#!/usr/bin/env bash
# Runs every case of leader-election and agent-assignment, then the mutant ledger, then bounded PEx runs,
# strictly one checker at a time. Results land in formal/PCheckerOutput/suite/suite.log (gitignored).
# Expect ~45 minutes of random runs before the PEx section; run it from the nix shell (nix develop).
# Strictly sequential: never two p check processes at once (see formal-specs skill).
set +e
ROOT=$(cd "$(dirname "$0")/.." && pwd)
S=$ROOT/formal/PCheckerOutput/suite
OUT=$S/out; mkdir -p $OUT
LOG=$S/suite.log; : > $LOG
P="nix develop $ROOT -c p"

run() { # dir proj tc schedules label
  local dir=$1 proj=$2 tc=$3 n=$4 label=$5
  local o=$OUT/$label-$tc
  ( cd $ROOT/formal/$dir && $P check -tc $tc -s $n -m 6 -o $o ) > $o.raw 2>&1
  local bugs=$(grep -E "^bugs:" $o/BugFinding/${proj}_pchecker_summary.txt 2>/dev/null | cut -d: -f2)
  local sched=$(grep -E "^schedules:" $o/BugFinding/${proj}_pchecker_summary.txt 2>/dev/null | cut -d: -f2)
  local secs=$(grep -E "^time_seconds:" $o/BugFinding/${proj}_pchecker_summary.txt 2>/dev/null | cut -d: -f2)
  local pct=$(grep -oE "Found [0-9.]+% buggy" $o.raw | head -1)
  local err=$(grep -h -oE "<ErrorLog>.*" $o/BugFinding/${proj}_0_0.txt 2>/dev/null | head -1 | cut -c1-200)
  echo "$label | $tc | bugs=$bugs | schedules=$sched | ${secs}s | $pct | $err" >> $LOG
}

compile() { # dir proj files...
  local dir=$1 proj=$2; shift 2
  ( cd $ROOT/formal/$dir && $P compile --pfiles "$@" --projname $proj --outdir . ) > $OUT/compile-$proj.raw 2>&1
  grep -q "Compilation succeeded" $OUT/compile-$proj.raw || { echo "COMPILE FAILED $proj" >> $LOG; return 1; }
}

mutate() { # file old new  (python literal replace, asserts applied once)
  python3 - "$1" "$2" "$3" <<'PY'
import sys, pathlib
f, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
p = pathlib.Path(f); s = p.read_text()
assert s.count(old) == 1, ("mutant anchor not unique/found", s.count(old))
p.write_text(s.replace(old, new))
PY
}

LE=$ROOT/formal/leader-election; AO=$ROOT/formal/agent-assignment
LEF="LeaderElectionModel.p LeaderElectionSpec.p LeaderElectionTest.p"
AOF="AgentOwnershipModel.p AgentOwnershipSpec.p AgentOwnershipTest.p"

echo "### leader-election pristine (random)" >> $LOG
compile leader-election LeaderElection $LEF
for tc in tcQuietCluster tcCrash tcGracefulStop tcLockSessionDrop tcHeartbeatBlip tcChaos tcIneligibleNode tcEligibilityRevoked; do
  run leader-election LeaderElection $tc 20000 le
done

echo "### agent-assignment pristine (random)" >> $LOG
compile agent-assignment AgentOwnership $AOF
for tc in tcSteadyState tcCrash tcPartitionHealStock tcPartitionHealSweep tcLeaderHandoverStock tcLeaderHandoverSweep tcChaosSweep tcLeaderHandoverHold tcSteadyStateShipped tcCrashShipped tcPartitionHealShipped tcLeaderHandoverShipped tcChaosShipped tcPartitionHealEpoch tcLeaderHandoverEpoch tcChaosEpoch; do
  run agent-assignment AgentOwnership $tc 20000 ao
done

echo "### leader-election mutants" >> $LOG
cp $LE/LeaderElectionModel.p $S/LE.pristine.p
# m1: the CanLead gate removed -- an ineligible node contends like any other
cp $S/LE.pristine.p $LE/LeaderElectionModel.p
mutate $LE/LeaderElectionModel.p $'      if (!canLead) {\n        if (isLeader) {' $'      if (false) {\n        if (isLeader) {'
compile leader-election LeaderElection $LEF && run leader-election LeaderElection tcIneligibleNode 20000 le-m1-no-gate
# m2: an ineligible node keeps a lease it already holds (no step-down on revocation)
cp $S/LE.pristine.p $LE/LeaderElectionModel.p
mutate $LE/LeaderElectionModel.p $'      if (!canLead) {\n        if (isLeader) {\n          stepDownLocally();\n        }\n        goto Ticking;\n      }' $'      if (!canLead) {\n        goto Ticking;\n      }'
compile leader-election LeaderElection $LEF && run leader-election LeaderElection tcEligibilityRevoked 20000 le-m2-no-stepdown
# m3: step-down keeps the advisory lock
cp $S/LE.pristine.p $LE/LeaderElectionModel.p
mutate $LE/LeaderElectionModel.p $'      if (lockHolder == m.id) {\n        lockHolder = 0;\n        refills = refillAll();\n      }' $'      if (false) {\n        lockHolder = 0;\n        refills = refillAll();\n      }'
compile leader-election LeaderElection $LEF && run leader-election LeaderElection tcEligibilityRevoked 20000 le-m3-stepdown-keeps-lock
cp $S/LE.pristine.p $LE/LeaderElectionModel.p
compile leader-election LeaderElection $LEF

echo "### agent-assignment mutants" >> $LOG
cp $AO/AgentOwnershipModel.p $S/AO.pristine.p
# m1: the sweep's claim side removed (a runner with no row never restores its claim)
cp $S/AO.pristine.p $AO/AgentOwnershipModel.p
mutate $AO/AgentOwnershipModel.p $'              announce eMRowPending;\n              send store, eClaimRow, (id = id,);\n              announceMe(true);' $'              announceMe(true);'
compile agent-assignment AgentOwnership $AOF && run agent-assignment AgentOwnership tcPartitionHealShipped 20000 ao-m1-no-claim && run agent-assignment AgentOwnership tcChaosShipped 20000 ao-m1-no-claim
# m2: the sweep's stop side removed
cp $S/AO.pristine.p $AO/AgentOwnershipModel.p
mutate $AO/AgentOwnershipModel.p $'            if (localRun && r.owner != 0) {\n              localRun = false;\n              announceMe(true);' $'            if (localRun && r.owner != 0) {\n              announceMe(true);'
compile agent-assignment AgentOwnership $AOF && run agent-assignment AgentOwnership tcLeaderHandoverShipped 20000 ao-m2-no-stop
# m3: the sweep's start side removed
cp $S/AO.pristine.p $AO/AgentOwnershipModel.p
mutate $AO/AgentOwnershipModel.p $'            } else {\n              localRun = true;\n              announce eMRowPending;\n              send store, eAddRow, (id = id,);\n              announceMe(true);\n            }' $'            } else {\n              announceMe(true);\n            }'
compile agent-assignment AgentOwnership $AOF && run agent-assignment AgentOwnership tcPartitionHealShipped 20000 ao-m3-no-start
# m4: the takeover hold ignores orphans too (holds everything)
cp $S/AO.pristine.p $AO/AgentOwnershipModel.p
mutate $AO/AgentOwnershipModel.p $'        holding = evalsSinceElection < hold && pendingTarget == 0 && !r.orphan;' $'        holding = evalsSinceElection < hold && pendingTarget == 0;'
compile agent-assignment AgentOwnership $AOF && run agent-assignment AgentOwnership tcCrashShipped 20000 ao-m4-hold-orphans
cp $S/AO.pristine.p $AO/AgentOwnershipModel.p
compile agent-assignment AgentOwnership $AOF

echo "### PEx (systematic), bounded budgets" >> $LOG
( cd $LE && $P compile --pfiles $LEF --projname LeaderElection --outdir . --mode pex ) > $OUT/pex-compile-le.raw 2>&1
for tc in tcIneligibleNode tcEligibilityRevoked; do
  o=$OUT/pex-le-$tc; ( cd $LE && $P check --mode pex -tc $tc -s 200000 -m 6 -o $o ) > $o.raw 2>&1
  echo "pex-le | $tc | $(grep -oE 'correct for any depth|partially correct.*|Found [0-9]+ bug.*|violated.*' $o.raw | head -2 | tr '\n' ' ')" >> $LOG
done
( cd $AO && $P compile --pfiles $AOF --projname AgentOwnership --outdir . --mode pex ) > $OUT/pex-compile-ao.raw 2>&1
for tc in tcLeaderHandoverShipped tcPartitionHealShipped tcChaosShipped tcLeaderHandoverHold; do
  o=$OUT/pex-ao-$tc; ( cd $AO && $P check --mode pex -tc $tc -s 200000 -m 6 -o $o ) > $o.raw 2>&1
  echo "pex-ao | $tc | $(grep -oE 'correct for any depth|partially correct.*|Found [0-9]+ bug.*|violated.*' $o.raw | head -2 | tr '\n' ' ')" >> $LOG
done
echo "### DONE" >> $LOG
