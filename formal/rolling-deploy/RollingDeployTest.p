/*
The cases (see the README): steady (no fault), wedge (an agent silently dies with its
row standing — the GH-3987 assigned-but-not-running trap), scale-out (one node added,
so the post-deploy rebalance is load-bearing), rolling deploy (every node replaced,
one pod at a time), mini deploy (the smallest rollout that can strand an agent),
chaos (a rollout with a wedge landing mid-flight), crunch-recover (a scale-down below
total capacity, then a scale-out — the shed agent must wait, then be placed), and
cascade (a node lost while the survivors cannot absorb its share — the GH-3959 case:
bounded degradation, not a chain of overload deaths).

begin(nodes, agents, tick budget k, per-node capacity, replaced, extra joins,
      removed-without-replacement, wedges, squeezes)

The squeeze cases cover DYNAMIC capacity: a node's advertised ceiling shrinks at runtime
(memory pressure — e.g. a backlog inflating per-agent cost), the leader sheds down to the
new number, displaced agents land elsewhere or wait, and a later scale-out recovers them.
*/

machine SteadyDriver {
  start state Init { entry { begin(3, 4, 3, 2, 0, 0, 0, 0, 0); } }
}

machine WedgeDriver {
  start state Init { entry { begin(2, 2, 3, 2, 0, 0, 0, 1, 0); } }
}

machine ScaleOutDriver {
  start state Init { entry { begin(3, 4, 3, 2, 0, 1, 0, 0, 0); } }
}

machine RollingDeployDriver {
  start state Init { entry { begin(3, 4, 3, 2, 3, 0, 0, 0, 0); } }
}

/* The smallest rollout that can strand an agent: the drain-to-quiescence windows inside
   a deploy step are shallow here, so mid-rollout availability is actually asserted
   rather than skipped over (see the gate-everything mutant in the README). */
machine MiniDeployDriver {
  start state Init { entry { begin(2, 2, 2, 2, 1, 0, 0, 0, 0); } }
}

machine DeployChaosDriver {
  start state Init { entry { begin(3, 4, 3, 2, 2, 0, 0, 1, 0); } }
}

/* 3 agents on 2 nodes of capacity 2; one node removed (capacity 2 < 3 agents), then one
   added. Mid-crunch the third agent must WAIT rather than overload the survivor; when
   capacity returns it must be placed — the recovery half of GH-3959. */
machine CrunchRecoverDriver {
  start state Init { entry { begin(2, 3, 3, 2, 0, 1, 1, 0, 0); } }
}

/* 5 agents on 3 nodes of capacity 2; one node removed. The survivors (capacity 4) cannot
   absorb all 5: one agent waits, nobody exceeds the ceiling, nobody dies. Without the
   ceiling this is GH-3959's production incident — each redistribution pushes the next
   survivor over and the fleet cascades to nothing. */
machine CascadeDriver {
  start state Init { entry { begin(3, 5, 3, 2, 0, 0, 1, 0, 0); } }
}

/* 3 agents fit exactly after the squeeze (capacity 4 -> 3): the displaced agent must be
   re-placed onto the other node, not dropped and not left overloading the squeezed one. */
machine SqueezeDriver {
  start state Init { entry { begin(2, 3, 3, 2, 0, 0, 0, 0, 1); } }
}

/* 4 agents on capacity 4; a squeeze shrinks it to 3, so one agent must WAIT — then a
   scale-out join restores headroom and the waiter must be placed. Dynamic capacity's two
   obligations, in one run: shed-to-the-new-number, and recover-when-capacity-returns. */
machine SqueezeRecoverDriver {
  start state Init { entry { begin(2, 4, 3, 2, 0, 1, 0, 0, 1); } }
}

fun begin(n: int, agents: int, k: int, cap: int, replace: int, extraJoins: int, remove: int, wedges: int, squeezes: int) {
  var store: machine;
  var nodes: seq[machine];
  var victims: seq[machine];
  var downs: seq[machine];
  var i: int;

  announce eMAgents, (count = agents,);
  announce eMRoster, (delta = n,);

  store = new Store((agents = agents,));
  i = 0;
  while (i < n) {
    /* Initial nodes never report readiness; the deployer field is a placeholder. */
    nodes += (i, new Node((store = store, id = i + 1, k = k, cap = cap, agents = agents, deployer = store, reportReady = false)));
    i = i + 1;
  }
  i = 0;
  while (i < n) {
    send nodes[i], eGo;
    i = i + 1;
  }

  /* Arsonists arm the store; the fault fires on the next agent start it records. */
  i = 0;
  while (i < wedges) {
    new Arsonist((store = store, kind = 0));
    i = i + 1;
  }
  i = 0;
  while (i < squeezes) {
    new Arsonist((store = store, kind = 1));
    i = i + 1;
  }

  if (replace > 0 || extraJoins > 0 || remove > 0) {
    i = 0;
    while (i < replace) {
      victims += (i, nodes[i]);
      i = i + 1;
    }
    /* Scale-down takes the LAST nodes, so it never overlaps the replaced ones. */
    i = 0;
    while (i < remove) {
      downs += (i, nodes[n - 1 - i]);
      i = i + 1;
    }
    new Deployer((store = store, victims = victims, downs = downs, extraJoins = extraJoins, k = k, cap = cap, agents = agents, firstId = n + 1));
  }
}

test tcSteadyState [main=SteadyDriver]:
  assert DeployConverges in
    { SteadyDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcWedge [main=WedgeDriver]:
  assert DeployConverges in
    { WedgeDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcScaleOut [main=ScaleOutDriver]:
  assert DeployConverges in
    { ScaleOutDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcRollingDeploy [main=RollingDeployDriver]:
  assert DeployConverges in
    { RollingDeployDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcMiniDeploy [main=MiniDeployDriver]:
  assert DeployConverges in
    { MiniDeployDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcDeployChaos [main=DeployChaosDriver]:
  assert DeployConverges in
    { DeployChaosDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcCrunchRecover [main=CrunchRecoverDriver]:
  assert DeployConverges in
    { CrunchRecoverDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcCascade [main=CascadeDriver]:
  assert DeployConverges in
    { CascadeDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcSqueeze [main=SqueezeDriver]:
  assert DeployConverges in
    { SqueezeDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };

test tcSqueezeRecover [main=SqueezeRecoverDriver]:
  assert DeployConverges in
    { SqueezeRecoverDriver, Store, Node, Deployer, RunCourier, StrikeCourier, TripCourier, SqueezeCourier, Arsonist };
