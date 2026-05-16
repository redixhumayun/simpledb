// Buffer pool residency protocol — core hit / miss / install / evict paths.
//
// Scope: directory + frame state machine only. No writeback / dirty-flush.
// Model size: parameterised; see buffer_pool_2x2x2 for the concrete 2·2·2 instance.
//
// Hit path models the double-check OCC protocol from buffer_pool_scaling_targets.md:
//   (1) directory lookup → observe (frame, gen)
//   (2) first validate: frame ResidentStable + gen matches               → PhValidated
//   (3) pin_attempt:    increment pin_count                              → PhPinPending
//   (4) re-validate:    frame still ResidentStable + gen still matches   → PhPinned
//                       frame state/gen changed (race with eviction)     → undo pin, retry
//
// The PhValidated → PhPinPending window is where eviction can race: an evictor
// may have seen pin_count==0 and set the frame to Evicting between steps (2) and (3).
// The re-validation (4) detects this and undoes the pin safely.
//
// Invariants checked:
//   INV-1  at most one installer per block
//   INV-2  at most one resident mapping per frame
//   INV-3  confirmed pin (PhPinned) ⇒ frame holds the looked-up block with matching gen
//   INV-4  confirmed pin (PhPinned) ⇒ frame is ResidentStable
//   INV-5  generation in directory Resident entry always matches frame_gen
//   INV-6  DirInstalling ↔ exactly one owning thread
//
// Note on pin_count transience: a PhPinPending thread may have pin_count > 0 on a
// FrameFree or FrameLoading frame. This occurs when do_evict_complete completes
// (at pin_count==0) and then the window pinner fires do_pin_attempt into the now-Free
// frame. The subsequent do_pin_revalidate_fail sees non-ResidentStable and undoes the
// pin. This is a legitimate transient — the invariant frameNotReusedWhilePinned (which
// used raw pin_count) was too strict and has been removed. INV-3/4 (pinnedImpliesResidentStable)
// covers the actual safety concern: no thread can reach PhPinned on a non-ResidentStable frame.
//
// Verification commands (run from project root):
//   quint typecheck specs/buffer_pool.qnt
//   quint run specs/buffer_pool.qnt \
//     --main=buffer_pool_2x2x2 --invariant=safetyOK \
//     --max-steps=60 --max-samples=10000
//   quint test specs/buffer_pool.qnt --main=buffer_pool_2x2x2
//   quint verify specs/buffer_pool.qnt \
//     --main=buffer_pool_2x2x2 --invariant=safetyOK --max-steps=15

module buffer_pool {

  // ---- Constants --------------------------------------------------------
  const NUM_THREADS : int
  const NUM_FRAMES  : int
  const NUM_BLOCKS  : int

  // ---- Types ------------------------------------------------------------

  // Directory state for one block.
  //   Absent     – no mapping; a thread may claim Install
  //   Installing – exactly one thread owns install for this block
  //   Resident   – block published at (frame, gen); hit path may validate + pin
  type DirState =
    | DirAbsent
    | DirInstalling
    | DirResident({ frame : int, gen : int })

  // Coarse frame residency / control state.
  //   Free           – unoccupied; available for a new install
  //   Loading        – being filled by the owning installer; not yet pinnable
  //   ResidentStable – resident and pinnable; hit path validates here
  //   Evicting       – claimed for reuse; new pins must not succeed
  type FrameState =
    | FrameFree
    | FrameLoading
    | FrameResidentStable
    | FrameEvicting

  // Per-thread execution phase.
  //
  //   PhIdle          – thread is between operations
  //   PhLookedUp      – directory observation stored; frame not yet checked
  //   PhValidated     – first frame check passed (ResidentStable + gen match);
  //                     pin_count not yet incremented — eviction can still race here
  //   PhPinPending    – pin_count incremented; re-validation pending
  //                     frame may be Evicting if eviction raced the pin_attempt
  //   PhPinned        – second validation passed; pin confirmed safe
  //   PhInstalling    – thread owns DirInstalling for a block; loading in progress
  //   PhEvictClaiming – thread has set a frame to FrameEvicting
  type ThreadPhase =
    | PhIdle
    | PhLookedUp({ block : int, frame : int, gen_obs : int })
    | PhValidated({ block : int, frame : int, gen_obs : int })
    | PhPinPending({ block : int, frame : int, gen_obs : int })
    | PhPinned({ block : int, frame : int })
    | PhInstalling({ block : int, frame : int })
    | PhEvictClaiming({ frame : int })

  // ---- Helpers ----------------------------------------------------------
  pure def threadIds : Set[int] = 0.to(NUM_THREADS - 1)
  pure def frameIds  : Set[int] = 0.to(NUM_FRAMES  - 1)
  pure def blockIds  : Set[int] = 0.to(NUM_BLOCKS  - 1)

  // Sentinel: frame has no block. Must not appear as a valid block id.
  pure val NO_BLOCK : int = -1

  // ---- State Variables --------------------------------------------------
  var dir          : int -> DirState    // block_id  → directory state
  var frame_state  : int -> FrameState  // frame_id  → frame state
  var frame_gen    : int -> int         // frame_id  → generation counter
  var frame_block  : int -> int         // frame_id  → block_id (NO_BLOCK when Free)
  var frame_pins   : int -> int         // frame_id  → pin count
  var thread_phase : int -> ThreadPhase // thread_id → execution phase

  // ---- Init -------------------------------------------------------------
  action init = all {
    dir'          = blockIds.fold(Map(), (m, b) => m.put(b, DirAbsent)),
    frame_state'  = frameIds.fold(Map(), (m, f) => m.put(f, FrameFree)),
    frame_gen'    = frameIds.fold(Map(), (m, f) => m.put(f, 0)),
    frame_block'  = frameIds.fold(Map(), (m, f) => m.put(f, NO_BLOCK)),
    frame_pins'   = frameIds.fold(Map(), (m, f) => m.put(f, 0)),
    thread_phase' = threadIds.fold(Map(), (m, t) => m.put(t, PhIdle)),
  }

  // ---- Action: Directory Lookup -----------------------------------------
  //
  // Thread reads directory[bid]. If Resident, stores (frame, gen) as a local
  // observation → PhLookedUp. Frame validation is deferred to model the
  // non-atomic window between directory read and frame-local check.
  action do_dir_lookup(tid : int, bid : int) : bool = {
    val phase_idle  = thread_phase.get(tid) == PhIdle
    val entry       = dir.get(bid)
    val is_resident = match entry { | DirResident(_) => true | _ => false }
    val obs_frame   = match entry { | DirResident(r) => r.frame | _ => 0 }
    val obs_gen     = match entry { | DirResident(r) => r.gen   | _ => 0 }
    all {
      phase_idle,
      is_resident,
      thread_phase' = thread_phase.put(tid, PhLookedUp({ block: bid, frame: obs_frame, gen_obs: obs_gen })),
      dir'          = dir,
      frame_state'  = frame_state,
      frame_gen'    = frame_gen,
      frame_block'  = frame_block,
      frame_pins'   = frame_pins,
    }
  }

  // ---- Action: First Validate (hit path step 2) -------------------------
  //
  // Thread checks frame state and generation against its directory observation.
  // If both match, moves to PhValidated. Pin count is NOT yet incremented here.
  // This is an optimistic fast path — a concurrent eviction can race between
  // this step and the pin attempt.
  action do_first_validate_ok(tid : int) : bool = {
    val phase    = thread_phase.get(tid)
    val phase_ok = match phase { | PhLookedUp(_) => true | _ => false }
    val obs      = match phase { | PhLookedUp(o) => o | _ => { block: 0, frame: 0, gen_obs: 0 } }
    val state_ok = frame_state.get(obs.frame) == FrameResidentStable
    val gen_ok   = frame_gen.get(obs.frame) == obs.gen_obs
    all {
      phase_ok,
      state_ok,
      gen_ok,
      thread_phase' = thread_phase.put(tid, PhValidated({ block: obs.block, frame: obs.frame, gen_obs: obs.gen_obs })),
      dir'          = dir,
      frame_state'  = frame_state,
      frame_gen'    = frame_gen,
      frame_block'  = frame_block,
      frame_pins'   = frame_pins,
    }
  }

  // ---- Action: First Validate Fail (fast-path retry) --------------------
  //
  // Frame state is not ResidentStable, or generation has already changed.
  // No pin was attempted; thread returns to PhIdle to retry.
  action do_first_validate_fail(tid : int) : bool = {
    val phase    = thread_phase.get(tid)
    val phase_ok = match phase { | PhLookedUp(_) => true | _ => false }
    val obs      = match phase { | PhLookedUp(o) => o | _ => { block: 0, frame: 0, gen_obs: 0 } }
    val stale    = (frame_state.get(obs.frame) != FrameResidentStable)
                or (frame_gen.get(obs.frame) != obs.gen_obs)
    all {
      phase_ok,
      stale,
      thread_phase' = thread_phase.put(tid, PhIdle),
      dir'          = dir,
      frame_state'  = frame_state,
      frame_gen'    = frame_gen,
      frame_block'  = frame_block,
      frame_pins'   = frame_pins,
    }
  }

  // ---- Action: Pin Attempt (hit path step 3) ----------------------------
  //
  // Thread increments pin_count. Frame state is not re-checked here — this
  // models the atomic pin increment after the first (non-atomic) validation.
  // A concurrent eviction may have set the frame to Evicting between
  // do_first_validate_ok and this step; the re-validation will catch that.
  action do_pin_attempt(tid : int) : bool = {
    val phase    = thread_phase.get(tid)
    val phase_ok = match phase { | PhValidated(_) => true | _ => false }
    val obs      = match phase { | PhValidated(v) => v | _ => { block: 0, frame: 0, gen_obs: 0 } }
    all {
      phase_ok,
      frame_pins'   = frame_pins.put(obs.frame, frame_pins.get(obs.frame) + 1),
      thread_phase' = thread_phase.put(tid, PhPinPending({ block: obs.block, frame: obs.frame, gen_obs: obs.gen_obs })),
      dir'          = dir,
      frame_state'  = frame_state,
      frame_gen'    = frame_gen,
      frame_block'  = frame_block,
    }
  }

  // ---- Action: Pin Re-validate Success (hit path step 4, ok) -----------
  //
  // After incrementing pin_count, thread re-reads frame state and generation.
  // Both still match: pin is confirmed. Thread moves to PhPinned.
  // No eviction can have raced after pin_attempt (do_evict_claim requires
  // pin_count==0), so the only residual check is for generation changes
  // caused by a pre-attempt eviction+reinstall cycle.
  action do_pin_revalidate_ok(tid : int) : bool = {
    val phase    = thread_phase.get(tid)
    val phase_ok = match phase { | PhPinPending(_) => true | _ => false }
    val pp       = match phase { | PhPinPending(p) => p | _ => { block: 0, frame: 0, gen_obs: 0 } }
    val state_ok = frame_state.get(pp.frame) == FrameResidentStable
    val gen_ok   = frame_gen.get(pp.frame) == pp.gen_obs
    all {
      phase_ok,
      state_ok,
      gen_ok,
      thread_phase' = thread_phase.put(tid, PhPinned({ block: pp.block, frame: pp.frame })),
      dir'          = dir,
      frame_state'  = frame_state,
      frame_gen'    = frame_gen,
      frame_block'  = frame_block,
      frame_pins'   = frame_pins,
    }
  }

  // ---- Action: Pin Re-validate Fail (hit path step 4, race lost) --------
  //
  // Frame state or generation changed between pin_attempt and re-validation.
  // Typical cause: an evictor raced between do_first_validate_ok and
  // do_pin_attempt (the window where pin_count was still 0), setting the
  // frame to FrameEvicting before the pin became visible.
  // Thread undoes the pin (pin_count--) and returns to PhIdle to retry.
  action do_pin_revalidate_fail(tid : int) : bool = {
    val phase    = thread_phase.get(tid)
    val phase_ok = match phase { | PhPinPending(_) => true | _ => false }
    val pp       = match phase { | PhPinPending(p) => p | _ => { block: 0, frame: 0, gen_obs: 0 } }
    val stale    = (frame_state.get(pp.frame) != FrameResidentStable)
                or (frame_gen.get(pp.frame) != pp.gen_obs)
    all {
      phase_ok,
      stale,
      frame_pins'   = frame_pins.put(pp.frame, frame_pins.get(pp.frame) - 1),
      thread_phase' = thread_phase.put(tid, PhIdle),
      dir'          = dir,
      frame_state'  = frame_state,
      frame_gen'    = frame_gen,
      frame_block'  = frame_block,
    }
  }

  // ---- Action: Miss / Claim Install -------------------------------------
  //
  // Block is Absent and a Free frame is available. Thread atomically claims
  // both: directory moves to DirInstalling, frame to FrameLoading.
  // Generation increments here so that any prior observation of this frame's
  // old residency becomes stale and forces retry.
  action do_miss_install(tid : int, bid : int, fid : int) : bool = all {
    thread_phase.get(tid) == PhIdle,
    dir.get(bid) == DirAbsent,
    frame_state.get(fid) == FrameFree,
    dir'          = dir.put(bid, DirInstalling),
    frame_state'  = frame_state.put(fid, FrameLoading),
    frame_gen'    = frame_gen.put(fid, frame_gen.get(fid) + 1),
    frame_block'  = frame_block.put(fid, bid),
    thread_phase' = thread_phase.put(tid, PhInstalling({ block: bid, frame: fid })),
    frame_pins'   = frame_pins,
  }

  // ---- Action: Publish Resident -----------------------------------------
  //
  // Installer finishes loading. Frame transitions to FrameResidentStable and
  // directory entry becomes DirResident, pairing this frame with its current
  // generation. Concurrent hit threads may now observe and validate.
  action do_publish(tid : int) : bool = {
    val phase    = thread_phase.get(tid)
    val phase_ok = match phase { | PhInstalling(_) => true | _ => false }
    val inst     = match phase { | PhInstalling(i) => i | _ => { block: 0, frame: 0 } }
    val loading  = frame_state.get(inst.frame) == FrameLoading
    all {
      phase_ok,
      loading,
      frame_state'  = frame_state.put(inst.frame, FrameResidentStable),
      dir'          = dir.put(inst.block, DirResident({ frame: inst.frame, gen: frame_gen.get(inst.frame) })),
      thread_phase' = thread_phase.put(tid, PhIdle),
      frame_gen'    = frame_gen,
      frame_block'  = frame_block,
      frame_pins'   = frame_pins,
    }
  }

  // ---- Action: Eviction Claim -------------------------------------------
  //
  // Thread claims a ResidentStable, unpinned frame. Transitions it to
  // FrameEvicting. Any concurrent thread that first-validated this frame but
  // has not yet pinned will see FrameEvicting during pin re-validation and
  // undo their pin. New first-validations will see FrameEvicting and fail fast.
  action do_evict_claim(tid : int, fid : int) : bool = all {
    thread_phase.get(tid) == PhIdle,
    frame_state.get(fid) == FrameResidentStable,
    frame_pins.get(fid) == 0,
    frame_state'  = frame_state.put(fid, FrameEvicting),
    thread_phase' = thread_phase.put(tid, PhEvictClaiming({ frame: fid })),
    dir'          = dir,
    frame_gen'    = frame_gen,
    frame_block'  = frame_block,
    frame_pins'   = frame_pins,
  }

  // ---- Action: Eviction Complete ----------------------------------------
  //
  // Evicting thread removes the directory entry (DirResident → DirAbsent) and
  // marks the frame Free. Guard: frame_pins must be zero.
  //
  // Why the pin_count guard: an eviction claim requires pin_count==0, but a
  // window pinner (who first-validated before the eviction claim, then did
  // pin_attempt into an Evicting frame) may have incremented pin_count after
  // the claim. That pinner will see FrameEvicting in its re-validation and
  // decrement pin_count (do_pin_revalidate_fail), so this guard eventually
  // becomes true. Without it, the evictor would free a frame that still has
  // a pending pin, violating frameNotReusedWhilePinned.
  action do_evict_complete(tid : int) : bool = {
    val phase    = thread_phase.get(tid)
    val phase_ok = match phase { | PhEvictClaiming(_) => true | _ => false }
    val ev_frame = match phase { | PhEvictClaiming(ev) => ev.frame | _ => 0 }
    val bid      = frame_block.get(ev_frame)
    val bid_valid  = blockIds.contains(bid)
    val dir_entry  = if (bid_valid) dir.get(bid) else DirAbsent
    val dir_ok     = match dir_entry { | DirResident(r) => r.frame == ev_frame | _ => false }
    val pins_zero  = frame_pins.get(ev_frame) == 0
    all {
      phase_ok,
      bid_valid,
      dir_ok,
      pins_zero,
      frame_state'  = frame_state.put(ev_frame, FrameFree),
      frame_block'  = frame_block.put(ev_frame, NO_BLOCK),
      dir'          = dir.put(bid, DirAbsent),
      thread_phase' = thread_phase.put(tid, PhIdle),
      frame_gen'    = frame_gen,
      frame_pins'   = frame_pins,
    }
  }

  // ---- Action: Unpin ----------------------------------------------------
  action do_unpin(tid : int) : bool = {
    val phase    = thread_phase.get(tid)
    val phase_ok = match phase { | PhPinned(_) => true | _ => false }
    val p        = match phase { | PhPinned(pp) => pp | _ => { block: 0, frame: 0 } }
    val pins_ok  = frame_pins.get(p.frame) > 0
    all {
      phase_ok,
      pins_ok,
      frame_pins'   = frame_pins.put(p.frame, frame_pins.get(p.frame) - 1),
      thread_phase' = thread_phase.put(tid, PhIdle),
      dir'          = dir,
      frame_state'  = frame_state,
      frame_gen'    = frame_gen,
      frame_block'  = frame_block,
    }
  }

  // ---- Step Action (scheduler) -----------------------------------------
  //
  // In each step one thread, one block, and one frame are picked
  // nondeterministically, and one enabled action fires.
  action step = {
    nondet tid = threadIds.oneOf()
    nondet bid = blockIds.oneOf()
    nondet fid = frameIds.oneOf()
    any {
      do_dir_lookup(tid, bid),
      do_first_validate_ok(tid),
      do_first_validate_fail(tid),
      do_pin_attempt(tid),
      do_pin_revalidate_ok(tid),
      do_pin_revalidate_fail(tid),
      do_miss_install(tid, bid, fid),
      do_publish(tid),
      do_evict_claim(tid, fid),
      do_evict_complete(tid),
      do_unpin(tid),
    }
  }

  // ---- Invariants (Safety) -----------------------------------------------

  // Basic type / domain consistency.
  val typeOK : bool = and {
    frameIds.forall(f => frame_pins.get(f) >= 0),
    frameIds.forall(f => frame_gen.get(f) >= 0),
    // Free frames have no block. They may transiently have pin_count > 0 if a
    // PhPinPending thread fires do_pin_attempt after do_evict_complete has already
    // freed the frame; do_pin_revalidate_fail will undo that pin.
    frameIds.forall(f =>
      (frame_state.get(f) == FrameFree) implies (frame_block.get(f) == NO_BLOCK)
    ),
    // Non-free frames hold a valid block id.
    frameIds.forall(f =>
      (frame_state.get(f) != FrameFree) implies blockIds.contains(frame_block.get(f))
    ),
  }

  // INV-1 & INV-7: At most one installer per block; DirInstalling ↔ exactly
  // one owning thread in PhInstalling for that block.
  val atMostOneInstallerPerBlock : bool =
    blockIds.forall(bid => {
      val installers = threadIds.filter(tid =>
        match thread_phase.get(tid) {
          | PhInstalling(inst) => inst.block == bid
          | _                  => false
        }
      )
      and {
        installers.size() <= 1,
        (dir.get(bid) == DirInstalling) implies (installers.size() == 1),
      }
    })

  // INV-2: At most one resident directory mapping per frame.
  val atMostOneResidentMappingPerFrame : bool =
    frameIds.forall(fid =>
      blockIds.filter(bid =>
        match dir.get(bid) {
          | DirResident(r) => r.frame == fid
          | _              => false
        }
      ).size() <= 1
    )

  // INV-3 & INV-4: A confirmed pin (PhPinned) ⇒ frame is ResidentStable,
  // holds the right block, and has a positive pin count.
  // PhPinPending is excluded: during re-validation the frame may be Evicting.
  val pinnedImpliesResidentStable : bool =
    threadIds.forall(tid =>
      match thread_phase.get(tid) {
        | PhPinned(p) =>
          and {
            frame_state.get(p.frame) == FrameResidentStable,
            frame_pins.get(p.frame) > 0,
            frame_block.get(p.frame) == p.block,
          }
        | _ => true
      }
    )

  // INV-5: The generation and block in a DirResident entry always match the
  // frame's current generation counter and frame_block. The frame may be
  // ResidentStable (normal) or FrameEvicting (transient window between eviction
  // claim and directory unpublish); Loading or Free is impossible because those
  // transitions require the directory entry to already be Absent.
  val dirResidentConsistent : bool =
    blockIds.forall(bid =>
      match dir.get(bid) {
        | DirResident(r) =>
          and {
            frame_gen.get(r.frame) == r.gen,
            frame_block.get(r.frame) == bid,
            (frame_state.get(r.frame) == FrameResidentStable)
              or (frame_state.get(r.frame) == FrameEvicting),
          }
        | _ => true
      }
    )

  // Combined safety invariant.
  val safetyOK : bool = and {
    typeOK,
    atMostOneInstallerPerBlock,
    atMostOneResidentMappingPerFrame,
    pinnedImpliesResidentStable,
    dirResidentConsistent,
  }

  // ---- Temporal (Liveness) — requires Apalache + fairness annotations ----

  def phaseIsPinPending(tid : int) : bool =
    match thread_phase.get(tid) { | PhPinPending(_) => true | _ => false }

  def phaseIsPinned(tid : int) : bool =
    match thread_phase.get(tid) { | PhPinned(_) => true | _ => false }

  def phaseIsInstalling(tid : int) : bool =
    match thread_phase.get(tid) { | PhInstalling(_) => true | _ => false }

  def phaseIsEvicting(tid : int) : bool =
    match thread_phase.get(tid) { | PhEvictClaiming(_) => true | _ => false }

  // Helper for tests/documentation: whether the miss/install claim is enabled
  // in the current state for a specific (thread, block, frame) choice.
  def missInstallEnabled(tid : int, bid : int, fid : int) : bool =
    and {
      thread_phase.get(tid) == PhIdle,
      dir.get(bid) == DirAbsent,
      frame_state.get(fid) == FrameFree,
    }

  // Helper for tests/documentation: whether unpin is enabled for a thread in
  // the current state.
  def unpinEnabled(tid : int) : bool =
    {
      val phase    = thread_phase.get(tid)
      val phase_ok = match phase { | PhPinned(_) => true | _ => false }
      val p        = match phase { | PhPinned(pp) => pp | _ => { block: 0, frame: 0 } }
      val pins_ok  = frame_pins.get(p.frame) > 0
      and {
        phase_ok,
        pins_ok,
      }
    }

  // Helper for tests/documentation: whether eviction claim is enabled for a
  // specific (thread, frame) choice in the current state.
  def evictClaimEnabled(tid : int, fid : int) : bool =
    and {
      thread_phase.get(tid) == PhIdle,
      frame_state.get(fid) == FrameResidentStable,
      frame_pins.get(fid) == 0,
    }

  // L1: Every pending or confirmed pin eventually resolves (pin_count drops).
  temporal pinnedEventuallyUnpins : bool =
    threadIds.forall(tid =>
      always(
        (phaseIsPinPending(tid) or phaseIsPinned(tid))
          implies eventually(thread_phase.get(tid) == PhIdle)
      )
    )

  // L2: Every installer eventually publishes.
  temporal installerEventuallyPublishes : bool =
    threadIds.forall(tid =>
      always(phaseIsInstalling(tid) implies eventually(thread_phase.get(tid) == PhIdle))
    )

  // L3: Every evictor eventually frees the frame.
  temporal evictorEventuallyCompletes : bool =
    threadIds.forall(tid =>
      always(phaseIsEvicting(tid) implies eventually(thread_phase.get(tid) == PhIdle))
    )

  // Stutter step for liveness specs.
  action stutter = all {
    dir'          = dir,
    frame_state'  = frame_state,
    frame_gen'    = frame_gen,
    frame_block'  = frame_block,
    frame_pins'   = frame_pins,
    thread_phase' = thread_phase,
  }

  temporal Spec : bool =
    init and
    always(step.orKeep((dir, frame_state, frame_gen, frame_block, frame_pins, thread_phase))) and
    weakFair(step, (dir, frame_state, frame_gen, frame_block, frame_pins, thread_phase))
}

// ---- Concrete instantiation for model checking ----------------------------
// 2 threads · 2 frames · 2 blocks.
//   - two threads exercise the double-check race window
//   - two blocks expose install-exclusivity races
//   - evict + reinstall on the same frame advances generation and exercises
//     the stale-observation retry path
module buffer_pool_2x2x2 {
  import buffer_pool(
    NUM_THREADS = 2,
    NUM_FRAMES  = 2,
    NUM_BLOCKS  = 2
  ).*

  // ---- Deterministic tests (run with `quint test specs/buffer_pool.qnt`) ----

  // TEST 1: Stale observation → first-validate fail → retry without pinning.
  //
  // Thread 1 observes (frame=0, gen=1). Before it first-validates, thread 0
  // evicts and reinstalls block 1 on frame 0 (gen advances to 2). Thread 1's
  // first-validate sees the gen mismatch and returns to PhIdle without pinning.
  run staleObservationTest =
    init
      .then(do_miss_install(0, 0, 0))
      .then(do_publish(0))
    // Thread 1 captures (frame=0, gen_obs=1).
      .then(do_dir_lookup(1, 0))
    // Thread 0 evicts and reinstalls before thread 1 validates.
      .then(do_evict_claim(0, 0))
      .then(do_evict_complete(0))
      .then(do_miss_install(0, 1, 0))
      .then(do_publish(0))
    // Frame gen has advanced to 2; thread 1's gen_obs=1 is stale.
      .expect(frame_gen.get(0) == 2)
    // First-validate fails (gen mismatch); thread 1 returns to Idle.
      .then(do_first_validate_fail(1))
      .expect(thread_phase.get(1) == PhIdle)

  // TEST 2: Double-check race — eviction races between first-validate and pin_attempt.
  //
  // Thread 1 first-validates successfully (frame ResidentStable, gen=1).
  // Before it increments pin_count, thread 0 evicts frame 0 (pin_count was 0).
  // Thread 1 then increments pin_count into an Evicting frame.
  // Re-validation detects FrameEvicting and undoes the pin.
  run doubleCheckRaceTest =
    init
      .then(do_miss_install(0, 0, 0))
      .then(do_publish(0))
    // Thread 1: directory lookup and first-validate succeed.
      .then(do_dir_lookup(1, 0))
      .then(do_first_validate_ok(1))
    // Thread 0 evicts frame 0 while thread 1 is in PhValidated (pin_count==0).
      .then(do_evict_claim(0, 0))
    // Thread 1 increments pin_count into an Evicting frame → PhPinPending.
      .then(do_pin_attempt(1))
      .expect(frame_state.get(0) == FrameEvicting)
      .expect(frame_pins.get(0) == 1)
    // Re-validation detects FrameEvicting ≠ ResidentStable → undo pin, retry.
      .then(do_pin_revalidate_fail(1))
      .expect(thread_phase.get(1) == PhIdle)
      .expect(frame_pins.get(0) == 0)
    // Now evictor can complete (pin_count dropped to 0).
      .then(do_evict_complete(0))
      .expect(frame_state.get(0) == FrameFree)

  // TEST 3: Install exclusivity — DirInstalling blocks a second concurrent install.
  run installExclusivityTest =
    init
      .then(do_miss_install(0, 0, 0))
    // Thread 1 is idle and frame 1 is still Free, but a second install of the
    // same block is disabled because dir[0] is already DirInstalling.
      .expect(thread_phase.get(1) == PhIdle)
      .expect(frame_state.get(1) == FrameFree)
      .expect(dir.get(0) == DirInstalling)
      .expect(not(missInstallEnabled(1, 0, 1)))
      .then(do_publish(0))
      .expect(frame_state.get(0) == FrameResidentStable)

  // TEST 4: Eviction blocked while frame is pinned; succeeds after unpin.
  run evictionBlockedByPinTest =
    init
      .then(do_miss_install(0, 0, 0))
      .then(do_publish(0))
    // Thread 1 pins frame 0 via the full double-check protocol.
      .then(do_dir_lookup(1, 0))
      .then(do_first_validate_ok(1))
      .then(do_pin_attempt(1))
      .then(do_pin_revalidate_ok(1))
    // Only the owning pinned thread may unpin.
      .expect(unpinEnabled(1))
      .expect(not(unpinEnabled(0)))
    // Pin count > 0 blocks eviction claim.
      .expect(frame_pins.get(0) > 0)
      .expect(not(evictClaimEnabled(0, 0)))
    // Thread 1 unpins; now eviction can proceed.
      .then(do_unpin(1))
      .expect(frame_pins.get(0) == 0)
      .expect(evictClaimEnabled(0, 0))
      .then(do_evict_claim(0, 0))
      .expect(frame_state.get(0) == FrameEvicting)
}
