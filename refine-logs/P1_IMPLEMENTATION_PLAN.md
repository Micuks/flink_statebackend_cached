# P1 ready-gated prefetch implementation plan

Current immutable revision: `P1_IMPLEMENTATION_PLAN_20260903_190949.md`.

P1 is a default-off, bounded, strictly ordered ready-gated ring. Implementation
is split into independently tested and committed completion-hook, reflective
bridge, ring/availability, and config/observability slices. See the immutable
revision for invariants and advancement gates.
