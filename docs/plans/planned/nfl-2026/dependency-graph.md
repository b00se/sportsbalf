# Dependency Graph

    [R0 complete on main]
    R0 -> {R1.1, R1.3, R1.4}
    R1.1 -> R1.2
    {R1.1, R1.3} -> R1.5

    R1 -> {R2.1, R2.4}
    {R1.1, R2.1} -> R2.2 -> R2.3
    {R2.1, R2.2, R2.3} -> R2.5

    R2.5 -> {R3.1, R3.2, R3.3, R3.4}
    {R3.1..R3.4} -> R3.5 -> R3.6 -> R3.7 -> R3.8

    R3 -> R4.1
    R1.2 + R1.3 -> R4.2
    R4.2 + R1.5 -> R4.3
    {R4.1, R4.2, R4.3} -> R4.4 -> R4.5 -> R4.6 -> R4.7 -> R4.8

    R4.8 -> {R5, P0}

R0 completion evidence is retained in [evidence/](evidence/), especially the
[closeout record](evidence/R0.closeout.md). Task definitions and full exit
criteria remain authoritative in the master roadmap.
