import hashlib, os, sys, time

cpus = {int(x) for x in sys.argv[1].split(",")}
dur = int(sys.argv[2])
os.sched_setaffinity(0, cpus)

def hash_chain(seed: bytes, n: int) -> bytes:
    h = seed
    for _ in range(n):
        h = hashlib.sha256(h).digest()
    return h

def lcg(n: int) -> int:
    acc = 0
    for _ in range(n):
        acc = (acc * 1103515245 + 12345) & 0x7fffffff
    return acc

# self-calibrate references (whatever the correct answer is, it must NOT change)
HREF = hash_chain(b"arbbot-core-check", 200_000)
LREF = lcg(2_000_000)

t0 = time.time(); iters = 0; bad = 0
while time.time() - t0 < dur:
    iters += 1
    if hash_chain(b"arbbot-core-check", 200_000) != HREF:
        bad += 1; print(f"HASH CORRUPTION iter={iters} cpus={sorted(cpus)}", flush=True)
    if lcg(2_000_000) != LREF:
        bad += 1; print(f"ARITH CORRUPTION iter={iters} cpus={sorted(cpus)}", flush=True)
print(f"DONE cpus={sorted(cpus)} iters={iters} bad={bad} elapsed={int(time.time()-t0)}s", flush=True)
